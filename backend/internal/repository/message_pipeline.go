package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/goim/goim/internal/model"
	redislua "github.com/goim/goim/internal/redis"
)

// AtomicMessageWriter is deliberately separate from RedisRepo so existing
// service mocks remain source-compatible while the new pipeline is rolled out.
type AtomicMessageWriter interface {
	WritePrivateMessageAtomic(context.Context, int64, *model.SendMessage) (*redislua.MessageWriteResult, error)
	WriteGroupMessageAtomic(context.Context, int64, int64, *model.SendMessage) (*redislua.MessageWriteResult, error)
}

type PersistPublisher interface {
	PublishPersistEvent(context.Context, *model.MessagePersistEvent) error
}

type StreamStore interface {
	EnsurePersistGroup(context.Context, string, string) error
	ReadPersistEvents(context.Context, string, string, time.Duration, int) ([]PersistStreamEntry, error)
	ClaimPersistEvents(context.Context, string, string, time.Duration, int) ([]PersistStreamEntry, error)
	AckDeletePersistEvent(context.Context, string, string) error
	TrimPersistStream(context.Context, string, time.Duration, int) error
}

type GroupMembershipCache interface {
	WarmGroupMembers(context.Context, int64, []model.GroupMember) error
	SetGroupMemberInfo(context.Context, int64, int64, int, bool) error
}

type PersistStreamEntry struct {
	ID      string
	Payload []byte
}

func (r *RedisRepoImpl) WritePrivateMessageAtomic(ctx context.Context, senderID int64, req *model.SendMessage) (*redislua.MessageWriteResult, error) {
	return redislua.ExecPrivateMsgWrite(r.rdb, ctx, senderID, req.ToID, req)
}

func (r *RedisRepoImpl) WriteGroupMessageAtomic(ctx context.Context, groupID, senderID int64, req *model.SendMessage) (*redislua.MessageWriteResult, error) {
	return redislua.ExecGroupMsgWrite(r.rdb, ctx, groupID, senderID, req)
}

func (r *RedisRepoImpl) WarmGroupMembers(ctx context.Context, groupID int64, members []model.GroupMember) error {
	groupKey := fmt.Sprintf("group_members:%d", groupID)
	pipe := r.rdb.Pipeline()
	pipe.Del(ctx, groupKey, fmt.Sprintf("group_member_info:%d", groupID))
	for _, m := range members {
		uid := strconv.FormatInt(m.UserID, 10)
		pipe.SAdd(ctx, groupKey, uid)
		pipe.SAdd(ctx, fmt.Sprintf("user_groups:%d", m.UserID), strconv.FormatInt(groupID, 10))
		muted := m.MutedUntil != nil && m.MutedUntil.After(time.Now())
		info, _ := json.Marshal(map[string]interface{}{"role": m.Role, "muted": muted})
		pipe.HSet(ctx, fmt.Sprintf("group_member_info:%d", groupID), uid, string(info))
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisRepoImpl) SetGroupMemberInfo(ctx context.Context, groupID, userID int64, role int, muted bool) error {
	info, err := json.Marshal(map[string]interface{}{"role": role, "muted": muted})
	if err != nil {
		return err
	}
	return r.rdb.HSet(ctx, fmt.Sprintf("group_member_info:%d", groupID), strconv.FormatInt(userID, 10), string(info)).Err()
}

func (r *RedisRepoImpl) EnsurePersistGroup(ctx context.Context, group, consumer string) error {
	_, err := r.rdb.XGroupCreateMkStream(ctx, "message_persist_stream", group, "0").Result()
	if err != nil && !isBusyGroupErr(err) {
		return err
	}
	return nil
}

func isBusyGroupErr(err error) bool {
	return err != nil && (err.Error() == "BUSYGROUP Consumer Group name already exists" || len(err.Error()) >= 8 && err.Error()[:8] == "BUSYGROUP")
}

func (r *RedisRepoImpl) ReadPersistEvents(ctx context.Context, group, consumer string, block time.Duration, count int) ([]PersistStreamEntry, error) {
	if count <= 0 {
		count = 10
	}
	res, err := r.rdb.XReadGroup(ctx, &goredis.XReadGroupArgs{Group: group, Consumer: consumer, Streams: []string{"message_persist_stream", ">"}, Count: int64(count), Block: block}).Result()
	if err != nil {
		if err == goredis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return flattenStreamResult(res), nil
}

func (r *RedisRepoImpl) ClaimPersistEvents(ctx context.Context, group, consumer string, minIdle time.Duration, count int) ([]PersistStreamEntry, error) {
	res, _, err := r.rdb.XAutoClaim(ctx, &goredis.XAutoClaimArgs{Stream: "message_persist_stream", Group: group, Consumer: consumer, MinIdle: minIdle, Start: "-", Count: int64(count)}).Result()
	if err != nil && err != goredis.Nil {
		return nil, err
	}
	entries := make([]PersistStreamEntry, 0, len(res))
	for _, e := range res {
		entries = append(entries, PersistStreamEntry{ID: e.ID, Payload: fieldBytes(e.Values["payload"])})
	}
	return entries, nil
}

func flattenStreamResult(res []goredis.XStream) []PersistStreamEntry {
	var out []PersistStreamEntry
	for _, s := range res {
		for _, e := range s.Messages {
			out = append(out, PersistStreamEntry{ID: e.ID, Payload: fieldBytes(e.Values["payload"])})
		}
	}
	return out
}
func fieldBytes(v interface{}) []byte {
	switch x := v.(type) {
	case string:
		return []byte(x)
	case []byte:
		return x
	default:
		b, _ := json.Marshal(x)
		return b
	}
}

func (r *RedisRepoImpl) AckDeletePersistEvent(ctx context.Context, group, streamID string) error {
	pipe := r.rdb.Pipeline()
	// Leave a short-lived confirmation marker so the periodic janitor can
	// safely remove a stream entry if the process dies between XACK and XDEL.
	pipe.SAdd(ctx, "message_persist_confirmed", streamID)
	pipe.Expire(ctx, "message_persist_confirmed", 7*24*time.Hour)
	pipe.XAck(ctx, "message_persist_stream", group, streamID)
	pipe.XDel(ctx, "message_persist_stream", streamID)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisRepoImpl) TrimPersistStream(ctx context.Context, stream string, olderThan time.Duration, maxLen int) error {
	if maxLen > 0 {
		return r.rdb.XTrimMaxLenApprox(ctx, stream, int64(maxLen), 0).Err()
	}
	cutoff := time.Now().Add(-olderThan).UnixMilli()
	return r.rdb.XTrimMinIDApprox(ctx, stream, fmt.Sprintf("%d-0", cutoff), 0).Err()
}

func (r *RedisRepoImpl) RedisClient() *goredis.Client { return r.rdb }

// ConvertPersistEvent creates the legacy message model used by existing
// repository methods and by the MySQL consumer.
func ConvertPersistEvent(e *model.MessagePersistEvent) (interface{}, error) {
	if e.ConvType == model.ConvTypePrivate {
		return &model.PrivateMessage{ID: e.ServerMsgID, ClientMsgID: e.ClientMsgID, SenderID: e.SenderID, ReceiverID: e.ReceiverID, Content: e.Content, MsgType: e.MsgType, CreatedAt: time.UnixMilli(e.ServerTimestamp)}, nil
	}
	if e.ConvType == model.ConvTypeGroup {
		return &model.GroupMessage{ID: e.ServerMsgID, ClientMsgID: e.ClientMsgID, GroupID: e.GroupID, SenderID: e.SenderID, Content: e.Content, MsgType: e.MsgType, GroupSeq: e.GroupSeq, CreatedAt: time.UnixMilli(e.ServerTimestamp)}, nil
	}
	return nil, fmt.Errorf("unknown conversation type %d", e.ConvType)
}

func ParseStreamID(id string) (int64, error) {
	p := id
	for i, c := range id {
		if c == '-' {
			p = id[:i]
			break
		}
	}
	return strconv.ParseInt(p, 10, 64)
}

var _ AtomicMessageWriter = (*RedisRepoImpl)(nil)
var _ StreamStore = (*RedisRepoImpl)(nil)
var _ GroupMembershipCache = (*RedisRepoImpl)(nil)
