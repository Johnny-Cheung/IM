package redis

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/goim/goim/internal/model"
	"github.com/stretchr/testify/require"
)

func TestAtomicPrivateMessageWrite(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()
	ctx := context.Background()
	clientID := "atomic-private-test"
	keys := []string{"friend:91001:91002", "friend:91002:91001", "blacklist:91002", "msg_dedup:91001:" + clientID, "inbox_order:91001", "inbox_order:91002", "inbox_data:91001", "inbox_data:91002", "conv_order:91001", "conv_order:91002", "conv_meta:91001", "conv_meta:91002", "unread:91002"}
	rdb.Del(ctx, keys...)
	defer rdb.Del(ctx, keys...)
	defer rdb.XTrimMaxLen(ctx, "message_persist_stream", 0).Err()
	rdb.Set(ctx, keys[0], "1", 0)
	rdb.Set(ctx, keys[1], "1", 0)
	res, err := ExecPrivateMsgWrite(rdb, ctx, 91001, 91002, &model.SendMessage{ClientMsgID: clientID, MsgType: model.MsgTypeText, Content: "hello", ToID: 91002})
	require.NoError(t, err)
	require.Equal(t, MsgWriteOK, res.ErrCode)
	require.NotZero(t, res.MsgID)
	require.NotEmpty(t, res.EventID)
	entries, err := rdb.XRange(ctx, "message_persist_stream", res.EventID, res.EventID).Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	var event model.MessagePersistEvent
	require.NoError(t, json.Unmarshal([]byte(entries[0].Values["payload"].(string)), &event))
	require.Equal(t, res.MsgID, event.ServerMsgID)
	require.Equal(t, int64(1), rdb.ZCard(ctx, "inbox_order:91002").Val())
	unread, err := rdb.HGet(ctx, "unread:91002", "p_91001_91002").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(1), unread)
	dup, err := ExecPrivateMsgWrite(rdb, ctx, 91001, 91002, &model.SendMessage{ClientMsgID: clientID, MsgType: model.MsgTypeText, Content: "hello", ToID: 91002})
	require.NoError(t, err)
	require.Equal(t, MsgWriteDuplicate, dup.ErrCode)
	require.Equal(t, res.MsgID, dup.MsgID)
}

func TestAtomicGroupMessageWrite(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()
	ctx := context.Background()
	clientID := "atomic-group-test"
	keys := []string{"group_members:91003", "group_member_info:91003", "msg_dedup:91004:" + clientID, "group_seq:91003", "outbox_order:91003", "outbox_data:91003", "conv_order:91004", "conv_meta:91004", "unread:91004", "unread:91005"}
	rdb.Del(ctx, keys...)
	defer rdb.Del(ctx, keys...)
	defer rdb.XTrimMaxLen(ctx, "message_persist_stream", 0).Err()
	rdb.SAdd(ctx, keys[0], "91004", "91005")
	rdb.HSet(ctx, keys[1], "91004", `{"role":0,"muted":false}`)
	res, err := ExecGroupMsgWrite(rdb, ctx, 91003, 91004, &model.SendMessage{ClientMsgID: clientID, MsgType: model.MsgTypeText, Content: "group", ToID: 91003})
	require.NoError(t, err)
	require.Equal(t, MsgWriteOK, res.ErrCode)
	require.NotZero(t, res.GroupSeq)
	require.NotEmpty(t, res.EventID)
	entries, err := rdb.XRange(ctx, "message_persist_stream", res.EventID, res.EventID).Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	var event model.MessagePersistEvent
	require.NoError(t, json.Unmarshal([]byte(entries[0].Values["payload"].(string)), &event))
	require.Equal(t, res.MsgID, event.ServerMsgID)
	require.Equal(t, int64(1), rdb.ZCard(ctx, "outbox_order:91003").Val())
	unread, err := rdb.HGet(ctx, "unread:91005", "g_91003").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(1), unread)
}
