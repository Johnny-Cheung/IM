package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/redis"
)

// RedisRepo 定义了消息服务和下游MQ消费者所需的所有Redis操作。
// 该接口便于在测试中进行mock。
type RedisRepo interface {
	// ── 收件箱 / 发件箱 ──
	WriteInbox(ctx context.Context, userID int64, msg *model.InboxMessage) error
	WriteOutbox(ctx context.Context, groupID int64, msg *model.InboxMessage) error
	ReadInbox(ctx context.Context, userID int64, lastSyncTime, lastSyncMsgID int64, batchSize int) ([]model.InboxMessage, error)
	ReadOutbox(ctx context.Context, groupID int64, lastSyncTime, lastSyncMsgID int64, limit int) ([]model.InboxMessage, error)

	// ── 会话列表 ──
	UpdateConvList(ctx context.Context, userID int64, convID string, summary string, timestamp int64) error
	GetConvList(ctx context.Context, userID int64) ([]model.ConvSummary, error)

	// ── 未读计数器 ──
	IncrementUnread(ctx context.Context, userID int64, convID string) error
	ClearUnread(ctx context.Context, userID int64, convID string) error
	GetUnreadMap(ctx context.Context, userID int64) (map[string]int64, error)

	// ── 群组已读位置 ──
	SetGroupReadPos(ctx context.Context, userID int64, convID string, seq int64) error
	GetGroupReadPos(ctx context.Context, userID int64, convID string) (int64, error)

	// ── 群组成员关系 ──
	GetGroupMemberships(ctx context.Context, userID int64) ([]int64, error)
	GetGroupMembers(ctx context.Context, groupID int64) ([]int64, error)
	AddGroupMemberRedis(ctx context.Context, groupID, userID int64) error
	RemoveGroupMemberRedis(ctx context.Context, groupID, userID int64) error

	// ── 去重 ──
	CheckDuplicate(ctx context.Context, userID int64, clientMsgID string) (bool, error)

	// ── 修剪（用于清理任务）──
	TrimInbox(ctx context.Context, userID int64, maxCount int) error
	TrimOutbox(ctx context.Context, groupID int64, maxCount int) error
	TrimInboxByTime(ctx context.Context, userID int64, beforeTimestamp int64) error
	TrimOutboxByTime(ctx context.Context, groupID int64, beforeTimestamp int64) error
	TrimConvListByTime(ctx context.Context, userID int64, beforeTimestamp int64) error
	TrimTimelineByTime(ctx context.Context, userID int64, beforeTimestamp int64) error

	// ── Lua脚本封装 ──
	ExecPrivateMsgCheck(ctx context.Context, senderID, receiverID int64, clientMsgID string) (*redis.PrivateMsgCheckResult, error)
	ExecGroupMsgCheck(ctx context.Context, groupID, senderID int64, clientMsgID string) (*redis.GroupMsgCheckResult, error)
	ExecInboxMarkRead(ctx context.Context, userID int64, convID string) (int64, error)
	ExecRevokeMsg(ctx context.Context, userID int64, convID string, msgID int64, revokeMsgJSON string, nowTimestamp int64) (bool, error)
	// ── 动态流（推拉结合）──
	PublishMomentFeed(ctx context.Context, userID int64, momentID int64, timestamp int64) error
	GetMomentFeed(ctx context.Context, userID int64, lastSyncTime int64, limit int) ([]int64, error)
	// 写扩散：批量将动态推入多个好友收件箱（pipeline），并按 maxLen 裁剪各收件箱。
	FanoutMomentFeed(ctx context.Context, friendIDs []int64, momentID int64, timestamp int64, maxLen int) error
	// 寄件箱：作者发布的每条动态都写入自己的寄件箱（拉取源）。
	AddToOutbox(ctx context.Context, authorID int64, momentID int64, timestamp int64, maxLen int) error
	// 大V集合：标记/批量筛选大V用户（好友数超阈值，其动态走拉模式）。
	MarkBigUser(ctx context.Context, userID int64) error
	FilterBigUsers(ctx context.Context, userIDs []int64) ([]int64, error)
	// 分页读取收件箱/寄件箱，按 score 降序、复合游标 (maxTs,maxID) 分页。
	// maxTs<0 表示首页（从最新开始）。
	GetTimelinePage(ctx context.Context, userID int64, maxTs int64, maxID int64, limit int) ([]model.FeedEntry, error)
	GetOutboxPage(ctx context.Context, userID int64, maxTs int64, maxID int64, limit int) ([]model.FeedEntry, error)

	// ── 朋友圈点赞（高并发）──
	// LikeMomentAtomic 原子点赞：SADD 判重 + INCR 计数（单 Lua 脚本）。
	// changed=true 表示本次为新增赞，count 为最新点赞数。
	LikeMomentAtomic(ctx context.Context, momentID, userID int64) (changed bool, count int64, err error)
	// UnlikeMomentAtomic 原子取消赞：SREM + DECR（计数不低于 0）。
	UnlikeMomentAtomic(ctx context.Context, momentID, userID int64) (changed bool, count int64, err error)
	// EnsureMomentLikesLoaded 确保点赞集合/计数已从持久层预热到 Redis。
	// 通过 loaded 标记 + NX 锁防缓存击穿；冷 key 时调用 loader 从 MySQL 拉取全部点赞用户，
	// 载入 Set 并置 count=len，四个 key 统一带 ttl。必须在任何点赞/读取操作前调用以保证计数正确。
	EnsureMomentLikesLoaded(ctx context.Context, momentID int64, loader func(context.Context) ([]int64, error), ttl time.Duration) error
	// GetMomentLikeStats 批量读取多条动态的点赞数与"viewer 是否已赞"（单次 pipeline）。
	GetMomentLikeStats(ctx context.Context, viewerID int64, momentIDs []int64) (counts map[int64]int64, liked map[int64]bool, err error)
	// GetMomentLikerIDs 返回当前 Redis 点赞集合中的全部用户 ID；调用前需先预热。
	GetMomentLikerIDs(ctx context.Context, momentID int64) ([]int64, error)
	// DeleteMomentLikes 清理删除动态遗留的点赞缓存。
	DeleteMomentLikes(ctx context.Context, momentID int64) error

	// ── 好友缓存 ──
	SetFriendCache(ctx context.Context, uidA, uidB int64) error
}

// ──────────────────────────────────────────────────────
// RedisRepoImpl — 使用go-redis的具体实现
// ──────────────────────────────────────────────────────

type RedisRepoImpl struct {
	rdb *goredis.Client
}

func NewRedisRepo(rdb *goredis.Client) *RedisRepoImpl {
	return &RedisRepoImpl{rdb: rdb}
}

// ── 收件箱 / 发件箱 ──

func (r *RedisRepoImpl) WriteInbox(ctx context.Context, userID int64, msg *model.InboxMessage) error {
	key := fmt.Sprintf("inbox:%d", userID)
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化收件箱消息失败: %w", err)
	}
	return r.rdb.ZAdd(ctx, key, goredis.Z{
		Score:  float64(msg.Timestamp),
		Member: string(msgJSON),
	}).Err()
}

func (r *RedisRepoImpl) WriteOutbox(ctx context.Context, groupID int64, msg *model.InboxMessage) error {
	key := fmt.Sprintf("outbox:%d", groupID)
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化发件箱消息失败: %w", err)
	}
	return r.rdb.ZAdd(ctx, key, goredis.Z{
		Score:  float64(msg.Timestamp),
		Member: string(msgJSON),
	}).Err()
}

func (r *RedisRepoImpl) ReadInbox(ctx context.Context, userID int64, lastSyncTime, lastSyncMsgID int64, batchSize int) ([]model.InboxMessage, error) {
	if n, err := r.rdb.Exists(ctx, fmt.Sprintf("inbox_order:%d", userID)).Result(); err == nil && n > 0 {
		return r.readIndexedMessages(ctx, fmt.Sprintf("inbox_order:%d", userID), fmt.Sprintf("inbox_data:%d", userID), lastSyncTime, lastSyncMsgID, batchSize)
	}
	key := fmt.Sprintf("inbox:%d", userID)
	return r.readMessagesAfter(ctx, key, lastSyncTime, lastSyncMsgID, batchSize)
}

func (r *RedisRepoImpl) ReadOutbox(ctx context.Context, groupID int64, lastSyncTime, lastSyncMsgID int64, limit int) ([]model.InboxMessage, error) {
	if n, err := r.rdb.Exists(ctx, fmt.Sprintf("outbox_order:%d", groupID)).Result(); err == nil && n > 0 {
		return r.readIndexedMessages(ctx, fmt.Sprintf("outbox_order:%d", groupID), fmt.Sprintf("outbox_data:%d", groupID), lastSyncTime, lastSyncMsgID, limit)
	}
	key := fmt.Sprintf("outbox:%d", groupID)
	return r.readMessagesAfter(ctx, key, lastSyncTime, lastSyncMsgID, limit)
}

func (r *RedisRepoImpl) readIndexedMessages(ctx context.Context, orderKey, dataKey string, lastSyncTime, lastSyncMsgID int64, limit int) ([]model.InboxMessage, error) {
	ids, err := r.rdb.ZRangeByScore(ctx, orderKey, &goredis.ZRangeBy{Min: fmt.Sprintf("%d", lastSyncTime), Max: "+inf"}).Result()
	if err != nil {
		return nil, fmt.Errorf("read message order: %w", err)
	}
	if len(ids) == 0 {
		return []model.InboxMessage{}, nil
	}
	pipe := r.rdb.Pipeline()
	cmds := make([]*goredis.StringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGet(ctx, dataKey, id)
	}
	_, _ = pipe.Exec(ctx)
	msgs := make([]model.InboxMessage, 0, len(ids))
	for _, cmd := range cmds {
		raw, e := cmd.Result()
		if e != nil {
			continue
		}
		var m model.InboxMessage
		if json.Unmarshal([]byte(raw), &m) != nil {
			continue
		}
		if m.Timestamp < lastSyncTime || (m.Timestamp == lastSyncTime && lastSyncMsgID > 0 && m.MsgID <= lastSyncMsgID) {
			continue
		}
		msgs = append(msgs, m)
		if limit > 0 && len(msgs) >= limit {
			break
		}
	}
	sort.Slice(msgs, func(i, j int) bool {
		if msgs[i].Timestamp != msgs[j].Timestamp {
			return msgs[i].Timestamp < msgs[j].Timestamp
		}
		return msgs[i].MsgID < msgs[j].MsgID
	})
	return msgs, nil
}

// readMessagesAfter 按复合游标 (timestamp, msgId) 严格向后读取消息。
// ZSet score 只有 timestamp，因此先读取边界及之后的有限集合（每个箱最多保留 2000 条），
// 再按 (timestamp ASC, msgId ASC) 排序和过滤，避免同毫秒消息跨页重复或遗漏。
func (r *RedisRepoImpl) readMessagesAfter(ctx context.Context, key string, lastSyncTime, lastSyncMsgID int64, limit int) ([]model.InboxMessage, error) {
	results, err := r.rdb.ZRangeByScore(ctx, key, &goredis.ZRangeBy{
		Min: fmt.Sprintf("%d", lastSyncTime),
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("ZRangeByScore消息箱: %w", err)
	}

	msgs := make([]model.InboxMessage, 0, len(results))
	for _, raw := range results {
		var m model.InboxMessage
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			continue
		}
		if m.Timestamp < lastSyncTime ||
			(m.Timestamp == lastSyncTime && lastSyncMsgID > 0 && m.MsgID <= lastSyncMsgID) {
			continue
		}
		msgs = append(msgs, m)
	}
	sort.Slice(msgs, func(i, j int) bool {
		if msgs[i].Timestamp != msgs[j].Timestamp {
			return msgs[i].Timestamp < msgs[j].Timestamp
		}
		return msgs[i].MsgID < msgs[j].MsgID
	})
	if limit > 0 && len(msgs) > limit {
		msgs = msgs[:limit]
	}
	return msgs, nil
}

// ── 会话列表 ──

func (r *RedisRepoImpl) UpdateConvList(ctx context.Context, userID int64, convID string, summary string, timestamp int64) error {
	// New representation keeps a stable convID index and the JSON separately.
	if err := r.rdb.ZAdd(ctx, fmt.Sprintf("conv_order:%d", userID), goredis.Z{Score: float64(timestamp), Member: convID}).Err(); err != nil {
		return err
	}
	if summary == "" {
		summary = convID
	}
	if err := r.rdb.HSet(ctx, fmt.Sprintf("conv_meta:%d", userID), convID, summary).Err(); err != nil {
		return err
	}
	key := fmt.Sprintf("conv_list:%d", userID)
	// Legacy representation stores the complete JSON summary as the ZSet member.
	// Remove every older member for this convID before adding the latest summary.
	members, err := r.rdb.ZRange(ctx, key, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("读取旧会话摘要: %w", err)
	}
	for _, existing := range members {
		var old model.ConvSummary
		if (json.Unmarshal([]byte(existing), &old) == nil && old.ConvID == convID) || existing == convID {
			if err := r.rdb.ZRem(ctx, key, existing).Err(); err != nil {
				return fmt.Errorf("移除旧会话摘要: %w", err)
			}
		}
	}
	// 将摘要JSON存储为ZSet成员，以便GetConvList可以返回完整的元数据。
	member := summary
	if member == "" {
		member = convID // 后备方案：如果未提供摘要，则仅存储convID
	}
	return r.rdb.ZAdd(ctx, key, goredis.Z{
		Score:  float64(timestamp),
		Member: member,
	}).Err()
}

func (r *RedisRepoImpl) GetConvList(ctx context.Context, userID int64) ([]model.ConvSummary, error) {
	orderKey, dataKey := fmt.Sprintf("conv_order:%d", userID), fmt.Sprintf("conv_meta:%d", userID)
	if n, _ := r.rdb.Exists(ctx, orderKey).Result(); n > 0 {
		ids, err := r.rdb.ZRevRange(ctx, orderKey, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		raw, err := r.rdb.HMGet(ctx, dataKey, ids...).Result()
		if err != nil {
			return nil, err
		}
		out := make([]model.ConvSummary, 0, len(ids))
		for i, v := range raw {
			if v == nil {
				continue
			}
			var s model.ConvSummary
			if json.Unmarshal([]byte(fmt.Sprint(v)), &s) == nil && s.ConvID != "" {
				out = append(out, s)
			} else {
				out = append(out, model.ConvSummary{ConvID: ids[i]})
			}
		}
		return out, nil
	}
	key := fmt.Sprintf("conv_list:%d", userID)
	// ZREVRANGE返回按lastMsgTime降序排列的成员。
	members, err := r.rdb.ZRevRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("ZRevRange会话列表: %w", err)
	}

	summaries := make([]model.ConvSummary, 0, len(members))
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		// 如果成员是JSON格式，尝试将其解析为ConvSummary。
		// 如果解析失败，则将其视为普通的convID字符串。
		var s model.ConvSummary
		if err := json.Unmarshal([]byte(member), &s); err == nil && s.ConvID != "" {
			if _, ok := seen[s.ConvID]; ok {
				continue
			}
			seen[s.ConvID] = struct{}{}
			summaries = append(summaries, s)
		} else {
			summaries = append(summaries, model.ConvSummary{ConvID: member})
		}
	}
	return summaries, nil
}

// ── 未读计数器 ──

func (r *RedisRepoImpl) IncrementUnread(ctx context.Context, userID int64, convID string) error {
	key := fmt.Sprintf("unread:%d", userID)
	return r.rdb.HIncrBy(ctx, key, convID, 1).Err()
}

func (r *RedisRepoImpl) ClearUnread(ctx context.Context, userID int64, convID string) error {
	key := fmt.Sprintf("unread:%d", userID)
	return r.rdb.HSet(ctx, key, convID, 0).Err()
}

func (r *RedisRepoImpl) GetUnreadMap(ctx context.Context, userID int64) (map[string]int64, error) {
	key := fmt.Sprintf("unread:%d", userID)
	raw, err := r.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("HGetAll未读数: %w", err)
	}

	result := make(map[string]int64, len(raw))
	for k, v := range raw {
		n, _ := strconv.ParseInt(v, 10, 64)
		if n > 0 {
			result[k] = n
		}
	}
	return result, nil
}

// ── 群组已读位置 ──

func (r *RedisRepoImpl) SetGroupReadPos(ctx context.Context, userID int64, convID string, seq int64) error {
	key := fmt.Sprintf("group_read_pos:%d", userID)
	return r.rdb.HSet(ctx, key, convID, seq).Err()
}

func (r *RedisRepoImpl) GetGroupReadPos(ctx context.Context, userID int64, convID string) (int64, error) {
	key := fmt.Sprintf("group_read_pos:%d", userID)
	val, err := r.rdb.HGet(ctx, key, convID).Result()
	if err != nil {
		if err == goredis.Nil {
			return 0, nil // 尚无已读位置
		}
		return 0, fmt.Errorf("HGet群组已读位置: %w", err)
	}
	return strconv.ParseInt(val, 10, 64)
}

// ── 群组成员关系 ──

func (r *RedisRepoImpl) GetGroupMemberships(ctx context.Context, userID int64) ([]int64, error) {
	key := fmt.Sprintf("user_groups:%d", userID)
	results, err := r.rdb.SMembers(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("SMembers用户群组: %w", err)
	}

	groupIDs := make([]int64, 0, len(results))
	for _, s := range results {
		id, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			continue
		}
		groupIDs = append(groupIDs, id)
	}
	return groupIDs, nil
}

func (r *RedisRepoImpl) GetGroupMembers(ctx context.Context, groupID int64) ([]int64, error) {
	key := fmt.Sprintf("group_members:%d", groupID)
	results, err := r.rdb.SMembers(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("SMembers群组成员: %w", err)
	}

	memberIDs := make([]int64, 0, len(results))
	for _, s := range results {
		id, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			continue
		}
		memberIDs = append(memberIDs, id)
	}
	return memberIDs, nil
}

func (r *RedisRepoImpl) AddGroupMemberRedis(ctx context.Context, groupID, userID int64) error {
	groupKey := fmt.Sprintf("group_members:%d", groupID)
	userKey := fmt.Sprintf("user_groups:%d", userID)
	userIDStr := strconv.FormatInt(userID, 10)
	groupIDStr := strconv.FormatInt(groupID, 10)

	if err := r.rdb.SAdd(ctx, groupKey, userIDStr).Err(); err != nil {
		return fmt.Errorf("SADD群组成员: %w", err)
	}
	if err := r.rdb.SAdd(ctx, userKey, groupIDStr).Err(); err != nil {
		return fmt.Errorf("SADD用户群组: %w", err)
	}
	if err := r.rdb.HSetNX(ctx, fmt.Sprintf("group_member_info:%d", groupID), userIDStr, `{"role":0,"muted":false}`).Err(); err != nil {
		return fmt.Errorf("HSET群成员信息: %w", err)
	}
	return nil
}

func (r *RedisRepoImpl) RemoveGroupMemberRedis(ctx context.Context, groupID, userID int64) error {
	groupKey := fmt.Sprintf("group_members:%d", groupID)
	userKey := fmt.Sprintf("user_groups:%d", userID)
	userIDStr := strconv.FormatInt(userID, 10)
	groupIDStr := strconv.FormatInt(groupID, 10)

	convID := fmt.Sprintf("g_%d", groupID)
	convKey := fmt.Sprintf("conv_list:%d", userID)
	members, err := r.rdb.ZRange(ctx, convKey, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("读取退群用户会话摘要: %w", err)
	}
	pipe := r.rdb.Pipeline()
	pipe.SRem(ctx, groupKey, userIDStr)
	pipe.SRem(ctx, userKey, groupIDStr)
	pipe.HDel(ctx, fmt.Sprintf("group_member_info:%d", groupID), userIDStr)
	for _, existing := range members {
		var summary model.ConvSummary
		if (json.Unmarshal([]byte(existing), &summary) == nil && summary.ConvID == convID) || existing == convID {
			pipe.ZRem(ctx, convKey, existing)
		}
	}
	pipe.HDel(ctx, fmt.Sprintf("unread:%d", userID), convID)
	pipe.HDel(ctx, fmt.Sprintf("group_read_pos:%d", userID), convID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("清理退群用户缓存: %w", err)
	}
	return nil
}

// ── 去重 ──

func (r *RedisRepoImpl) CheckDuplicate(ctx context.Context, userID int64, clientMsgID string) (bool, error) {
	key := fmt.Sprintf("msg_dedup:%d:%s", userID, clientMsgID)
	ok, err := r.rdb.SetNX(ctx, key, "1", 300*time.Second).Result() // 300秒TTL
	if err != nil {
		return false, fmt.Errorf("SetNX去重: %w", err)
	}
	return !ok, nil // true = 重复消息（SetNX失败）
}

// ── 修剪 ──

func (r *RedisRepoImpl) TrimInbox(ctx context.Context, userID int64, maxCount int) error {
	key := fmt.Sprintf("inbox:%d", userID)
	card, err := r.rdb.ZCard(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("ZCard收件箱: %w", err)
	}
	if card > int64(maxCount) {
		// 删除超出maxCount的最旧条目
		removeCount := card - int64(maxCount)
		if err := r.rdb.ZRemRangeByRank(ctx, key, 0, removeCount-1).Err(); err != nil {
			return fmt.Errorf("ZRemRangeByRank收件箱: %w", err)
		}
	}
	return nil
}

func (r *RedisRepoImpl) TrimOutbox(ctx context.Context, groupID int64, maxCount int) error {
	key := fmt.Sprintf("outbox:%d", groupID)
	card, err := r.rdb.ZCard(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("ZCard发件箱: %w", err)
	}
	if card > int64(maxCount) {
		removeCount := card - int64(maxCount)
		if err := r.rdb.ZRemRangeByRank(ctx, key, 0, removeCount-1).Err(); err != nil {
			return fmt.Errorf("ZRemRangeByRank发件箱: %w", err)
		}
	}
	return nil
}

// ── 基于时间的修剪 ──

func (r *RedisRepoImpl) TrimInboxByTime(ctx context.Context, userID int64, beforeTimestamp int64) error {
	key := fmt.Sprintf("inbox:%d", userID)
	max := fmt.Sprintf("%d", beforeTimestamp)
	if err := r.rdb.ZRemRangeByScore(ctx, key, "0", max).Err(); err != nil {
		return fmt.Errorf("ZRemRangeByScore收件箱: %w", err)
	}
	return nil
}

func (r *RedisRepoImpl) TrimOutboxByTime(ctx context.Context, groupID int64, beforeTimestamp int64) error {
	key := fmt.Sprintf("outbox:%d", groupID)
	max := fmt.Sprintf("%d", beforeTimestamp)
	if err := r.rdb.ZRemRangeByScore(ctx, key, "0", max).Err(); err != nil {
		return fmt.Errorf("ZRemRangeByScore发件箱: %w", err)
	}
	return nil
}

func (r *RedisRepoImpl) TrimConvListByTime(ctx context.Context, userID int64, beforeTimestamp int64) error {
	key := fmt.Sprintf("conv_list:%d", userID)
	max := fmt.Sprintf("%d", beforeTimestamp)
	if err := r.rdb.ZRemRangeByScore(ctx, key, "0", max).Err(); err != nil {
		return fmt.Errorf("ZRemRangeByScore会话列表: %w", err)
	}
	return nil
}

func (r *RedisRepoImpl) TrimTimelineByTime(ctx context.Context, userID int64, beforeTimestamp int64) error {
	key := fmt.Sprintf("timeline:%d", userID)
	max := fmt.Sprintf("%d", beforeTimestamp)
	if err := r.rdb.ZRemRangeByScore(ctx, key, "0", max).Err(); err != nil {
		return fmt.Errorf("ZRemRangeByScore时间线: %w", err)
	}
	return nil
}

// ── Lua脚本封装 ──

func (r *RedisRepoImpl) ExecPrivateMsgCheck(ctx context.Context, senderID, receiverID int64, clientMsgID string) (*redis.PrivateMsgCheckResult, error) {
	return redis.ExecPrivateMsgCheck(r.rdb, ctx, senderID, receiverID, clientMsgID)
}

func (r *RedisRepoImpl) ExecGroupMsgCheck(ctx context.Context, groupID, senderID int64, clientMsgID string) (*redis.GroupMsgCheckResult, error) {
	return redis.ExecGroupMsgCheck(r.rdb, ctx, groupID, senderID, clientMsgID)
}

func (r *RedisRepoImpl) ExecInboxMarkRead(ctx context.Context, userID int64, convID string) (int64, error) {
	return redis.ExecInboxMarkRead(r.rdb, ctx, userID, convID)
}

func (r *RedisRepoImpl) ExecRevokeMsg(ctx context.Context, userID int64, convID string, msgID int64, revokeMsgJSON string, nowTimestamp int64) (bool, error) {
	return redis.ExecRevokeMsg(r.rdb, ctx, userID, convID, msgID, revokeMsgJSON, nowTimestamp)
}

// ── 动态流 ──

func (r *RedisRepoImpl) PublishMomentFeed(ctx context.Context, userID int64, momentID int64, timestamp int64) error {
	key := fmt.Sprintf("timeline:%d", userID)
	return r.rdb.ZAdd(ctx, key, goredis.Z{
		Score:  float64(timestamp),
		Member: strconv.FormatInt(momentID, 10),
	}).Err()
}

func (r *RedisRepoImpl) GetMomentFeed(ctx context.Context, userID int64, lastSyncTime int64, limit int) ([]int64, error) {
	key := fmt.Sprintf("timeline:%d", userID)
	min := fmt.Sprintf("%d", lastSyncTime)
	results, err := r.rdb.ZRangeByScore(ctx, key, &goredis.ZRangeBy{
		Min:    min,
		Max:    "+inf",
		Offset: 0,
		Count:  int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("ZRangeByScore时间线: %w", err)
	}

	momentIDs := make([]int64, 0, len(results))
	for _, raw := range results {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			continue // 跳过损坏的条目
		}
		momentIDs = append(momentIDs, id)
	}
	return momentIDs, nil
}

// timelineKey / momentOutboxKey 集中管理 Feed 相关 Redis key，避免与群消息 outbox:{groupID} 冲突。
func timelineKey(userID int64) string     { return fmt.Sprintf("timeline:%d", userID) }
func momentOutboxKey(userID int64) string { return fmt.Sprintf("moment_outbox:%d", userID) }

const bigUsersKey = "moment:big_users"

// FanoutMomentFeed 用 pipeline 批量把动态推入多个好友收件箱，并按 maxLen 裁剪各收件箱。
func (r *RedisRepoImpl) FanoutMomentFeed(ctx context.Context, friendIDs []int64, momentID int64, timestamp int64, maxLen int) error {
	if len(friendIDs) == 0 {
		return nil
	}
	member := strconv.FormatInt(momentID, 10)
	pipe := r.rdb.Pipeline()
	for _, fid := range friendIDs {
		key := timelineKey(fid)
		pipe.ZAdd(ctx, key, goredis.Z{Score: float64(timestamp), Member: member})
		if maxLen > 0 {
			// 保留最新的 maxLen 条：删除 rank [0, -(maxLen+1)]（最旧的一批）
			pipe.ZRemRangeByRank(ctx, key, 0, int64(-(maxLen + 1)))
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("扇出动态到好友收件箱: %w", err)
	}
	return nil
}

// AddToOutbox 将动态写入作者自己的寄件箱 ZSet，并按 maxLen 裁剪。
func (r *RedisRepoImpl) AddToOutbox(ctx context.Context, authorID int64, momentID int64, timestamp int64, maxLen int) error {
	key := momentOutboxKey(authorID)
	pipe := r.rdb.Pipeline()
	pipe.ZAdd(ctx, key, goredis.Z{Score: float64(timestamp), Member: strconv.FormatInt(momentID, 10)})
	if maxLen > 0 {
		pipe.ZRemRangeByRank(ctx, key, 0, int64(-(maxLen + 1)))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("写入作者寄件箱: %w", err)
	}
	return nil
}

// MarkBigUser 将用户加入大V集合（sticky，只增不删）。
func (r *RedisRepoImpl) MarkBigUser(ctx context.Context, userID int64) error {
	if err := r.rdb.SAdd(ctx, bigUsersKey, strconv.FormatInt(userID, 10)).Err(); err != nil {
		return fmt.Errorf("标记大V用户: %w", err)
	}
	return nil
}

// FilterBigUsers 从给定用户 ID 中筛出属于大V集合的那些（SMISMEMBER 一次批量判定）。
func (r *RedisRepoImpl) FilterBigUsers(ctx context.Context, userIDs []int64) ([]int64, error) {
	if len(userIDs) == 0 {
		return []int64{}, nil
	}
	members := make([]interface{}, len(userIDs))
	for i, id := range userIDs {
		members[i] = strconv.FormatInt(id, 10)
	}
	flags, err := r.rdb.SMIsMember(ctx, bigUsersKey, members...).Result()
	if err != nil {
		return nil, fmt.Errorf("批量判定大V用户: %w", err)
	}
	bigUsers := make([]int64, 0, len(userIDs))
	for i, isBig := range flags {
		if isBig {
			bigUsers = append(bigUsers, userIDs[i])
		}
	}
	return bigUsers, nil
}

// GetTimelinePage 读取用户收件箱的一页（推来的动态）。
func (r *RedisRepoImpl) GetTimelinePage(ctx context.Context, userID int64, maxTs int64, maxID int64, limit int) ([]model.FeedEntry, error) {
	return r.getFeedPage(ctx, timelineKey(userID), maxTs, maxID, limit)
}

// GetOutboxPage 读取用户寄件箱的一页（其发布的动态，拉取源）。
func (r *RedisRepoImpl) GetOutboxPage(ctx context.Context, userID int64, maxTs int64, maxID int64, limit int) ([]model.FeedEntry, error) {
	return r.getFeedPage(ctx, momentOutboxKey(userID), maxTs, maxID, limit)
}

// getFeedPage 按 (ts,id) 降序读取一个 Feed ZSet 中"严格早于游标"的一页。
// maxTs<0 表示首页（无游标，从最新开始）。为规避同一 score(ts) 下多条动态的边界
// 重复/漏读，游标为复合键 (maxTs,maxID)：只返回 ts<maxTs，或 (ts==maxTs 且 id<maxID) 的条目。
// 由于 ZSet 的 member 是 momentID 字符串（非按 id 排序），同 score 的边界过滤在本函数内完成。
// 返回最多 limit 条，已严格排除游标本身，因此调用方无需再做边界去重。
func (r *RedisRepoImpl) getFeedPage(ctx context.Context, key string, maxTs int64, maxID int64, limit int) ([]model.FeedEntry, error) {
	if maxTs < 0 {
		// 首页：直接取 score 最大的 limit 条
		return r.zrevPage(ctx, key, "+inf", "-inf", limit)
	}

	entries := make([]model.FeedEntry, 0, limit)

	// 第一段：与游标同 score(ts==maxTs) 但 id<maxID 的条目（同毫秒内继续翻页）。
	sameScore, err := r.zrevPage(ctx, key, strconv.FormatInt(maxTs, 10), strconv.FormatInt(maxTs, 10), -1)
	if err != nil {
		return nil, err
	}
	for _, e := range sameScore {
		if e.MomentID < maxID {
			entries = append(entries, e)
			if len(entries) >= limit {
				return entries, nil
			}
		}
	}

	// 第二段：score 严格小于 maxTs 的条目（开区间上界）。
	older, err := r.zrevPage(ctx, key, "("+strconv.FormatInt(maxTs, 10), "-inf", limit-len(entries))
	if err != nil {
		return nil, err
	}
	entries = append(entries, older...)
	return entries, nil
}

// zrevPage 执行一次 ZREVRANGEBYSCORE（带 score），count<0 表示不限条数。
func (r *RedisRepoImpl) zrevPage(ctx context.Context, key, max, min string, count int) ([]model.FeedEntry, error) {
	opt := &goredis.ZRangeBy{Min: min, Max: max, Offset: 0}
	if count >= 0 {
		opt.Count = int64(count)
	}
	results, err := r.rdb.ZRevRangeByScoreWithScores(ctx, key, opt).Result()
	if err != nil {
		return nil, fmt.Errorf("ZRevRangeByScore读取Feed: %w", err)
	}
	entries := make([]model.FeedEntry, 0, len(results))
	for _, z := range results {
		raw, ok := z.Member.(string)
		if !ok {
			continue
		}
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			continue // 跳过损坏的条目
		}
		entries = append(entries, model.FeedEntry{MomentID: id, Ts: int64(z.Score)})
	}
	return entries, nil
}

// ── 朋友圈点赞（高并发）──
// 四个 key 集中管理，避免散落魔法字符串。
func momentLikesKey(momentID int64) string     { return fmt.Sprintf("moment:likes:%d", momentID) }
func momentLikeCountKey(momentID int64) string { return fmt.Sprintf("moment:like_count:%d", momentID) }
func momentLikeLoadedKey(momentID int64) string {
	return fmt.Sprintf("moment:like_loaded:%d", momentID)
}
func momentLikeLockKey(momentID int64) string { return fmt.Sprintf("moment:like_lock:%d", momentID) }

func (r *RedisRepoImpl) LikeMomentAtomic(ctx context.Context, momentID, userID int64) (bool, int64, error) {
	res, err := redis.ExecMomentLike(r.rdb, ctx, momentLikesKey(momentID), momentLikeCountKey(momentID), userID)
	if err != nil {
		return false, 0, fmt.Errorf("原子点赞: %w", err)
	}
	return res.Changed, res.Count, nil
}

func (r *RedisRepoImpl) UnlikeMomentAtomic(ctx context.Context, momentID, userID int64) (bool, int64, error) {
	res, err := redis.ExecMomentUnlike(r.rdb, ctx, momentLikesKey(momentID), momentLikeCountKey(momentID), userID)
	if err != nil {
		return false, 0, fmt.Errorf("原子取消赞: %w", err)
	}
	return res.Changed, res.Count, nil
}

// EnsureMomentLikesLoaded 见接口注释。空点赞集合也会写入 count=0 + loaded 标记，避免反复回源。
func (r *RedisRepoImpl) EnsureMomentLikesLoaded(ctx context.Context, momentID int64, loader func(context.Context) ([]int64, error), ttl time.Duration) error {
	flagKey := momentLikeLoadedKey(momentID)
	if ex, err := r.rdb.Exists(ctx, flagKey).Result(); err == nil && ex == 1 {
		return nil // 已预热
	}

	// 抢 warm-up 锁，防缓存击穿（并发只有一个回源）。
	lockKey := momentLikeLockKey(momentID)
	got, err := r.rdb.SetNX(ctx, lockKey, "1", 5*time.Second).Result()
	if err != nil {
		return fmt.Errorf("点赞预热抢锁: %w", err)
	}
	if !got {
		// 他人正在预热：轮询 loaded 标记 ~200ms；仍未就绪则尽力而为返回（TTL 兜底，下次再预热）。
		for i := 0; i < 20; i++ {
			time.Sleep(10 * time.Millisecond)
			if ex, _ := r.rdb.Exists(ctx, flagKey).Result(); ex == 1 {
				return nil
			}
		}
		return nil
	}
	defer r.rdb.Del(ctx, lockKey)

	// 抢到锁后二次确认（可能在抢锁间隙已被别人预热完）。
	if ex, _ := r.rdb.Exists(ctx, flagKey).Result(); ex == 1 {
		return nil
	}

	likers, err := loader(ctx)
	if err != nil {
		return fmt.Errorf("点赞预热回源: %w", err)
	}

	setKey := momentLikesKey(momentID)
	countKey := momentLikeCountKey(momentID)
	pipe := r.rdb.Pipeline()
	pipe.Del(ctx, setKey) // 清理残留，保证与 MySQL 一致
	if len(likers) > 0 {
		members := make([]interface{}, len(likers))
		for i, id := range likers {
			members[i] = id
		}
		pipe.SAdd(ctx, setKey, members...)
		pipe.Expire(ctx, setKey, ttl)
	}
	pipe.Set(ctx, countKey, len(likers), ttl)
	pipe.Set(ctx, flagKey, "1", ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("点赞预热写入: %w", err)
	}
	return nil
}

// GetMomentLikeStats 单次 pipeline 读取每条动态的 count（GET）与 viewer 是否已赞（SISMEMBER）。
// 缺失的 count 视为 0；调用方应先 EnsureMomentLikesLoaded 保证数据已预热。
func (r *RedisRepoImpl) GetMomentLikeStats(ctx context.Context, viewerID int64, momentIDs []int64) (map[int64]int64, map[int64]bool, error) {
	counts := make(map[int64]int64, len(momentIDs))
	liked := make(map[int64]bool, len(momentIDs))
	if len(momentIDs) == 0 {
		return counts, liked, nil
	}

	pipe := r.rdb.Pipeline()
	getCmds := make(map[int64]*goredis.StringCmd, len(momentIDs))
	memCmds := make(map[int64]*goredis.BoolCmd, len(momentIDs))
	viewer := strconv.FormatInt(viewerID, 10)
	for _, id := range momentIDs {
		getCmds[id] = pipe.Get(ctx, momentLikeCountKey(id))
		memCmds[id] = pipe.SIsMember(ctx, momentLikesKey(id), viewer)
	}
	// GET 命中缺失 key 会返回 redis.Nil，pipeline 聚合错误也是 Nil；逐条解析时单独处理。
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return nil, nil, fmt.Errorf("批量读取点赞状态: %w", err)
	}
	for _, id := range momentIDs {
		if v, err := getCmds[id].Int64(); err == nil {
			counts[id] = v
		} // 缺失/解析失败 → 保持 0
		if m, err := memCmds[id].Result(); err == nil {
			liked[id] = m
		}
	}
	return counts, liked, nil
}

func (r *RedisRepoImpl) GetMomentLikerIDs(ctx context.Context, momentID int64) ([]int64, error) {
	members, err := r.rdb.SMembers(ctx, momentLikesKey(momentID)).Result()
	if err != nil {
		return nil, fmt.Errorf("读取点赞用户集合: %w", err)
	}
	ids := make([]int64, 0, len(members))
	for _, member := range members {
		id, err := strconv.ParseInt(member, 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (r *RedisRepoImpl) DeleteMomentLikes(ctx context.Context, momentID int64) error {
	if err := r.rdb.Del(ctx, momentLikesKey(momentID), momentLikeCountKey(momentID), momentLikeLoadedKey(momentID), momentLikeLockKey(momentID)).Err(); err != nil {
		return fmt.Errorf("清理动态点赞缓存: %w", err)
	}
	return nil
}

// ── 好友缓存 ──

// SetFriendCache 在 Redis 中写入双向好友关系缓存。
// Lua 消息校验脚本依赖此 key 判断好友关系。
func (r *RedisRepoImpl) SetFriendCache(ctx context.Context, uidA, uidB int64) error {
	pipe := r.rdb.Pipeline()
	pipe.Set(ctx, fmt.Sprintf("friend:%d:%d", uidA, uidB), "1", 0)
	pipe.Set(ctx, fmt.Sprintf("friend:%d:%d", uidB, uidA), "1", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("设置好友缓存 %d<->%d: %w", uidA, uidB, err)
	}
	return nil
}

// DeleteFriendCache removes the bidirectional friendship keys used by the
// private-message Lua authorization check.
func (r *RedisRepoImpl) DeleteFriendCache(ctx context.Context, uidA, uidB int64) error {
	if err := r.rdb.Del(ctx,
		fmt.Sprintf("friend:%d:%d", uidA, uidB),
		fmt.Sprintf("friend:%d:%d", uidB, uidA),
	).Err(); err != nil {
		return fmt.Errorf("删除好友缓存 %d<->%d: %w", uidA, uidB, err)
	}
	return nil
}
