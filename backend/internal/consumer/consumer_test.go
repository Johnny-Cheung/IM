package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/protocol"
	redislua "github.com/goim/goim/internal/redis"
)

// ──────────────────────────────────────────────────────
// MockRedisRepo — 实现 repository.RedisRepo 接口，用于测试
// ──────────────────────────────────────────────────────

type MockRedisRepo struct {
	mu sync.Mutex

	// 捕获的数据，用于验证
	inboxWrites      map[int64][]*model.InboxMessage // userID -> 消息列表
	outboxWrites     map[int64][]*model.InboxMessage // groupID -> 消息列表
	convListUpdates  map[int64][]convListEntry       // userID -> 条目列表
	unreadIncrements map[string]int                  // "userID:convID" -> 计数
	groupMembers     map[int64][]int64               // groupID -> 成员ID列表

	// 记录哪些用户/群组被调用了裁剪
	trimmedInboxUsers   []int64
	trimmedOutboxGroups []int64

	// 朋友圈推拉结合：捕获寄件箱写入、扇出目标、大V标记
	momentOutbox map[int64][]int64 // authorID -> momentID 列表
	fanoutInbox  map[int64][]int64 // friendID -> momentID 列表（写扩散收件箱）
	bigUsers     map[int64]bool    // 被标记为大V的用户

	// 控制：注入错误
	writeInboxErr      error
	writeOutboxErr     error
	getGroupMembersErr error
	addOutboxErr       error
	countFriendsErr    error
}

type convListEntry struct {
	convID    string
	summary   string
	timestamp int64
}

func newMockRedisRepo() *MockRedisRepo {
	return &MockRedisRepo{
		inboxWrites:      make(map[int64][]*model.InboxMessage),
		outboxWrites:     make(map[int64][]*model.InboxMessage),
		convListUpdates:  make(map[int64][]convListEntry),
		unreadIncrements: make(map[string]int),
		groupMembers:     make(map[int64][]int64),
		momentOutbox:     make(map[int64][]int64),
		fanoutInbox:      make(map[int64][]int64),
		bigUsers:         make(map[int64]bool),
	}
}

func (m *MockRedisRepo) WriteInbox(ctx context.Context, userID int64, msg *model.InboxMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeInboxErr != nil {
		return m.writeInboxErr
	}
	m.inboxWrites[userID] = append(m.inboxWrites[userID], msg)
	return nil
}

func (m *MockRedisRepo) WriteOutbox(ctx context.Context, groupID int64, msg *model.InboxMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeOutboxErr != nil {
		return m.writeOutboxErr
	}
	m.outboxWrites[groupID] = append(m.outboxWrites[groupID], msg)
	return nil
}

func (m *MockRedisRepo) ReadInbox(ctx context.Context, userID int64, lastSyncTime, lastSyncMsgID int64, batchSize int) ([]model.InboxMessage, error) {
	return nil, fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) ReadOutbox(ctx context.Context, groupID int64, lastSyncTime, lastSyncMsgID int64, limit int) ([]model.InboxMessage, error) {
	return nil, fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) UpdateConvList(ctx context.Context, userID int64, convID string, summary string, timestamp int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.convListUpdates[userID] = append(m.convListUpdates[userID], convListEntry{
		convID:    convID,
		summary:   summary,
		timestamp: timestamp,
	})
	return nil
}

func (m *MockRedisRepo) GetConvList(ctx context.Context, userID int64) ([]model.ConvSummary, error) {
	return nil, fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) IncrementUnread(ctx context.Context, userID int64, convID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%d:%s", userID, convID)
	m.unreadIncrements[key]++
	return nil
}

func (m *MockRedisRepo) ClearUnread(ctx context.Context, userID int64, convID string) error {
	return fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) GetUnreadMap(ctx context.Context, userID int64) (map[string]int64, error) {
	return nil, fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) SetGroupReadPos(ctx context.Context, userID int64, convID string, seq int64) error {
	return fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) GetGroupReadPos(ctx context.Context, userID int64, convID string) (int64, error) {
	return 0, fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) GetGroupSeq(ctx context.Context, groupID int64) (int64, error) {
	return 0, fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) GetGroupMemberships(ctx context.Context, userID int64) ([]int64, error) {
	return nil, fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) GetGroupMembers(ctx context.Context, groupID int64) ([]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getGroupMembersErr != nil {
		return nil, m.getGroupMembersErr
	}
	return m.groupMembers[groupID], nil
}

func (m *MockRedisRepo) CheckDuplicate(ctx context.Context, userID int64, clientMsgID string) (bool, error) {
	return false, fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) TrimInbox(ctx context.Context, userID int64, maxCount int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.trimmedInboxUsers = append(m.trimmedInboxUsers, userID)
	return nil
}

func (m *MockRedisRepo) TrimOutbox(ctx context.Context, groupID int64, maxCount int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.trimmedOutboxGroups = append(m.trimmedOutboxGroups, groupID)
	return nil
}

func (m *MockRedisRepo) TrimInboxByTime(ctx context.Context, userID int64, beforeTimestamp int64) error {
	return fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) TrimOutboxByTime(ctx context.Context, groupID int64, beforeTimestamp int64) error {
	return fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) TrimConvListByTime(ctx context.Context, userID int64, beforeTimestamp int64) error {
	return fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) TrimTimelineByTime(ctx context.Context, userID int64, beforeTimestamp int64) error {
	return fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) ExecPrivateMsgCheck(ctx context.Context, senderID, receiverID int64, clientMsgID string) (*redislua.PrivateMsgCheckResult, error) {
	return nil, fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) ExecGroupMsgCheck(ctx context.Context, groupID, senderID int64, clientMsgID string) (*redislua.GroupMsgCheckResult, error) {
	return nil, fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) ExecInboxMarkRead(ctx context.Context, userID int64, convID string) (int64, error) {
	return 0, fmt.Errorf("存根：消费者未使用")
}

func (m *MockRedisRepo) ExecRevokeMsg(ctx context.Context, userID int64, convID string, msgID int64, revokeMsgJSON string, nowTimestamp int64) (bool, error) {
	return false, fmt.Errorf("存根：消费者未使用")
}
func (m *MockRedisRepo) AddGroupMemberRedis(_ context.Context, _ int64, _ int64) error    { return nil }
func (m *MockRedisRepo) RemoveGroupMemberRedis(_ context.Context, _ int64, _ int64) error { return nil }
func (m *MockRedisRepo) PublishMomentFeed(_ context.Context, _ int64, _ int64, _ int64) error {
	return nil
}
func (m *MockRedisRepo) GetMomentFeed(_ context.Context, _ int64, _ int64, _ int) ([]int64, error) {
	return nil, nil
}

func (m *MockRedisRepo) AddToOutbox(_ context.Context, authorID int64, momentID int64, _ int64, _ int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.addOutboxErr != nil {
		return m.addOutboxErr
	}
	m.momentOutbox[authorID] = append(m.momentOutbox[authorID], momentID)
	return nil
}

func (m *MockRedisRepo) FanoutMomentFeed(_ context.Context, friendIDs []int64, momentID int64, _ int64, _ int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, fid := range friendIDs {
		m.fanoutInbox[fid] = append(m.fanoutInbox[fid], momentID)
	}
	return nil
}

func (m *MockRedisRepo) MarkBigUser(_ context.Context, userID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bigUsers[userID] = true
	return nil
}

func (m *MockRedisRepo) FilterBigUsers(_ context.Context, userIDs []int64) ([]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []int64
	for _, id := range userIDs {
		if m.bigUsers[id] {
			out = append(out, id)
		}
	}
	return out, nil
}

func (m *MockRedisRepo) GetTimelinePage(_ context.Context, _ int64, _ int64, _ int64, _ int) ([]model.FeedEntry, error) {
	return nil, nil
}

func (m *MockRedisRepo) GetOutboxPage(_ context.Context, _ int64, _ int64, _ int64, _ int) ([]model.FeedEntry, error) {
	return nil, nil
}

func (m *MockRedisRepo) SetFriendCache(_ context.Context, _ int64, _ int64) error { return nil }

// ── 高并发点赞新增接口 ──
func (m *MockRedisRepo) LikeMomentAtomic(_ context.Context, _ int64, _ int64) (bool, int64, error) {
	return false, 0, nil
}
func (m *MockRedisRepo) UnlikeMomentAtomic(_ context.Context, _ int64, _ int64) (bool, int64, error) {
	return false, 0, nil
}
func (m *MockRedisRepo) EnsureMomentLikesLoaded(_ context.Context, _ int64, _ func(context.Context) ([]int64, error), _ time.Duration) error {
	return nil
}
func (m *MockRedisRepo) GetMomentLikeStats(_ context.Context, _ int64, _ []int64) (map[int64]int64, map[int64]bool, error) {
	return nil, nil, nil
}
func (m *MockRedisRepo) GetMomentLikerIDs(_ context.Context, _ int64) ([]int64, error) {
	return nil, nil
}
func (m *MockRedisRepo) DeleteMomentLikes(_ context.Context, _ int64) error { return nil }

// ──────────────────────────────────────────────────────
// 辅助函数：创建一个不带真实 websocket.Conn 的 ClientConnection
// 我们直接设置字段，避免依赖真实的 WebSocket 连接。
// ──────────────────────────────────────────────────────

func makeTestClientConnection(userID int64) *conn.ClientConnection {
	return &conn.ClientConnection{
		UserID:   userID,
		Conn:     nil, // nil — 测试中仅使用 SendCh
		SendCh:   make(chan []byte, 256),
		CloseCh:  make(chan struct{}),
		LastPing: time.Now(),
	}
}

// ──────────────────────────────────────────────────────
// PrivateMsgConsumer 测试
// ──────────────────────────────────────────────────────

func TestPrivateMsgConsumer_Process(t *testing.T) {
	logger := zap.NewNop()
	redisRepo := newMockRedisRepo()
	realCM := conn.NewConnectionManager()

	// 注册发送者和接收者；消费者完成处理后向发送者返回 serverAck。
	senderClient := makeTestClientConnection(100)
	realCM.Register(100, senderClient)
	receiverClient := makeTestClientConnection(200)
	realCM.Register(200, receiverClient)

	var pushedMsgs []string
	go func() {
		for msg := range receiverClient.SendCh {
			pushedMsgs = append(pushedMsgs, string(msg))
		}
	}()

	consumer := &PrivateMsgConsumer{
		ch:        nil,
		mysqlRepo: nil, // MySQL 将失败 — 我们在此点之前验证 Redis 操作
		redisRepo: redisRepo,
		cm:        realCM,
		logger:    logger,
	}

	msg := &model.PrivateMessage{
		ID:          1001,
		ClientMsgID: "private-client-1001",
		SenderID:    100,
		ReceiverID:  200,
		Content:     "hello",
		MsgType:     model.MsgTypeText,
		CreatedAt:   time.UnixMilli(1700000000000),
	}

	ctx := context.Background()

	// process() 会在 MySQL 插入处失败，因为 db 为 nil，
	// 但在此之前所有 Redis 操作都应该成功。
	err := consumer.process(ctx, msg)
	require.NoError(t, err, "即使 mysqlRepo 为 nil，process 也应该成功")

	// 验证 Redis 操作在 MySQL 失败之前已发生
	redisRepo.mu.Lock()

	// 1. 接收者收件箱中应有 readStatus=0 的消息
	receiverInbox := redisRepo.inboxWrites[200]
	assert.Len(t, receiverInbox, 1, "接收者应有 1 条收件箱消息")
	assert.Equal(t, 0, receiverInbox[0].ReadStatus, "接收者的消息应为未读")
	assert.Equal(t, int64(1001), receiverInbox[0].MsgID)
	assert.Equal(t, "p_100_200", receiverInbox[0].ConvID)

	// 2. 发送者收件箱中应有 readStatus=1 的消息
	senderInbox := redisRepo.inboxWrites[100]
	assert.Len(t, senderInbox, 1, "发送者应有 1 条收件箱消息")
	assert.Equal(t, 1, senderInbox[0].ReadStatus, "发送者的消息应为已读")
	assert.Equal(t, int64(1001), senderInbox[0].MsgID)

	// 3. 双方用户的 conv_list 都应更新
	assert.Len(t, redisRepo.convListUpdates[100], 1, "发送者 conv_list 已更新")
	assert.Len(t, redisRepo.convListUpdates[200], 1, "接收者 conv_list 已更新")
	assert.Equal(t, "p_100_200", redisRepo.convListUpdates[100][0].convID)
	assert.Equal(t, "p_100_200", redisRepo.convListUpdates[200][0].convID)

	// 4. 仅接收者的未读计数器应递增
	key := "200:p_100_200"
	assert.Equal(t, 1, redisRepo.unreadIncrements[key], "接收者未读数已递增")
	senderKey := "100:p_100_200"
	assert.Equal(t, 0, redisRepo.unreadIncrements[senderKey], "发送者未读数未递增")

	// 5. 双方收件箱都应被裁剪
	assert.Contains(t, redisRepo.trimmedInboxUsers, int64(200))
	assert.Contains(t, redisRepo.trimmedInboxUsers, int64(100))

	redisRepo.mu.Unlock()

	// 6. Redis 与持久化处理完成后，发送者才收到 serverAck。
	select {
	case raw := <-senderClient.SendCh:
		var wsMsg model.WsMessage
		require.NoError(t, json.Unmarshal(raw, &wsMsg))
		assert.Equal(t, protocol.TypeServerAck, wsMsg.Type)
		var ack model.ServerAck
		require.NoError(t, json.Unmarshal(wsMsg.Data, &ack))
		assert.Equal(t, "private-client-1001", ack.ClientMsgID)
		assert.Equal(t, int64(1001), ack.ServerMsgID)
	default:
		t.Fatal("发送者应在消息处理成功后收到 serverAck")
	}

	// 7. 验证 WebSocket 推送给接收者
	time.Sleep(50 * time.Millisecond)
	assert.Len(t, pushedMsgs, 1, "接收者应收到 1 条 WS 推送")
	if len(pushedMsgs) > 0 {
		var wsMsg model.WsMessage
		err := json.Unmarshal([]byte(pushedMsgs[0]), &wsMsg)
		require.NoError(t, err)
		assert.Equal(t, protocol.TypeMsg, wsMsg.Type, "推送类型应为 'msg'")

		var inboxMsg model.InboxMessage
		err = json.Unmarshal(wsMsg.Data, &inboxMsg)
		require.NoError(t, err)
		assert.Equal(t, int64(1001), inboxMsg.MsgID)
		assert.Equal(t, 0, inboxMsg.ReadStatus)
	}
}

func TestPrivateMsgConsumer_Deserialize(t *testing.T) {
	msg := &model.PrivateMessage{
		ID:          42,
		ClientMsgID: "private-deserialize-42",
		SenderID:    1,
		ReceiverID:  2,
		Content:     "test",
		MsgType:     model.MsgTypeText,
		CreatedAt:   time.Now(),
	}

	body, err := json.Marshal(msg)
	require.NoError(t, err)

	parsed, err := deserializePrivateMsg(body)
	require.NoError(t, err)
	assert.Equal(t, int64(42), parsed.ID)
	assert.Equal(t, "private-deserialize-42", parsed.ClientMsgID)
	assert.Equal(t, int64(1), parsed.SenderID)
	assert.Equal(t, int64(2), parsed.ReceiverID)

	// 测试无效消息（缺少 receiverID）
	invalidMsg := &model.PrivateMessage{SenderID: 1, ReceiverID: 0}
	invalidBody, err := json.Marshal(invalidMsg)
	require.NoError(t, err)
	_, err = deserializePrivateMsg(invalidBody)
	assert.Error(t, err, "应拒绝 receiverID 为零的消息")

	// 测试格式错误的 JSON
	_, err = deserializePrivateMsg([]byte("not json"))
	assert.Error(t, err)
}

func TestPrivateMsgConsumer_WriteInboxFailure(t *testing.T) {
	logger := zap.NewNop()
	redisRepo := newMockRedisRepo()
	redisRepo.writeInboxErr = fmt.Errorf("Redis 连接错误")
	realCM := conn.NewConnectionManager()
	senderClient := makeTestClientConnection(1)
	realCM.Register(1, senderClient)

	consumer := &PrivateMsgConsumer{
		ch:        nil,
		mysqlRepo: nil,
		redisRepo: redisRepo,
		cm:        realCM,
		logger:    logger,
	}

	msg := &model.PrivateMessage{
		ID:          100,
		ClientMsgID: "private-failed-100",
		SenderID:    1,
		ReceiverID:  2,
		Content:     "test",
		MsgType:     model.MsgTypeText,
		CreatedAt:   time.Now(),
	}

	ctx := context.Background()
	err := consumer.process(ctx, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "写入接收者收件箱失败")
	select {
	case pushed := <-senderClient.SendCh:
		t.Fatalf("消息处理失败时不应发送 serverAck: %s", pushed)
	default:
	}
}

func TestPrivateMsgConsumer_HandleDelivery_Malformed(t *testing.T) {
	// 测试格式错误的消息体在反序列化时失败
	_, err := deserializePrivateMsg([]byte("bad json"))
	assert.Error(t, err, "格式错误的消息体在反序列化时应失败")
}

// ──────────────────────────────────────────────────────
// GroupMsgConsumer 测试
// ──────────────────────────────────────────────────────

func TestGroupMsgConsumer_Process(t *testing.T) {
	logger := zap.NewNop()
	redisRepo := newMockRedisRepo()
	// 设置群组成员：群 5 有成员 [1, 2, 3]
	redisRepo.groupMembers[5] = []int64{1, 2, 3}

	realCM := conn.NewConnectionManager()
	senderClient := makeTestClientConnection(1)
	realCM.Register(1, senderClient)

	// 让成员 2 和 3 在线接收消息，发送者 1 在线接收 serverAck。
	client2 := makeTestClientConnection(2)
	realCM.Register(2, client2)

	client3 := makeTestClientConnection(3)
	realCM.Register(3, client3)

	var pushed2 []string
	var pushed3 []string
	go func() {
		for msg := range client2.SendCh {
			pushed2 = append(pushed2, string(msg))
		}
	}()
	go func() {
		for msg := range client3.SendCh {
			pushed3 = append(pushed3, string(msg))
		}
	}()

	consumer := &GroupMsgConsumer{
		ch:        nil,
		mysqlRepo: nil, // MySQL 将失败
		redisRepo: redisRepo,
		cm:        realCM,
		logger:    logger,
	}

	msg := &model.GroupMessage{
		ID:          2001,
		ClientMsgID: "group-client-2001",
		GroupID:     5,
		SenderID:    1,
		Content:     "group hello",
		MsgType:     model.MsgTypeText,
		GroupSeq:    10,
		CreatedAt:   time.UnixMilli(1700000000000),
	}

	ctx := context.Background()
	err := consumer.process(ctx, msg)
	require.NoError(t, err, "即使 mysqlRepo 为 nil，process 也应该成功")

	// 验证 Redis 操作在 MySQL 失败之前已发生
	redisRepo.mu.Lock()

	// 1. 群组发件箱中应有该消息
	outboxWrites := redisRepo.outboxWrites[5]
	assert.Len(t, outboxWrites, 1, "群组发件箱应有 1 条消息")
	assert.Equal(t, int64(2001), outboxWrites[0].MsgID)
	assert.Equal(t, model.ConvTypeGroup, outboxWrites[0].ConvType)
	assert.Equal(t, int64(10), outboxWrites[0].GroupSeq)
	assert.Equal(t, "g_5", outboxWrites[0].ConvID)

	// 2. 所有 3 个成员的 conv_list 都应更新
	assert.Len(t, redisRepo.convListUpdates[1], 1, "发送者 conv_list 已更新")
	assert.Len(t, redisRepo.convListUpdates[2], 1, "成员2 conv_list 已更新")
	assert.Len(t, redisRepo.convListUpdates[3], 1, "成员3 conv_list 已更新")
	assert.Equal(t, "g_5", redisRepo.convListUpdates[1][0].convID)
	assert.Equal(t, "g_5", redisRepo.convListUpdates[2][0].convID)

	// 3. 仅成员 2 和 3 的未读数应递增（发送者 1 不递增）
	assert.Equal(t, 1, redisRepo.unreadIncrements["2:g_5"], "成员2 未读数已递增")
	assert.Equal(t, 1, redisRepo.unreadIncrements["3:g_5"], "成员3 未读数已递增")
	assert.Equal(t, 0, redisRepo.unreadIncrements["1:g_5"], "发送者未读数未递增")

	// 4. 发件箱应被裁剪
	assert.Contains(t, redisRepo.trimmedOutboxGroups, int64(5))

	redisRepo.mu.Unlock()

	// 5. Redis 与持久化处理完成后，发送者收到携带 groupSeq 的 serverAck。
	select {
	case raw := <-senderClient.SendCh:
		var wsMsg model.WsMessage
		require.NoError(t, json.Unmarshal(raw, &wsMsg))
		assert.Equal(t, protocol.TypeServerAck, wsMsg.Type)
		var ack model.ServerAck
		require.NoError(t, json.Unmarshal(wsMsg.Data, &ack))
		assert.Equal(t, "group-client-2001", ack.ClientMsgID)
		assert.Equal(t, int64(2001), ack.ServerMsgID)
		assert.Equal(t, int64(10), ack.GroupSeq)
	default:
		t.Fatal("发送者应在群消息处理成功后收到 serverAck")
	}

	// 6. 验证 WebSocket 推送 — 仅成员 2 和 3，不含发送者 1
	time.Sleep(50 * time.Millisecond)
	assert.Len(t, pushed2, 1, "成员 2 应收到 1 条 WS 推送")
	assert.Len(t, pushed3, 1, "成员 3 应收到 1 条 WS 推送")

	if len(pushed2) > 0 {
		var wsMsg model.WsMessage
		err := json.Unmarshal([]byte(pushed2[0]), &wsMsg)
		require.NoError(t, err)
		assert.Equal(t, protocol.TypeMsg, wsMsg.Type)

		var inboxMsg model.InboxMessage
		err = json.Unmarshal(wsMsg.Data, &inboxMsg)
		require.NoError(t, err)
		assert.Equal(t, int64(2001), inboxMsg.MsgID)
		assert.Equal(t, int64(10), inboxMsg.GroupSeq)
		assert.Equal(t, model.ConvTypeGroup, inboxMsg.ConvType)
	}
}

func TestGroupMsgConsumer_Process_NoMembers(t *testing.T) {
	logger := zap.NewNop()
	redisRepo := newMockRedisRepo()
	// 群 10 没有成员
	redisRepo.groupMembers[10] = []int64{}

	realCM := conn.NewConnectionManager()

	consumer := &GroupMsgConsumer{
		ch:        nil,
		mysqlRepo: nil,
		redisRepo: redisRepo,
		cm:        realCM,
		logger:    logger,
	}

	msg := &model.GroupMessage{
		ID:        3001,
		GroupID:   10,
		SenderID:  1,
		Content:   "hello empty group",
		MsgType:   model.MsgTypeText,
		GroupSeq:  1,
		CreatedAt: time.Now(),
	}

	ctx := context.Background()
	err := consumer.process(ctx, msg)
	require.NoError(t, err, "即使 mysqlRepo 为 nil，process 也应该成功")

	// 验证发件箱仍然写入（即使没有成员，发件箱也会持久化）
	redisRepo.mu.Lock()
	assert.Len(t, redisRepo.outboxWrites[10], 1, "即使没有成员，发件箱也应写入")
	redisRepo.mu.Unlock()
}

func TestGroupMsgConsumer_GetMembersFailure(t *testing.T) {
	logger := zap.NewNop()
	redisRepo := newMockRedisRepo()
	redisRepo.getGroupMembersErr = fmt.Errorf("Redis 不可用")

	realCM := conn.NewConnectionManager()
	senderClient := makeTestClientConnection(1)
	realCM.Register(1, senderClient)

	consumer := &GroupMsgConsumer{
		ch:        nil,
		mysqlRepo: nil,
		redisRepo: redisRepo,
		cm:        realCM,
		logger:    logger,
	}

	msg := &model.GroupMessage{
		ID:          4001,
		ClientMsgID: "group-failed-4001",
		GroupID:     5,
		SenderID:    1,
		Content:     "test",
		MsgType:     model.MsgTypeText,
		GroupSeq:    1,
		CreatedAt:   time.Now(),
	}

	ctx := context.Background()
	err := consumer.process(ctx, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "获取群组成员失败")
	select {
	case pushed := <-senderClient.SendCh:
		t.Fatalf("消息处理失败时不应发送 serverAck: %s", pushed)
	default:
	}
}

func TestGroupMsgConsumer_Deserialize(t *testing.T) {
	msg := &model.GroupMessage{
		ID:          42,
		ClientMsgID: "group-deserialize-42",
		GroupID:     5,
		SenderID:    1,
		Content:     "test",
		MsgType:     model.MsgTypeText,
		GroupSeq:    10,
		CreatedAt:   time.Now(),
	}

	body, err := json.Marshal(msg)
	require.NoError(t, err)

	parsed, err := deserializeGroupMsg(body)
	require.NoError(t, err)
	assert.Equal(t, int64(42), parsed.ID)
	assert.Equal(t, "group-deserialize-42", parsed.ClientMsgID)
	assert.Equal(t, int64(5), parsed.GroupID)
	assert.Equal(t, int64(10), parsed.GroupSeq)

	// 测试无效消息（缺少 groupID）
	invalidMsg := &model.GroupMessage{GroupID: 0, SenderID: 1}
	invalidBody, err := json.Marshal(invalidMsg)
	require.NoError(t, err)
	_, err = deserializeGroupMsg(invalidBody)
	assert.Error(t, err, "应拒绝 groupID 为零的消息")

	// 测试格式错误的 JSON
	_, err = deserializeGroupMsg([]byte("not json"))
	assert.Error(t, err)
}

// ──────────────────────────────────────────────────────
// 共享辅助函数测试
// ──────────────────────────────────────────────────────

func TestTruncateContent(t *testing.T) {
	assert.Equal(t, "hello", truncateContent("hello", 50))
	assert.Equal(t, "a very long message that needs...", truncateContent("a very long message that needs truncation", 30))
	// 短的多行内容不会被截断（总长度 <= maxLen）
	assert.Equal(t, "first line\nsecond line", truncateContent("first line\nsecond line", 50))
	// 长多行内容中，第一行本身就超过 maxLen：仅截断第一行
	assert.Equal(t, "a very long first line that ex...", truncateContent("a very long first line that exceeds max\nsecond line", 30))
}

func TestBuildPrivateConvSummary(t *testing.T) {
	msg := &model.PrivateMessage{
		ID:         1,
		SenderID:   100,
		ReceiverID: 200,
		Content:    "hello there",
		MsgType:    model.MsgTypeText,
		CreatedAt:  time.UnixMilli(1700000000000),
	}

	convID := model.BuildConvID(model.ConvTypePrivate, 100, 200)
	summary := buildPrivateConvSummary(convID, msg)

	assert.Equal(t, "p_100_200", summary.ConvID)
	assert.Equal(t, model.ConvTypePrivate, summary.ConvType)
	assert.Equal(t, int64(200), summary.TargetID)
	assert.Equal(t, "hello there", summary.LastMsg)
	assert.Equal(t, int64(1700000000000), summary.LastMsgTime)
}

func TestBuildGroupConvSummary(t *testing.T) {
	msg := &model.GroupMessage{
		ID:        1,
		GroupID:   5,
		SenderID:  10,
		Content:   "group hello",
		MsgType:   model.MsgTypeText,
		GroupSeq:  3,
		CreatedAt: time.UnixMilli(1700000000000),
	}

	convID := model.BuildConvID(model.ConvTypeGroup, 5, 0)
	summary := buildGroupConvSummary(convID, msg)

	assert.Equal(t, "g_5", summary.ConvID)
	assert.Equal(t, model.ConvTypeGroup, summary.ConvType)
	assert.Equal(t, int64(5), summary.TargetID)
	assert.Equal(t, "group hello", summary.LastMsg)
	assert.Equal(t, int64(1700000000000), summary.LastMsgTime)
}

func TestPushToConnection_Offline(t *testing.T) {
	logger := zap.NewNop()
	cm := conn.NewConnectionManager()

	// 用户 999 不在线 — 推送应被静默丢弃
	pushToConnection(cm, logger, 999, protocol.TypeMsg, &model.InboxMessage{MsgID: 1})
	// 无 panic，无错误 — 仅静默丢弃
}

func TestPushToConnection_Online(t *testing.T) {
	logger := zap.NewNop()
	cm := conn.NewConnectionManager()

	// 手动创建一个客户端连接（不带真实的 websocket.Conn）
	client := makeTestClientConnection(100)
	cm.Register(100, client)

	var received []string
	go func() {
		for msg := range client.SendCh {
			received = append(received, string(msg))
		}
	}()

	pushToConnection(cm, logger, 100, protocol.TypeMsg, &model.InboxMessage{MsgID: 42})

	time.Sleep(50 * time.Millisecond)
	assert.Len(t, received, 1)

	if len(received) > 0 {
		var wsMsg model.WsMessage
		err := json.Unmarshal([]byte(received[0]), &wsMsg)
		require.NoError(t, err)
		assert.Equal(t, protocol.TypeMsg, wsMsg.Type)
	}
}

func TestPushToConnection_BufferFull(t *testing.T) {
	logger := zap.NewNop()
	cm := conn.NewConnectionManager()

	// 创建一个 SendCh 缓冲区很小的客户端
	client := &conn.ClientConnection{
		UserID:   100,
		Conn:     nil,
		SendCh:   make(chan []byte, 2), // 微小缓冲区
		CloseCh:  make(chan struct{}),
		LastPing: time.Now(),
	}
	cm.Register(100, client)

	// 填满缓冲区
	client.SendCh <- []byte("msg1")
	client.SendCh <- []byte("msg2")

	// 此推送应被丢弃（缓冲区已满）— 无 panic
	pushToConnection(cm, logger, 100, protocol.TypeMsg, &model.InboxMessage{MsgID: 99})
	// 无 panic — 仅记录日志并丢弃
}
