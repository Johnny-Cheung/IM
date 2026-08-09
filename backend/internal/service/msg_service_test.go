package service

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/protocol"
	redislua "github.com/goim/goim/internal/redis"
)

// ──────────────────────────────────────────────────────
// Mock 实现
// ──────────────────────────────────────────────────────

// mockRedisRepo 实现 repository.RedisRepo 接口，用于测试。
type mockRedisRepo struct {
	mu sync.Mutex

	// Lua 脚本的可配置返回值
	privateCheckResult *redislua.PrivateMsgCheckResult
	privateCheckErr    error
	groupCheckResult   *redislua.GroupMsgCheckResult
	groupCheckErr      error
	inboxMarkReadCount int64
	inboxMarkReadErr   error
	revokeResult       bool
	revokeErr          error

	// 捕获的数据
	inboxMessages  map[int64][]model.InboxMessage // userID -> 消息列表
	outboxMessages map[int64][]model.InboxMessage // groupID -> 消息列表
	groupMembers   map[int64][]int64              // userID -> groupID 列表
	groupReadPos   map[string]int64               // "userID:convID" -> seq
	groupSeq       map[int64]int64                // groupID -> 群最新序号
	unreadMap      map[string]int64               // convID -> 计数
	convList       map[int64][]model.ConvSummary  // userID -> 会话摘要列表

	// 方法调用追踪
	privateCheckCalled bool
	groupCheckCalled   bool
	publishCalled      bool
	markReadCalled     bool
	revokeCalled       bool
}

func newMockRedisRepo() *mockRedisRepo {
	return &mockRedisRepo{
		inboxMessages:  make(map[int64][]model.InboxMessage),
		outboxMessages: make(map[int64][]model.InboxMessage),
		groupMembers:   make(map[int64][]int64),
		groupReadPos:   make(map[string]int64),
		groupSeq:       make(map[int64]int64),
		unreadMap:      make(map[string]int64),
		convList:       make(map[int64][]model.ConvSummary),
	}
}

func (m *mockRedisRepo) WriteInbox(_ context.Context, userID int64, msg *model.InboxMessage) error {
	m.mu.Lock()
	m.inboxMessages[userID] = append(m.inboxMessages[userID], *msg)
	m.mu.Unlock()
	return nil
}

func (m *mockRedisRepo) WriteOutbox(_ context.Context, groupID int64, msg *model.InboxMessage) error {
	m.mu.Lock()
	m.outboxMessages[groupID] = append(m.outboxMessages[groupID], *msg)
	m.mu.Unlock()
	return nil
}

func (m *mockRedisRepo) ReadInbox(_ context.Context, userID int64, lastSyncTime, lastSyncMsgID int64, batchSize int) ([]model.InboxMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msgs := m.inboxMessages[userID]
	var result []model.InboxMessage
	for _, msg := range msgs {
		if msg.Timestamp > lastSyncTime ||
			(msg.Timestamp == lastSyncTime && (lastSyncMsgID == 0 || msg.MsgID > lastSyncMsgID)) {
			result = append(result, msg)
			if len(result) >= batchSize {
				break
			}
		}
	}
	return result, nil
}

func (m *mockRedisRepo) ReadOutbox(_ context.Context, groupID int64, lastSyncTime, lastSyncMsgID int64, limit int) ([]model.InboxMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msgs := m.outboxMessages[groupID]
	var result []model.InboxMessage
	for _, msg := range msgs {
		if msg.Timestamp > lastSyncTime ||
			(msg.Timestamp == lastSyncTime && (lastSyncMsgID == 0 || msg.MsgID > lastSyncMsgID)) {
			result = append(result, msg)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *mockRedisRepo) UpdateConvList(_ context.Context, userID int64, convID string, summary string, timestamp int64) error {
	m.mu.Lock()
	m.convList[userID] = append(m.convList[userID], model.ConvSummary{ConvID: convID, LastMsgTime: timestamp})
	m.mu.Unlock()
	return nil
}

func (m *mockRedisRepo) GetConvList(_ context.Context, userID int64) ([]model.ConvSummary, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.convList[userID], nil
}

func (m *mockRedisRepo) IncrementUnread(_ context.Context, userID int64, convID string) error {
	m.mu.Lock()
	m.unreadMap[convID]++
	m.mu.Unlock()
	return nil
}

func (m *mockRedisRepo) ClearUnread(_ context.Context, userID int64, convID string) error {
	m.mu.Lock()
	m.unreadMap[convID] = 0
	m.mu.Unlock()
	return nil
}

func (m *mockRedisRepo) GetUnreadMap(_ context.Context, userID int64) (map[string]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]int64)
	for k, v := range m.unreadMap {
		result[k] = v
	}
	return result, nil
}

func (m *mockRedisRepo) SetGroupReadPos(_ context.Context, userID int64, convID string, seq int64) error {
	m.mu.Lock()
	m.groupReadPos[convID] = seq
	m.mu.Unlock()
	return nil
}

func (m *mockRedisRepo) GetGroupReadPos(_ context.Context, userID int64, convID string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.groupReadPos[convID], nil
}

func (m *mockRedisRepo) GetGroupSeq(_ context.Context, groupID int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.groupSeq[groupID], nil
}

func (m *mockRedisRepo) GetGroupMemberships(_ context.Context, userID int64) ([]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.groupMembers[userID], nil
}

func (m *mockRedisRepo) GetGroupMembers(_ context.Context, groupID int64) ([]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.groupMembers[groupID], nil
}

func (m *mockRedisRepo) AddGroupMemberRedis(_ context.Context, _ int64, _ int64) error    { return nil }
func (m *mockRedisRepo) RemoveGroupMemberRedis(_ context.Context, _ int64, _ int64) error { return nil }

func (m *mockRedisRepo) CheckDuplicate(_ context.Context, userID int64, clientMsgID string) (bool, error) {
	return false, nil
}

func (m *mockRedisRepo) TrimInbox(_ context.Context, userID int64, maxCount int) error   { return nil }
func (m *mockRedisRepo) TrimOutbox(_ context.Context, groupID int64, maxCount int) error { return nil }
func (m *mockRedisRepo) TrimInboxByTime(_ context.Context, userID int64, beforeTimestamp int64) error {
	return nil
}
func (m *mockRedisRepo) TrimOutboxByTime(_ context.Context, groupID int64, beforeTimestamp int64) error {
	return nil
}
func (m *mockRedisRepo) TrimConvListByTime(_ context.Context, userID int64, beforeTimestamp int64) error {
	return nil
}
func (m *mockRedisRepo) TrimTimelineByTime(_ context.Context, userID int64, beforeTimestamp int64) error {
	return nil
}

func (m *mockRedisRepo) ExecPrivateMsgCheck(_ context.Context, senderID, receiverID int64, clientMsgID string) (*redislua.PrivateMsgCheckResult, error) {
	m.mu.Lock()
	m.privateCheckCalled = true
	m.mu.Unlock()
	return m.privateCheckResult, m.privateCheckErr
}

func (m *mockRedisRepo) ExecGroupMsgCheck(_ context.Context, groupID, senderID int64, clientMsgID string) (*redislua.GroupMsgCheckResult, error) {
	m.mu.Lock()
	m.groupCheckCalled = true
	m.mu.Unlock()
	return m.groupCheckResult, m.groupCheckErr
}

func (m *mockRedisRepo) ExecInboxMarkRead(_ context.Context, userID int64, convID string) (int64, error) {
	m.mu.Lock()
	m.markReadCalled = true
	m.mu.Unlock()
	return m.inboxMarkReadCount, m.inboxMarkReadErr
}

func (m *mockRedisRepo) ExecRevokeMsg(_ context.Context, userID int64, convID string, msgID int64, revokeMsgJSON string, nowTimestamp int64) (bool, error) {
	m.mu.Lock()
	m.revokeCalled = true
	m.mu.Unlock()
	return m.revokeResult, m.revokeErr
}

func (m *mockRedisRepo) PublishMomentFeed(_ context.Context, _ int64, _ int64, _ int64) error {
	return nil
}

func (m *mockRedisRepo) GetMomentFeed(_ context.Context, _ int64, _ int64, _ int) ([]int64, error) {
	return nil, nil
}
func (m *mockRedisRepo) FanoutMomentFeed(_ context.Context, _ []int64, _ int64, _ int64, _ int) error {
	return nil
}
func (m *mockRedisRepo) AddToOutbox(_ context.Context, _ int64, _ int64, _ int64, _ int) error {
	return nil
}
func (m *mockRedisRepo) MarkBigUser(_ context.Context, _ int64) error { return nil }
func (m *mockRedisRepo) FilterBigUsers(_ context.Context, _ []int64) ([]int64, error) {
	return nil, nil
}
func (m *mockRedisRepo) GetTimelinePage(_ context.Context, _ int64, _ int64, _ int64, _ int) ([]model.FeedEntry, error) {
	return nil, nil
}
func (m *mockRedisRepo) GetOutboxPage(_ context.Context, _ int64, _ int64, _ int64, _ int) ([]model.FeedEntry, error) {
	return nil, nil
}
func (m *mockRedisRepo) SetFriendCache(_ context.Context, _ int64, _ int64) error { return nil }

// mockMQRepo 实现 repository.MQRepo 接口，用于测试。
type mockMQRepo struct {
	mu             sync.Mutex
	privateMsgs    []*model.PrivateMessage
	groupMsgs      []*model.GroupMessage
	publishErr     error
	publishPrivate bool
	publishGroup   bool
}

func newMockMQRepo() *mockMQRepo {
	return &mockMQRepo{}
}

func (m *mockMQRepo) PublishPrivateMsg(_ context.Context, msg *model.PrivateMessage) error {
	m.mu.Lock()
	m.privateMsgs = append(m.privateMsgs, msg)
	m.publishPrivate = true
	m.mu.Unlock()
	return m.publishErr
}

func (m *mockMQRepo) PublishGroupMsg(_ context.Context, msg *model.GroupMessage) error {
	m.mu.Lock()
	m.groupMsgs = append(m.groupMsgs, msg)
	m.publishGroup = true
	m.mu.Unlock()
	return m.publishErr
}

func (m *mockMQRepo) PublishMomentPush(_ context.Context, _ *model.Moment) error {
	return nil
}

// ──────────────────────────────────────────────────────
// 辅助函数
// ──────────────────────────────────────────────────────

func testLogger() *zap.Logger {
	l, _ := zap.NewDevelopment()
	return l
}

// drainSendCh 从客户端的 SendCh 读取一条消息，带超时。
func drainSendCh(client *conn.ClientConnection) ([]byte, bool) {
	select {
	case msg := <-client.SendCh:
		return msg, true
	case <-time.After(2 * time.Second):
		return nil, false
	}
}

// decodeWsMessage 将原始字节解析为 WsMessage 信封。
func decodeWsMessage(raw []byte) (*model.WsMessage, error) {
	return protocol.DecodeMsg(raw)
}

// ──────────────────────────────────────────────────────
// 测试用例
// ──────────────────────────────────────────────────────

func TestPrivateMsgSend_Success(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.privateCheckResult = &redislua.PrivateMsgCheckResult{
		ErrCode:   redislua.PMErrOK,
		MsgID:     1001,
		IsOnline:  true,
		IsFriend:  true,
		IsBlocked: false,
	}
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	// 注册发送方和接收方的连接
	sender := conn.NewClientConnection(1, nil)
	receiver := conn.NewClientConnection(2, nil)
	cm.Register(1, sender)
	cm.Register(2, receiver)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	// 构建请求
	req := model.SendMessage{
		ClientMsgID: "client-msg-1",
		ConvType:    model.ConvTypePrivate,
		ToID:        2,
		MsgType:     model.MsgTypeText,
		Content:     "hello",
		Timestamp:   1000000,
	}
	data, _ := json.Marshal(req)

	svc.HandleSendMessage(1, data)

	// 验证：Lua 检查已被调用
	assert.True(t, redisMock.privateCheckCalled)

	// 验证：MQ 发布已被调用
	assert.True(t, mqMock.publishPrivate)
	assert.Len(t, mqMock.privateMsgs, 1)
	assert.Equal(t, int64(1001), mqMock.privateMsgs[0].ID)
	assert.Equal(t, int64(1), mqMock.privateMsgs[0].SenderID)
	assert.Equal(t, int64(2), mqMock.privateMsgs[0].ReceiverID)
	assert.Equal(t, "hello", mqMock.privateMsgs[0].Content)
	assert.Equal(t, "client-msg-1", mqMock.privateMsgs[0].ClientMsgID)

	// serverAck 由 MQ 消费者在消息写入 Redis 后发送，服务层不能提前确认。
	select {
	case pushed := <-sender.SendCh:
		t.Fatalf("服务层不应提前向发送方发送 serverAck: %s", pushed)
	default:
	}

	// 接收方推送由 MQ 消费者在 Redis 写入成功后统一完成。
	select {
	case pushed := <-receiver.SendCh:
		t.Fatalf("服务层不应直接向接收方推送消息: %s", pushed)
	default:
	}
}

func TestPrivateMsgSend_NotFriend(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.privateCheckResult = &redislua.PrivateMsgCheckResult{
		ErrCode:   redislua.PMErrNotFriend,
		MsgID:     0,
		IsOnline:  false,
		IsFriend:  false,
		IsBlocked: false,
	}
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.SendMessage{
		ClientMsgID: "client-msg-2",
		ConvType:    model.ConvTypePrivate,
		ToID:        2,
		MsgType:     model.MsgTypeText,
		Content:     "hello",
		Timestamp:   1000000,
	}
	data, _ := json.Marshal(req)

	svc.HandleSendMessage(1, data)

	// 验证：错误已发送给发送方
	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, err := decodeWsMessage(msg)
	assert.NoError(t, err)
	assert.Equal(t, protocol.TypeError, wsMsg.Type)

	var wsErr model.WsError
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &wsErr))
	assert.Equal(t, redislua.CodePMNotFriend, wsErr.Code)
	assert.Equal(t, ErrNotFriend, wsErr.Message)

	// 验证：MQ 发布未被调用
	assert.False(t, mqMock.publishPrivate)
}

func TestPrivateMsgSend_Blocked(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.privateCheckResult = &redislua.PrivateMsgCheckResult{
		ErrCode:   redislua.PMErrBlocked,
		MsgID:     0,
		IsOnline:  false,
		IsFriend:  true,
		IsBlocked: true,
	}
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.SendMessage{
		ClientMsgID: "client-msg-3",
		ConvType:    model.ConvTypePrivate,
		ToID:        2,
		MsgType:     model.MsgTypeText,
		Content:     "hello",
		Timestamp:   1000000,
	}
	data, _ := json.Marshal(req)

	svc.HandleSendMessage(1, data)

	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeError, wsMsg.Type)

	var wsErr model.WsError
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &wsErr))
	assert.Equal(t, redislua.CodePMBlocked, wsErr.Code)
	assert.Equal(t, ErrBlocked, wsErr.Message)
}

func TestPrivateMsgSend_Duplicate(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.privateCheckResult = &redislua.PrivateMsgCheckResult{
		ErrCode: redislua.PMErrDuplicate,
	}
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.SendMessage{
		ClientMsgID: "client-msg-dup",
		ConvType:    model.ConvTypePrivate,
		ToID:        2,
		MsgType:     model.MsgTypeText,
		Content:     "hello",
		Timestamp:   1000000,
	}
	data, _ := json.Marshal(req)

	svc.HandleSendMessage(1, data)

	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeError, wsMsg.Type)

	var wsErr model.WsError
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &wsErr))
	assert.Equal(t, redislua.CodePMDuplicate, wsErr.Code)
}

func TestPrivateMsgSend_ReceiverOffline(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.privateCheckResult = &redislua.PrivateMsgCheckResult{
		ErrCode:   redislua.PMErrOK,
		MsgID:     2001,
		IsOnline:  false, // 接收方离线
		IsFriend:  true,
		IsBlocked: false,
	}
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)
	// cm 中未注册接收方（离线）

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.SendMessage{
		ClientMsgID: "client-msg-offline",
		ConvType:    model.ConvTypePrivate,
		ToID:        2,
		MsgType:     model.MsgTypeText,
		Content:     "hello offline",
		Timestamp:   1000000,
	}
	data, _ := json.Marshal(req)

	svc.HandleSendMessage(1, data)

	// 验证：MQ 仍然发布（当接收方上线时通过同步投递）
	assert.True(t, mqMock.publishPrivate)
	assert.Equal(t, "client-msg-offline", mqMock.privateMsgs[0].ClientMsgID)
	select {
	case pushed := <-sender.SendCh:
		t.Fatalf("服务层不应提前向发送方发送 serverAck: %s", pushed)
	default:
	}
}

func TestPrivateMsgSend_LuaError(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.privateCheckResult = nil
	redisMock.privateCheckErr = context.DeadlineExceeded
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.SendMessage{
		ClientMsgID: "client-msg-err",
		ConvType:    model.ConvTypePrivate,
		ToID:        2,
		MsgType:     model.MsgTypeText,
		Content:     "hello",
		Timestamp:   1000000,
	}
	data, _ := json.Marshal(req)

	svc.HandleSendMessage(1, data)

	// 验证：内部错误已发送给发送方
	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeError, wsMsg.Type)

	var wsErr model.WsError
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &wsErr))
	assert.Equal(t, 500, wsErr.Code)
}

func TestGroupMsgSend_Success(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.groupCheckResult = &redislua.GroupMsgCheckResult{
		ErrCode:  redislua.GMErrOK,
		MsgID:    3001,
		GroupSeq: 10,
		IsMember: true,
		IsMuted:  false,
	}
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.SendMessage{
		ClientMsgID: "client-msg-group-1",
		ConvType:    model.ConvTypeGroup,
		ToID:        100, // 群组ID
		MsgType:     model.MsgTypeText,
		Content:     "group hello",
		Timestamp:   1000000,
	}
	data, _ := json.Marshal(req)

	svc.HandleSendMessage(1, data)

	// 验证：Lua 检查已被调用
	assert.True(t, redisMock.groupCheckCalled)

	// 验证：MQ 发布已被调用
	assert.True(t, mqMock.publishGroup)
	assert.Len(t, mqMock.groupMsgs, 1)
	assert.Equal(t, int64(3001), mqMock.groupMsgs[0].ID)
	assert.Equal(t, int64(100), mqMock.groupMsgs[0].GroupID)
	assert.Equal(t, int64(10), mqMock.groupMsgs[0].GroupSeq)
	assert.Equal(t, "client-msg-group-1", mqMock.groupMsgs[0].ClientMsgID)

	// serverAck 由 MQ 消费者在消息写入 Redis 后发送，服务层不能提前确认。
	select {
	case pushed := <-sender.SendCh:
		t.Fatalf("服务层不应提前向发送方发送 serverAck: %s", pushed)
	default:
	}
}

func TestGroupMsgSend_NotMember(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.groupCheckResult = &redislua.GroupMsgCheckResult{
		ErrCode:  redislua.GMErrNotMember,
		MsgID:    0,
		GroupSeq: 0,
	}
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.SendMessage{
		ClientMsgID: "client-msg-group-notmem",
		ConvType:    model.ConvTypeGroup,
		ToID:        100,
		MsgType:     model.MsgTypeText,
		Content:     "hello",
		Timestamp:   1000000,
	}
	data, _ := json.Marshal(req)

	svc.HandleSendMessage(1, data)

	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeError, wsMsg.Type)

	var wsErr model.WsError
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &wsErr))
	assert.Equal(t, redislua.CodeGMNotMember, wsErr.Code)
	assert.Equal(t, ErrNotMember, wsErr.Message)
}

func TestGroupMsgSend_Muted(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.groupCheckResult = &redislua.GroupMsgCheckResult{
		ErrCode:  redislua.GMErrMuted,
		MsgID:    0,
		GroupSeq: 0,
		IsMember: true,
		IsMuted:  true,
	}
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.SendMessage{
		ClientMsgID: "client-msg-group-muted",
		ConvType:    model.ConvTypeGroup,
		ToID:        100,
		MsgType:     model.MsgTypeText,
		Content:     "hello",
		Timestamp:   1000000,
	}
	data, _ := json.Marshal(req)

	svc.HandleSendMessage(1, data)

	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeError, wsMsg.Type)

	var wsErr model.WsError
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &wsErr))
	assert.Equal(t, redislua.CodeGMMuted, wsErr.Code)
	assert.Equal(t, ErrMuted, wsErr.Message)
}

func TestDeliverAck(t *testing.T) {
	redisMock := newMockRedisRepo()
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	ack := model.DeliverAck{ServerMsgID: 1001}
	data, _ := json.Marshal(ack)

	// DeliverAck 是信息性的 — 仅验证它不会 panic
	svc.HandleDeliverAck(1, data)
	// 不应发送错误，不应有状态变更
}

func TestReadAck_Success(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.inboxMarkReadCount = 3
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.ReadAck{ConvID: "p_1_2"}
	data, _ := json.Marshal(req)

	svc.HandleReadAck(1, data)

	// 验证：Lua 标记已读已被调用
	assert.True(t, redisMock.markReadCalled)
	assert.Equal(t, int64(0), redisMock.unreadMap["p_1_2"])
}

func TestReadAck_GroupWritesWatermark(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.unreadMap["g_100"] = 10 // hash 残留不应参与群未读
	cm := conn.NewConnectionManager()
	svc := NewMsgService(redisMock, newMockMQRepo(), cm, testLogger())
	data, _ := json.Marshal(model.ReadAck{ConvID: "g_100", LastReadGroupSeq: 42})
	svc.HandleReadAck(1, data)
	assert.False(t, redisMock.markReadCalled, "群聊已读不再走收件箱 Lua")
	assert.Equal(t, int64(42), redisMock.groupReadPos["g_100"], "群已读水位线应写入客户端携带的序号")
}

func TestReadAck_GroupFallbackToLatestSeq(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.groupSeq[100] = 57
	cm := conn.NewConnectionManager()
	svc := NewMsgService(redisMock, newMockMQRepo(), cm, testLogger())
	// 老客户端不带 lastReadGroupSeq → 降级为标记到当前最新（行为等同旧的整会话清零）
	data, _ := json.Marshal(model.ReadAck{ConvID: "g_100"})
	svc.HandleReadAck(1, data)
	assert.Equal(t, int64(57), redisMock.groupReadPos["g_100"], "缺省序号时水位线应落到群最新 seq")
}

func TestReadAck_GroupInvalidConvID(t *testing.T) {
	redisMock := newMockRedisRepo()
	cm := conn.NewConnectionManager()
	svc := NewMsgService(redisMock, newMockMQRepo(), cm, testLogger())
	data, _ := json.Marshal(model.ReadAck{ConvID: "g_abc"})
	svc.HandleReadAck(1, data)
	assert.NotContains(t, redisMock.groupReadPos, "g_abc", "非法群 ID 不应写入水位线")
}

func TestReadAck_InvalidFormat(t *testing.T) {
	redisMock := newMockRedisRepo()
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	// 发送无效的 JSON
	svc.HandleReadAck(1, []byte("invalid json"))

	// 验证：错误已发送
	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeError, wsMsg.Type)
}

func TestSyncReq_WithMessages(t *testing.T) {
	redisMock := newMockRedisRepo()
	// 为用户 1 预填充收件箱
	redisMock.inboxMessages[1] = []model.InboxMessage{
		{MsgID: 1, ConvID: "p_1_2", ConvType: 1, FromID: 2, ToID: 1, Content: "hi", Timestamp: 1100000},
		{MsgID: 2, ConvID: "p_1_2", ConvType: 1, FromID: 2, ToID: 1, Content: "there", Timestamp: 1200000},
	}
	// 用户 1 在群组 100 中
	redisMock.groupMembers[1] = []int64{100}
	// 群组 100 的发件箱
	redisMock.outboxMessages[100] = []model.InboxMessage{
		{MsgID: 3, ConvID: "g_100", ConvType: 2, FromID: 5, ToID: 100, Content: "group msg", Timestamp: 1300000, GroupSeq: 5},
	}
	// 即使群已读位置已经超过消息序号，重新同步历史时也不能隐藏消息。
	redisMock.groupReadPos["g_100"] = 6
	// 未读映射表
	redisMock.unreadMap = map[string]int64{"p_1_2": 2, "g_100": 1}

	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	client := conn.NewClientConnection(1, nil)
	cm.Register(1, client)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.SyncReq{LastSyncTime: 1000000, BatchSize: 50}
	data, _ := json.Marshal(req)

	svc.HandleSyncReq(1, data)

	// 应该收到两条消息：SyncBatch 和 ConvSync
	msg1, ok := drainSendCh(client)
	assert.True(t, ok, "期望在 SendCh 上收到 SyncBatch")
	wsMsg1, err := decodeWsMessage(msg1)
	assert.NoError(t, err)
	assert.Equal(t, protocol.TypeSyncBatch, wsMsg1.Type)

	var batch model.SyncBatch
	assert.NoError(t, json.Unmarshal(wsMsg1.Data, &batch))
	// 应包含收件箱消息 + 群组历史消息
	assert.True(t, len(batch.Messages) >= 2) // 至少 2 条私聊 + 1 条群组消息

	// 群历史恢复由复合游标控制，不受已读位置过滤。
	groupFound := false
	for _, m := range batch.Messages {
		if m.ConvID == "g_100" && m.GroupSeq == 5 {
			groupFound = true
		}
	}
	assert.True(t, groupFound, "已读群消息在从零同步时仍应作为历史返回")
	assert.Equal(t, int64(6), redisMock.groupReadPos["g_100"], "同步本身不应推进或改写群已读位置")

	msg2, ok := drainSendCh(client)
	assert.True(t, ok, "期望在 SendCh 上收到 ConvSync")
	wsMsg2, _ := decodeWsMessage(msg2)
	assert.Equal(t, protocol.TypeConvSync, wsMsg2.Type)

	var convSync model.ConvSync
	assert.NoError(t, json.Unmarshal(wsMsg2.Data, &convSync))
	assert.Equal(t, int64(2), convSync.UnreadMap["p_1_2"])
}

func TestSyncReq_DefaultBatchSize(t *testing.T) {
	redisMock := newMockRedisRepo()
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	client := conn.NewClientConnection(1, nil)
	cm.Register(1, client)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	// 发送 batchSize=0 的 SyncReq（应默认使用 50）
	req := model.SyncReq{LastSyncTime: 0, BatchSize: 0}
	data, _ := json.Marshal(req)

	svc.HandleSyncReq(1, data)

	// 仍然应收到 SyncBatch
	msg, ok := drainSendCh(client)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeSyncBatch, wsMsg.Type)

	var batch model.SyncBatch
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &batch))
	// 当没有消息时，syncTime 设置为当前服务器时间（非零）
	assert.True(t, batch.SyncTime > 0, "即使没有消息，syncTime 也应为非零值")
	assert.False(t, batch.HasMore)
}

func TestSyncReq_CompositeCursorSameTimestamp(t *testing.T) {
	redisMock := newMockRedisRepo()
	const timestamp = int64(1700000000000)
	redisMock.inboxMessages[1] = []model.InboxMessage{
		{MsgID: 101, ConvID: "p_1_2", ConvType: 1, FromID: 2, ToID: 1, Timestamp: timestamp},
		{MsgID: 102, ConvID: "p_1_2", ConvType: 1, FromID: 2, ToID: 1, Timestamp: timestamp},
		{MsgID: 103, ConvID: "p_1_2", ConvType: 1, FromID: 2, ToID: 1, Timestamp: timestamp},
	}

	cm := conn.NewConnectionManager()
	client := conn.NewClientConnection(1, nil)
	cm.Register(1, client)
	svc := NewMsgService(redisMock, newMockMQRepo(), cm, testLogger())

	readBatch := func(req model.SyncReq) model.SyncBatch {
		data, _ := json.Marshal(req)
		svc.HandleSyncReq(1, data)
		raw, ok := drainSendCh(client)
		assert.True(t, ok)
		wsMsg, err := decodeWsMessage(raw)
		assert.NoError(t, err)
		assert.Equal(t, protocol.TypeSyncBatch, wsMsg.Type)
		var batch model.SyncBatch
		assert.NoError(t, json.Unmarshal(wsMsg.Data, &batch))
		_, ok = drainSendCh(client) // ConvSync
		assert.True(t, ok)
		return batch
	}

	first := readBatch(model.SyncReq{LastSyncTime: 0, BatchSize: 2})
	assert.True(t, first.HasMore)
	assert.Equal(t, []int64{101, 102}, []int64{first.Messages[0].MsgID, first.Messages[1].MsgID})
	assert.Equal(t, timestamp, first.SyncTime)
	assert.Equal(t, int64(102), first.SyncMsgID)

	second := readBatch(model.SyncReq{
		LastSyncTime:  first.SyncTime,
		LastSyncMsgID: first.SyncMsgID,
		BatchSize:     2,
	})
	assert.False(t, second.HasMore)
	if assert.Len(t, second.Messages, 1) {
		assert.Equal(t, int64(103), second.Messages[0].MsgID)
	}
}

func TestRevokeMsg_Success_Private(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.revokeResult = true
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	receiver := conn.NewClientConnection(2, nil)
	cm.Register(1, sender)
	cm.Register(2, receiver)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.RevokeMsgReq{ConvID: "p_1_2", ServerMsgID: 1001}
	data, _ := json.Marshal(req)

	svc.HandleRevokeMsg(1, data)

	// 验证：Lua 撤回已被调用
	assert.True(t, redisMock.revokeCalled)

	// 验证：发送方收到了 msgRevoked 通知
	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeMsgRevoked, wsMsg.Type)

	var notif model.RevokedNotification
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &notif))
	assert.Equal(t, "p_1_2", notif.ConvID)
	assert.Equal(t, int64(1001), notif.ServerMsgID)
	assert.Equal(t, int64(1), notif.OperatorID)

	// 验证：接收方也收到了 msgRevoked 通知
	msg, ok = drainSendCh(receiver)
	assert.True(t, ok)
	wsMsg, _ = decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeMsgRevoked, wsMsg.Type)
}

func TestRevokeMsg_Fail(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.revokeResult = false // 2 分钟窗口已过期或未授权
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.RevokeMsgReq{ConvID: "p_1_2", ServerMsgID: 1001}
	data, _ := json.Marshal(req)

	svc.HandleRevokeMsg(1, data)

	// 验证：错误已发送
	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeError, wsMsg.Type)

	var wsErr model.WsError
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &wsErr))
	assert.Equal(t, 403, wsErr.Code)
	assert.Equal(t, ErrRevokeFail, wsErr.Message)
}

func TestRevokeMsg_Group(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.revokeResult = true
	redisMock.groupMembers[100] = []int64{1, 2, 3}
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	member := conn.NewClientConnection(2, nil)
	cm.Register(1, sender)
	cm.Register(2, member)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.RevokeMsgReq{ConvID: "g_100", ServerMsgID: 3001}
	data, _ := json.Marshal(req)

	svc.HandleRevokeMsg(1, data)

	// 验证：Lua 撤回已被调用
	assert.True(t, redisMock.revokeCalled)

	// 验证：发送方收到了 msgRevoked 通知
	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeMsgRevoked, wsMsg.Type)

	// 在线群成员无需刷新，也应立即收到撤回通知。
	msg, ok = drainSendCh(member)
	assert.True(t, ok)
	wsMsg, _ = decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeMsgRevoked, wsMsg.Type)
	var notification model.RevokedNotification
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &notification))
	assert.Equal(t, int64(3001), notification.ServerMsgID)
}

func TestRevokeMsg_InvalidConvID(t *testing.T) {
	redisMock := newMockRedisRepo()
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.RevokeMsgReq{ConvID: "x_invalid", ServerMsgID: 1001}
	data, _ := json.Marshal(req)

	svc.HandleRevokeMsg(1, data)

	// 验证：错误已发送
	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeError, wsMsg.Type)

	var wsErr model.WsError
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &wsErr))
	assert.Equal(t, 400, wsErr.Code)
	assert.Equal(t, "convId 格式无效", wsErr.Message)
}

func TestHandleSendMessage_InvalidJSON(t *testing.T) {
	redisMock := newMockRedisRepo()
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	svc.HandleSendMessage(1, []byte("not json"))

	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeError, wsMsg.Type)

	var wsErr model.WsError
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &wsErr))
	assert.Equal(t, 400, wsErr.Code)
}

func TestHandleSendMessage_UnknownConvType(t *testing.T) {
	redisMock := newMockRedisRepo()
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.SendMessage{
		ClientMsgID: "client-msg-unknown",
		ConvType:    99, // 未知类型
		ToID:        2,
		MsgType:     1,
		Content:     "hello",
		Timestamp:   1000000,
	}
	data, _ := json.Marshal(req)

	svc.HandleSendMessage(1, data)

	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeError, wsMsg.Type)

	var wsErr model.WsError
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &wsErr))
	assert.Equal(t, 400, wsErr.Code)
	assert.Equal(t, "未知的会话类型", wsErr.Message)
}

func TestPrivateMsgSend_MQPublishFail(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.privateCheckResult = &redislua.PrivateMsgCheckResult{
		ErrCode:   redislua.PMErrOK,
		MsgID:     1001,
		IsOnline:  false,
		IsFriend:  true,
		IsBlocked: false,
	}
	mqMock := newMockMQRepo()
	mqMock.publishErr = context.DeadlineExceeded
	cm := conn.NewConnectionManager()
	logger := testLogger()

	sender := conn.NewClientConnection(1, nil)
	cm.Register(1, sender)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.SendMessage{
		ClientMsgID: "client-msg-mqfail",
		ConvType:    model.ConvTypePrivate,
		ToID:        2,
		MsgType:     model.MsgTypeText,
		Content:     "hello",
		Timestamp:   1000000,
	}
	data, _ := json.Marshal(req)

	svc.HandleSendMessage(1, data)

	// 验证：错误已发送给发送方
	msg, ok := drainSendCh(sender)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeError, wsMsg.Type)

	var wsErr model.WsError
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &wsErr))
	assert.Equal(t, 500, wsErr.Code)
	assert.Equal(t, "消息发布失败", wsErr.Message)
}

func TestGetOtherPartyID(t *testing.T) {
	assert.Equal(t, int64(2), getOtherPartyID("p_1_2", 1))
	assert.Equal(t, int64(1), getOtherPartyID("p_1_2", 2))
	assert.Equal(t, int64(0), getOtherPartyID("g_100", 1))   // 群组 convID 返回 0
	assert.Equal(t, int64(0), getOtherPartyID("invalid", 1)) // 无效格式
	assert.Equal(t, int64(0), getOtherPartyID("p_1", 1))     // 缺少第二个 ID
}

func TestReadAck_LuaError(t *testing.T) {
	redisMock := newMockRedisRepo()
	redisMock.inboxMarkReadErr = context.DeadlineExceeded
	mqMock := newMockMQRepo()
	cm := conn.NewConnectionManager()
	logger := testLogger()

	client := conn.NewClientConnection(1, nil)
	cm.Register(1, client)

	svc := NewMsgService(redisMock, mqMock, cm, logger)

	req := model.ReadAck{ConvID: "p_1_2"}
	data, _ := json.Marshal(req)

	svc.HandleReadAck(1, data)

	// 验证：错误已发送
	msg, ok := drainSendCh(client)
	assert.True(t, ok)
	wsMsg, _ := decodeWsMessage(msg)
	assert.Equal(t, protocol.TypeError, wsMsg.Type)

	var wsErr model.WsError
	assert.NoError(t, json.Unmarshal(wsMsg.Data, &wsErr))
	assert.Equal(t, 500, wsErr.Code)
}

// ── 高并发点赞新增接口的 mock 桩 ──

func (m *mockRedisRepo) LikeMomentAtomic(_ context.Context, _ int64, _ int64) (bool, int64, error) {
	return false, 0, nil
}
func (m *mockRedisRepo) UnlikeMomentAtomic(_ context.Context, _ int64, _ int64) (bool, int64, error) {
	return false, 0, nil
}
func (m *mockRedisRepo) EnsureMomentLikesLoaded(_ context.Context, _ int64, _ func(context.Context) ([]int64, error), _ time.Duration) error {
	return nil
}
func (m *mockRedisRepo) GetMomentLikeStats(_ context.Context, _ int64, _ []int64) (map[int64]int64, map[int64]bool, error) {
	return nil, nil, nil
}
func (m *mockRedisRepo) GetMomentLikerIDs(_ context.Context, _ int64) ([]int64, error) {
	return nil, nil
}
func (m *mockRedisRepo) DeleteMomentLikes(_ context.Context, _ int64) error { return nil }

func (m *mockMQRepo) PublishLikeEvent(_ context.Context, _ *model.LikeEvent) error { return nil }
