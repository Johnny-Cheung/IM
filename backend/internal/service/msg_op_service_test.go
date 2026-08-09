package service

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
	redislua "github.com/goim/goim/internal/redis"
)

// ──────────────────────────────────────────────────────
// Mock MySQLRepo，用于 msg_op 测试
// ──────────────────────────────────────────────────────

type mockMsgOpRepo struct {
	mu sync.Mutex

	// 存储已撤回消息，按 msgID 索引
	revokedMsgs map[int64]*model.MsgRevoked
	// 存储私聊消息，用于搜索
	privateMsgs map[int64]*model.PrivateMessage
	// 搜索错误覆盖
	searchErr error
	// InsertMsgRevoked 错误覆盖
	insertRevokedErr error
	// 下一个自增 ID
	nextRevokedID int64
}

func newMockMsgOpRepo() *mockMsgOpRepo {
	return &mockMsgOpRepo{
		revokedMsgs:   make(map[int64]*model.MsgRevoked),
		privateMsgs:   make(map[int64]*model.PrivateMessage),
		nextRevokedID: 1,
	}
}

// ── Msg-op 专用方法 ──

func (m *mockMsgOpRepo) InsertMsgRevoked(_ context.Context, revoked *model.MsgRevoked) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.insertRevokedErr != nil {
		return m.insertRevokedErr
	}
	revoked.ID = m.nextRevokedID
	m.nextRevokedID++
	m.revokedMsgs[revoked.MsgID] = revoked
	return nil
}

func (m *mockMsgOpRepo) SearchPrivateMessages(_ context.Context, userID int64, query string, limit, offset int) ([]model.PrivateMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.searchErr != nil {
		return nil, m.searchErr
	}
	var results []model.PrivateMessage
	for _, msg := range m.privateMsgs {
		if (msg.SenderID == userID || msg.ReceiverID == userID) && containsSubstring(msg.Content, query) {
			results = append(results, *msg)
		}
	}
	// Apply limit/offset
	if offset > len(results) {
		return nil, nil
	}
	results = results[offset:]
	if limit < len(results) {
		results = results[:limit]
	}
	return results, nil
}

func containsSubstring(s, sub string) bool {
	return len(sub) <= len(s) && (s == sub || len(s) > 0 && findSubstring(s, sub))
}

func findSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ── Stub out all other MySQLRepo methods ──

func (m *mockMsgOpRepo) InsertPrivateMessage(_ context.Context, msg *model.PrivateMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.privateMsgs[msg.ID] = msg
	return nil
}
func (m *mockMsgOpRepo) InsertGroupMessage(_ context.Context, _ *model.GroupMessage) error {
	return nil
}
func (m *mockMsgOpRepo) GetUserByID(_ context.Context, _ int64) (*model.User, error) { return nil, nil }
func (m *mockMsgOpRepo) GetUserByUsername(_ context.Context, _ string) (*model.User, error) {
	return nil, nil
}
func (m *mockMsgOpRepo) CreateUser(_ context.Context, _ *model.User) error { return nil }
func (m *mockMsgOpRepo) UpdateUser(_ context.Context, _ *model.User) error { return nil }
func (m *mockMsgOpRepo) DeleteMoment(_ context.Context, _ int64) error     { return nil }

func (m *mockMsgOpRepo) CreateFriendRequest(_ context.Context, _ *model.FriendRequest) error {
	return nil
}
func (m *mockMsgOpRepo) UpdateFriendRequest(_ context.Context, _ *model.FriendRequest) error {
	return nil
}
func (m *mockMsgOpRepo) GetFriendRequestByID(_ context.Context, _ int64) (*model.FriendRequest, error) {
	return nil, nil
}
func (m *mockMsgOpRepo) GetFriendRequestsByUser(_ context.Context, _ int64) ([]model.FriendRequest, error) {
	return nil, nil
}
func (m *mockMsgOpRepo) CreateFriendship(_ context.Context, _ *model.Friendship) error { return nil }
func (m *mockMsgOpRepo) DeleteFriendship(_ context.Context, _ int64, _ int64) error    { return nil }
func (m *mockMsgOpRepo) GetFriendList(_ context.Context, _ int64) ([]model.Friendship, error) {
	return nil, nil
}
func (m *mockMsgOpRepo) IsFriend(_ context.Context, _ int64, _ int64) (bool, error) {
	return false, nil
}
func (m *mockMsgOpRepo) CreateBlacklist(_ context.Context, _ *model.Blacklist) error { return nil }
func (m *mockMsgOpRepo) DeleteBlacklist(_ context.Context, _ int64, _ int64) error   { return nil }
func (m *mockMsgOpRepo) IsBlocked(_ context.Context, _ int64, _ int64) (bool, error) {
	return false, nil
}

func (m *mockMsgOpRepo) CreateGroup(_ context.Context, _ *model.Group) (int64, error) { return 0, nil }
func (m *mockMsgOpRepo) UpdateGroup(_ context.Context, _ *model.Group) error          { return nil }
func (m *mockMsgOpRepo) GetGroupByID(_ context.Context, _ int64) (*model.Group, error) {
	return nil, nil
}
func (m *mockMsgOpRepo) AddGroupMember(_ context.Context, _ *model.GroupMember) error { return nil }
func (m *mockMsgOpRepo) RemoveGroupMember(_ context.Context, _ int64, _ int64) error  { return nil }
func (m *mockMsgOpRepo) GetGroupMembers(_ context.Context, _ int64) ([]model.GroupMember, error) {
	return nil, nil
}
func (m *mockMsgOpRepo) UpdateGroupMemberRole(_ context.Context, _ int, _ int, _ int) error {
	return nil
}

func (m *mockMsgOpRepo) CreateMoment(_ context.Context, _ *model.Moment) error { return nil }
func (m *mockMsgOpRepo) GetMomentByID(_ context.Context, _ int64) (*model.Moment, error) {
	return nil, nil
}
func (m *mockMsgOpRepo) GetMomentsByUser(_ context.Context, _ int64, _ int, _ int) ([]model.Moment, error) {
	return nil, nil
}
func (m *mockMsgOpRepo) CreateMomentLike(_ context.Context, _ *model.MomentLike) error { return nil }
func (m *mockMsgOpRepo) DeleteMomentLike(_ context.Context, _ int64, _ int64) error    { return nil }
func (m *mockMsgOpRepo) CreateMomentComment(_ context.Context, _ *model.MomentComment) error {
	return nil
}
func (m *mockMsgOpRepo) GetMomentCommentByID(_ context.Context, _ int64) (*model.MomentComment, error) {
	return nil, nil
}
func (m *mockMsgOpRepo) GetMomentComments(_ context.Context, _ int64) ([]model.MomentComment, error) {
	return nil, nil
}
func (m *mockMsgOpRepo) DeleteMomentComment(_ context.Context, _ int64) error { return nil }
func (m *mockMsgOpRepo) CountFriends(_ context.Context, _ int64) (int, error) { return 0, nil }
func (m *mockMsgOpRepo) GetMomentsByIDs(_ context.Context, _ []int64) ([]model.Moment, error) {
	return nil, nil
}

func (m *mockMsgOpRepo) GetUserSettings(_ context.Context, _ int64) (*model.UserSettings, error) {
	return nil, nil
}
func (m *mockMsgOpRepo) CreateOrUpdateUserSettings(_ context.Context, _ *model.UserSettings) error {
	return nil
}

// ──────────────────────────────────────────────────────
// Mock RedisRepo for msg_op tests
// ──────────────────────────────────────────────────────

type mockMsgOpRedisRepo struct {
	mu sync.Mutex
	// revokeResult override: true = success, false = not authorized/not found
	revokeResult bool
	revokeErr    error
}

func (m *mockMsgOpRedisRepo) WriteInbox(_ context.Context, _ int64, _ *model.InboxMessage) error {
	return nil
}
func (m *mockMsgOpRedisRepo) WriteOutbox(_ context.Context, _ int64, _ *model.InboxMessage) error {
	return nil
}
func (m *mockMsgOpRedisRepo) ReadInbox(_ context.Context, _ int64, _, _ int64, _ int) ([]model.InboxMessage, error) {
	return nil, nil
}
func (m *mockMsgOpRedisRepo) ReadOutbox(_ context.Context, _ int64, _, _ int64, _ int) ([]model.InboxMessage, error) {
	return nil, nil
}
func (m *mockMsgOpRedisRepo) UpdateConvList(_ context.Context, _ int64, _ string, _ string, _ int64) error {
	return nil
}
func (m *mockMsgOpRedisRepo) GetConvList(_ context.Context, _ int64) ([]model.ConvSummary, error) {
	return nil, nil
}
func (m *mockMsgOpRedisRepo) IncrementUnread(_ context.Context, _ int64, _ string) error { return nil }
func (m *mockMsgOpRedisRepo) ClearUnread(_ context.Context, _ int64, _ string) error     { return nil }
func (m *mockMsgOpRedisRepo) GetUnreadMap(_ context.Context, _ int64) (map[string]int64, error) {
	return nil, nil
}
func (m *mockMsgOpRedisRepo) SetGroupReadPos(_ context.Context, _ int64, _ string, _ int64) error {
	return nil
}
func (m *mockMsgOpRedisRepo) GetGroupReadPos(_ context.Context, _ int64, _ string) (int64, error) {
	return 0, nil
}

func (m *mockMsgOpRedisRepo) GetGroupSeq(_ context.Context, _ int64) (int64, error) { return 0, nil }
func (m *mockMsgOpRedisRepo) GetGroupMemberships(_ context.Context, _ int64) ([]int64, error) {
	return nil, nil
}
func (m *mockMsgOpRedisRepo) GetGroupMembers(_ context.Context, _ int64) ([]int64, error) {
	return nil, nil
}
func (m *mockMsgOpRedisRepo) AddGroupMemberRedis(_ context.Context, _ int64, _ int64) error {
	return nil
}
func (m *mockMsgOpRedisRepo) RemoveGroupMemberRedis(_ context.Context, _ int64, _ int64) error {
	return nil
}
func (m *mockMsgOpRedisRepo) CheckDuplicate(_ context.Context, _ int64, _ string) (bool, error) {
	return false, nil
}
func (m *mockMsgOpRedisRepo) TrimInbox(_ context.Context, _ int64, _ int) error          { return nil }
func (m *mockMsgOpRedisRepo) TrimOutbox(_ context.Context, _ int64, _ int) error         { return nil }
func (m *mockMsgOpRedisRepo) TrimInboxByTime(_ context.Context, _ int64, _ int64) error  { return nil }
func (m *mockMsgOpRedisRepo) TrimOutboxByTime(_ context.Context, _ int64, _ int64) error { return nil }
func (m *mockMsgOpRedisRepo) TrimConvListByTime(_ context.Context, _ int64, _ int64) error {
	return nil
}
func (m *mockMsgOpRedisRepo) TrimTimelineByTime(_ context.Context, _ int64, _ int64) error {
	return nil
}

func (m *mockMsgOpRedisRepo) ExecPrivateMsgCheck(_ context.Context, _ int64, _ int64, _ string) (*redislua.PrivateMsgCheckResult, error) {
	return nil, nil
}
func (m *mockMsgOpRedisRepo) ExecGroupMsgCheck(_ context.Context, _ int64, _ int64, _ string) (*redislua.GroupMsgCheckResult, error) {
	return nil, nil
}
func (m *mockMsgOpRedisRepo) ExecInboxMarkRead(_ context.Context, _ int64, _ string) (int64, error) {
	return 0, nil
}
func (m *mockMsgOpRedisRepo) ExecRevokeMsg(_ context.Context, _ int64, _ string, _ int64, _ string, _ int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.revokeErr != nil {
		return false, m.revokeErr
	}
	return m.revokeResult, nil
}
func (m *mockMsgOpRedisRepo) PublishMomentFeed(_ context.Context, _ int64, _ int64, _ int64) error {
	return nil
}
func (m *mockMsgOpRedisRepo) GetMomentFeed(_ context.Context, _ int64, _ int64, _ int) ([]int64, error) {
	return nil, nil
}
func (m *mockMsgOpRedisRepo) FanoutMomentFeed(_ context.Context, _ []int64, _ int64, _ int64, _ int) error {
	return nil
}
func (m *mockMsgOpRedisRepo) AddToOutbox(_ context.Context, _ int64, _ int64, _ int64, _ int) error {
	return nil
}
func (m *mockMsgOpRedisRepo) MarkBigUser(_ context.Context, _ int64) error { return nil }
func (m *mockMsgOpRedisRepo) FilterBigUsers(_ context.Context, _ []int64) ([]int64, error) {
	return nil, nil
}
func (m *mockMsgOpRedisRepo) GetTimelinePage(_ context.Context, _ int64, _ int64, _ int64, _ int) ([]model.FeedEntry, error) {
	return nil, nil
}
func (m *mockMsgOpRedisRepo) GetOutboxPage(_ context.Context, _ int64, _ int64, _ int64, _ int) ([]model.FeedEntry, error) {
	return nil, nil
}
func (m *mockMsgOpRedisRepo) SetFriendCache(_ context.Context, _ int64, _ int64) error { return nil }

// ──────────────────────────────────────────────────────
// Helper: new test MsgOpService
// ──────────────────────────────────────────────────────

func newTestMsgOpService(repo *mockMsgOpRepo, redisRepo *mockMsgOpRedisRepo) *MsgOpService {
	logger := zap.NewNop()
	return NewMsgOpService(repo, redisRepo, logger)
}

// ──────────────────────────────────────────────────────
// RevokeMessage tests
// ──────────────────────────────────────────────────────

func TestMsgOp_RevokeMessage_Success(t *testing.T) {
	repo := newMockMsgOpRepo()
	redisRepo := &mockMsgOpRedisRepo{revokeResult: true}
	svc := newTestMsgOpService(repo, redisRepo)

	err := svc.RevokeMessage(context.Background(), 1, "p_1_2", 100)
	assert.NoError(t, err)

	// Verify the revoke record was persisted to MySQL mock
	repo.mu.Lock()
	revoked := repo.revokedMsgs[100]
	repo.mu.Unlock()
	assert.NotNil(t, revoked)
	assert.Equal(t, int64(100), revoked.MsgID)
	assert.Equal(t, "p_1_2", revoked.ConvID)
	assert.Equal(t, int64(1), revoked.OperatorID)
}

func TestMsgOp_RevokeMessage_LuaFails(t *testing.T) {
	repo := newMockMsgOpRepo()
	redisRepo := &mockMsgOpRedisRepo{revokeResult: false}
	svc := newTestMsgOpService(repo, redisRepo)

	err := svc.RevokeMessage(context.Background(), 1, "p_1_2", 100)
	assert.Error(t, err)
	assert.Equal(t, ErrMsgNotRevocable, err.Error())

	// Verify no revoke record was persisted
	repo.mu.Lock()
	revoked := repo.revokedMsgs[100]
	repo.mu.Unlock()
	assert.Nil(t, revoked)
}

func TestMsgOp_RevokeMessage_LuaError(t *testing.T) {
	repo := newMockMsgOpRepo()
	redisRepo := &mockMsgOpRedisRepo{revokeResult: false, revokeErr: fmt.Errorf("redis error")}
	svc := newTestMsgOpService(repo, redisRepo)

	err := svc.RevokeMessage(context.Background(), 1, "p_1_2", 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exec revoke lua")
}

func TestMsgOp_RevokeMessage_MySQLErrorDoesNotFail(t *testing.T) {
	repo := newMockMsgOpRepo()
	repo.insertRevokedErr = fmt.Errorf("mysql error")
	redisRepo := &mockMsgOpRedisRepo{revokeResult: true}
	svc := newTestMsgOpService(repo, redisRepo)

	// Should succeed even if MySQL persistence fails (just logs error)
	err := svc.RevokeMessage(context.Background(), 1, "p_1_2", 100)
	assert.NoError(t, err)
}

// ──────────────────────────────────────────────────────
// DeleteMessage tests
// ──────────────────────────────────────────────────────

func TestMsgOp_DeleteMessage_Success(t *testing.T) {
	repo := newMockMsgOpRepo()
	redisRepo := &mockMsgOpRedisRepo{revokeResult: true}
	svc := newTestMsgOpService(repo, redisRepo)

	err := svc.DeleteMessage(context.Background(), 1, "p_1_2", 100)
	assert.NoError(t, err)
}

func TestMsgOp_DeleteMessage_LuaFails(t *testing.T) {
	repo := newMockMsgOpRepo()
	redisRepo := &mockMsgOpRedisRepo{revokeResult: false}
	svc := newTestMsgOpService(repo, redisRepo)

	err := svc.DeleteMessage(context.Background(), 1, "p_1_2", 100)
	assert.Error(t, err)
	assert.Equal(t, ErrMsgDeleteFailed, err.Error())
}

// ──────────────────────────────────────────────────────
// SearchMessages tests
// ──────────────────────────────────────────────────────

func TestMsgOp_SearchMessages_Success(t *testing.T) {
	repo := newMockMsgOpRepo()
	// Add some messages to the mock
	repo.mu.Lock()
	repo.privateMsgs[1] = &model.PrivateMessage{ID: 1, SenderID: 1, ReceiverID: 2, Content: "hello world", MsgType: model.MsgTypeText, CreatedAt: time.Now()}
	repo.privateMsgs[2] = &model.PrivateMessage{ID: 2, SenderID: 2, ReceiverID: 1, Content: "hello there", MsgType: model.MsgTypeText, CreatedAt: time.Now()}
	repo.privateMsgs[3] = &model.PrivateMessage{ID: 3, SenderID: 1, ReceiverID: 3, Content: "goodbye", MsgType: model.MsgTypeText, CreatedAt: time.Now()}
	repo.mu.Unlock()

	redisRepo := &mockMsgOpRedisRepo{revokeResult: true}
	svc := newTestMsgOpService(repo, redisRepo)

	msgs, err := svc.SearchMessages(context.Background(), 1, "hello", 20, 0)
	assert.NoError(t, err)
	assert.Len(t, msgs, 2) // "hello world" and "hello there" involve user 1
}

func TestMsgOp_SearchMessages_EmptyQuery(t *testing.T) {
	repo := newMockMsgOpRepo()
	redisRepo := &mockMsgOpRedisRepo{}
	svc := newTestMsgOpService(repo, redisRepo)

	msgs, err := svc.SearchMessages(context.Background(), 1, "", 20, 0)
	assert.NoError(t, err)
	assert.Nil(t, msgs)
}

func TestMsgOp_SearchMessages_DefaultLimit(t *testing.T) {
	repo := newMockMsgOpRepo()
	redisRepo := &mockMsgOpRedisRepo{}
	svc := newTestMsgOpService(repo, redisRepo)

	msgs, err := svc.SearchMessages(context.Background(), 1, "test", 0, 0)
	assert.NoError(t, err)
	// Default limit=20 is applied; no error
	assert.Nil(t, msgs) // no messages in mock
}

func TestMsgOp_SearchMessages_Error(t *testing.T) {
	repo := newMockMsgOpRepo()
	repo.searchErr = fmt.Errorf("db error")
	redisRepo := &mockMsgOpRedisRepo{}
	svc := newTestMsgOpService(repo, redisRepo)

	msgs, err := svc.SearchMessages(context.Background(), 1, "test", 20, 0)
	assert.Error(t, err)
	assert.Nil(t, msgs)
}

// ── 高并发点赞新增接口的 mock 桩 ──

func (m *mockMsgOpRepo) GetMomentLikers(_ context.Context, _ int64) ([]int64, error) { return nil, nil }
func (m *mockMsgOpRepo) BatchUpsertMomentLikes(_ context.Context, _ []model.MomentLike) error {
	return nil
}
func (m *mockMsgOpRepo) BatchDeleteMomentLikes(_ context.Context, _ []model.MomentLikeKey) error {
	return nil
}

func (m *mockMsgOpRedisRepo) LikeMomentAtomic(_ context.Context, _ int64, _ int64) (bool, int64, error) {
	return false, 0, nil
}
func (m *mockMsgOpRedisRepo) UnlikeMomentAtomic(_ context.Context, _ int64, _ int64) (bool, int64, error) {
	return false, 0, nil
}
func (m *mockMsgOpRedisRepo) EnsureMomentLikesLoaded(_ context.Context, _ int64, _ func(context.Context) ([]int64, error), _ time.Duration) error {
	return nil
}
func (m *mockMsgOpRedisRepo) GetMomentLikeStats(_ context.Context, _ int64, _ []int64) (map[int64]int64, map[int64]bool, error) {
	return nil, nil, nil
}
func (m *mockMsgOpRedisRepo) GetMomentLikerIDs(_ context.Context, _ int64) ([]int64, error) {
	return nil, nil
}
func (m *mockMsgOpRedisRepo) DeleteMomentLikes(_ context.Context, _ int64) error { return nil }
