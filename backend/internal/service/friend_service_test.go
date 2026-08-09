package service

import (
	"context"
	"encoding/json"
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
// 模拟 MySQLRepo，用于好友相关测试
// ──────────────────────────────────────────────────────

type mockFriendRepo struct {
	mu sync.Mutex

	// 按 ID 存储好友请求
	friendRequests map[int64]*model.FriendRequest
	// 按 "userID:friendID" 存储好友关系
	friendships map[string]*model.Friendship
	// 按 "userID:blockedID" 存储黑名单条目
	blacklist map[string]*model.Blacklist
	// 按 ID 存储用户（用于 GetFriendList 补充信息）
	users map[int64]*model.User
	// 按作者存储朋友圈，用于测试添加好友后的历史回填。
	momentsByUser map[int64][]model.Moment
	// 下一个自增 ID
	nextRequestID int64
	nextFriendID  int64
	nextBlackID   int64

	// 错误覆盖
	isFriendErr      error
	isBlockedErr     error
	getRequestsErr   error
	createReqErr     error
	getReqByIDErr    error
	updateReqErr     error
	createFsErr      error
	deleteFsErr      error
	getFriendListErr error
	createBlackErr   error
	deleteBlackErr   error
	getUserByIDErr   error
}

func newMockFriendRepo() *mockFriendRepo {
	return &mockFriendRepo{
		friendRequests: make(map[int64]*model.FriendRequest),
		friendships:    make(map[string]*model.Friendship),
		blacklist:      make(map[string]*model.Blacklist),
		users:          make(map[int64]*model.User),
		momentsByUser:  make(map[int64][]model.Moment),
		nextRequestID:  1,
		nextFriendID:   1,
		nextBlackID:    1,
	}
}

func friendKey(userID, friendID int64) string {
	return fmt.Sprintf("%d:%d", userID, friendID)
}

func blackKey(userID, blockedID int64) string {
	return fmt.Sprintf("%d:%d", userID, blockedID)
}

// ── 好友专用方法 ──

func (m *mockFriendRepo) CreateFriendRequest(_ context.Context, req *model.FriendRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createReqErr != nil {
		return m.createReqErr
	}
	for _, stored := range m.friendRequests {
		if stored.FromUserID == req.FromUserID && stored.ToUserID == req.ToUserID {
			now := time.Now()
			stored.Message = req.Message
			stored.Status = req.Status
			stored.CreatedAt = now
			stored.UpdatedAt = now
			req.ID = stored.ID
			req.CreatedAt = now
			req.UpdatedAt = now
			return nil
		}
	}
	req.ID = m.nextRequestID
	m.nextRequestID++
	req.CreatedAt = time.Now()
	req.UpdatedAt = time.Now()
	m.friendRequests[req.ID] = req
	return nil
}

func (m *mockFriendRepo) UpdateFriendRequest(_ context.Context, req *model.FriendRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.updateReqErr != nil {
		return m.updateReqErr
	}
	if stored, ok := m.friendRequests[req.ID]; ok {
		stored.Status = req.Status
		stored.UpdatedAt = time.Now()
	}
	return nil
}

func (m *mockFriendRepo) GetFriendRequestByID(_ context.Context, id int64) (*model.FriendRequest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getReqByIDErr != nil {
		return nil, m.getReqByIDErr
	}
	r, ok := m.friendRequests[id]
	if !ok {
		return nil, nil
	}
	return r, nil
}

func (m *mockFriendRepo) GetFriendRequestsByUser(_ context.Context, userID int64) ([]model.FriendRequest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getRequestsErr != nil {
		return nil, m.getRequestsErr
	}
	var results []model.FriendRequest
	for _, r := range m.friendRequests {
		if (r.FromUserID == userID || r.ToUserID == userID) && r.Status == 0 {
			results = append(results, *r)
		}
	}
	return results, nil
}

func (m *mockFriendRepo) CreateFriendship(_ context.Context, fs *model.Friendship) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createFsErr != nil {
		return m.createFsErr
	}
	fs.ID = m.nextFriendID
	m.nextFriendID++
	fs.CreatedAt = time.Now()
	m.friendships[friendKey(fs.UserID, fs.FriendID)] = fs
	// 双向：同时添加 friend→user
	m.friendships[friendKey(fs.FriendID, fs.UserID)] = &model.Friendship{
		ID:        fs.ID + 1,
		UserID:    fs.FriendID,
		FriendID:  fs.UserID,
		CreatedAt: fs.CreatedAt,
	}
	m.nextFriendID++ // 为反向行递增计数
	return nil
}

func (m *mockFriendRepo) DeleteFriendship(_ context.Context, userID, friendID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteFsErr != nil {
		return m.deleteFsErr
	}
	delete(m.friendships, friendKey(userID, friendID))
	delete(m.friendships, friendKey(friendID, userID))
	return nil
}

func (m *mockFriendRepo) GetFriendList(_ context.Context, userID int64) ([]model.Friendship, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getFriendListErr != nil {
		return nil, m.getFriendListErr
	}
	var results []model.Friendship
	for _, fs := range m.friendships {
		if fs.UserID == userID {
			results = append(results, *fs)
		}
	}
	return results, nil
}

func (m *mockFriendRepo) IsFriend(_ context.Context, userID, friendID int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isFriendErr != nil {
		return false, m.isFriendErr
	}
	_, ok := m.friendships[friendKey(userID, friendID)]
	return ok, nil
}

func (m *mockFriendRepo) CreateBlacklist(_ context.Context, bl *model.Blacklist) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createBlackErr != nil {
		return m.createBlackErr
	}
	bl.ID = m.nextBlackID
	m.nextBlackID++
	bl.CreatedAt = time.Now()
	m.blacklist[blackKey(bl.UserID, bl.BlockedID)] = bl
	return nil
}

func (m *mockFriendRepo) DeleteBlacklist(_ context.Context, userID, blockedID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteBlackErr != nil {
		return m.deleteBlackErr
	}
	delete(m.blacklist, blackKey(userID, blockedID))
	return nil
}

func (m *mockFriendRepo) IsBlocked(_ context.Context, userID, blockedID int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isBlockedErr != nil {
		return false, m.isBlockedErr
	}
	_, ok := m.blacklist[blackKey(userID, blockedID)]
	return ok, nil
}

// ── 用户方法（用于 GetFriendList 补充信息） ──

func (m *mockFriendRepo) GetUserByID(_ context.Context, userID int64) (*model.User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getUserByIDErr != nil {
		return nil, m.getUserByIDErr
	}
	u, ok := m.users[userID]
	if !ok {
		return &model.User{ID: userID, Username: fmt.Sprintf("user%d", userID)}, nil
	}
	return u, nil
}

func (m *mockFriendRepo) GetUserByUsername(_ context.Context, _ string) (*model.User, error) {
	return nil, nil
}
func (m *mockFriendRepo) CreateUser(_ context.Context, _ *model.User) error { return nil }
func (m *mockFriendRepo) UpdateUser(_ context.Context, _ *model.User) error { return nil }
func (m *mockFriendRepo) DeleteMoment(_ context.Context, _ int64) error     { return nil }

// ── 桩实现：其他所有 MySQLRepo 方法 ──

func (m *mockFriendRepo) InsertPrivateMessage(_ context.Context, _ *model.PrivateMessage) error {
	return nil
}
func (m *mockFriendRepo) InsertGroupMessage(_ context.Context, _ *model.GroupMessage) error {
	return nil
}
func (m *mockFriendRepo) InsertMsgRevoked(_ context.Context, _ *model.MsgRevoked) error { return nil }

func (m *mockFriendRepo) CreateGroup(_ context.Context, _ *model.Group) (int64, error) { return 0, nil }
func (m *mockFriendRepo) UpdateGroup(_ context.Context, _ *model.Group) error          { return nil }
func (m *mockFriendRepo) GetGroupByID(_ context.Context, _ int64) (*model.Group, error) {
	return nil, nil
}
func (m *mockFriendRepo) AddGroupMember(_ context.Context, _ *model.GroupMember) error { return nil }
func (m *mockFriendRepo) RemoveGroupMember(_ context.Context, _ int64, _ int64) error  { return nil }
func (m *mockFriendRepo) GetGroupMembers(_ context.Context, _ int64) ([]model.GroupMember, error) {
	return nil, nil
}
func (m *mockFriendRepo) UpdateGroupMemberRole(_ context.Context, _ int, _ int, _ int) error {
	return nil
}

func (m *mockFriendRepo) CreateMoment(_ context.Context, _ *model.Moment) error { return nil }
func (m *mockFriendRepo) GetMomentByID(_ context.Context, _ int64) (*model.Moment, error) {
	return nil, nil
}
func (m *mockFriendRepo) GetMomentsByUser(_ context.Context, userID int64, limit, offset int) ([]model.Moment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	moments := m.momentsByUser[userID]
	if offset >= len(moments) {
		return []model.Moment{}, nil
	}
	end := offset + limit
	if end > len(moments) {
		end = len(moments)
	}
	return append([]model.Moment(nil), moments[offset:end]...), nil
}
func (m *mockFriendRepo) CreateMomentLike(_ context.Context, _ *model.MomentLike) error { return nil }
func (m *mockFriendRepo) DeleteMomentLike(_ context.Context, _ int64, _ int64) error    { return nil }
func (m *mockFriendRepo) CreateMomentComment(_ context.Context, _ *model.MomentComment) error {
	return nil
}
func (m *mockFriendRepo) DeleteMomentComment(_ context.Context, _ int64) error { return nil }
func (m *mockFriendRepo) CountFriends(_ context.Context, _ int64) (int, error) { return 0, nil }
func (m *mockFriendRepo) GetMomentsByIDs(_ context.Context, _ []int64) ([]model.Moment, error) {
	return nil, nil
}

func (m *mockFriendRepo) GetMomentCommentByID(_ context.Context, _ int64) (*model.MomentComment, error) {
	return nil, nil
}
func (m *mockFriendRepo) GetMomentComments(_ context.Context, _ int64) ([]model.MomentComment, error) {
	return nil, nil
}

func (m *mockFriendRepo) GetUserSettings(_ context.Context, _ int64) (*model.UserSettings, error) {
	return nil, nil
}
func (m *mockFriendRepo) CreateOrUpdateUserSettings(_ context.Context, _ *model.UserSettings) error {
	return nil
}
func (m *mockFriendRepo) SearchPrivateMessages(_ context.Context, _ int64, _ string, _ int, _ int) ([]model.PrivateMessage, error) {
	return nil, nil
}

// ──────────────────────────────────────────────────────
// 模拟 RedisRepo，用于好友相关测试
// ──────────────────────────────────────────────────────

type friendMomentFanout struct {
	recipientID int64
	momentID    int64
	timestamp   int64
}

type mockFriendRedisRepo struct {
	momentFanouts       []friendMomentFanout
	convUpdates         []friendConvUpdate
	deletedFriendCaches [][2]int64
}

type friendConvUpdate struct {
	userID, timestamp int64
	convID, summary   string
}

func (m *mockFriendRedisRepo) WriteInbox(_ context.Context, _ int64, _ *model.InboxMessage) error {
	return nil
}
func (m *mockFriendRedisRepo) WriteOutbox(_ context.Context, _ int64, _ *model.InboxMessage) error {
	return nil
}
func (m *mockFriendRedisRepo) ReadInbox(_ context.Context, _ int64, _, _ int64, _ int) ([]model.InboxMessage, error) {
	return nil, nil
}
func (m *mockFriendRedisRepo) ReadOutbox(_ context.Context, _ int64, _, _ int64, _ int) ([]model.InboxMessage, error) {
	return nil, nil
}

func (m *mockFriendRedisRepo) UpdateConvList(_ context.Context, userID int64, convID, summary string, timestamp int64) error {
	m.convUpdates = append(m.convUpdates, friendConvUpdate{userID: userID, convID: convID, summary: summary, timestamp: timestamp})
	return nil
}
func (m *mockFriendRedisRepo) GetConvList(_ context.Context, _ int64) ([]model.ConvSummary, error) {
	return nil, nil
}
func (m *mockFriendRedisRepo) IncrementUnread(_ context.Context, _ int64, _ string) error { return nil }
func (m *mockFriendRedisRepo) ClearUnread(_ context.Context, _ int64, _ string) error     { return nil }
func (m *mockFriendRedisRepo) GetUnreadMap(_ context.Context, _ int64) (map[string]int64, error) {
	return nil, nil
}
func (m *mockFriendRedisRepo) SetGroupReadPos(_ context.Context, _ int64, _ string, _ int64) error {
	return nil
}
func (m *mockFriendRedisRepo) GetGroupReadPos(_ context.Context, _ int64, _ string) (int64, error) {
	return 0, nil
}

func (m *mockFriendRedisRepo) GetGroupSeq(_ context.Context, _ int64) (int64, error) { return 0, nil }
func (m *mockFriendRedisRepo) GetGroupMemberships(_ context.Context, _ int64) ([]int64, error) {
	return nil, nil
}
func (m *mockFriendRedisRepo) GetGroupMembers(_ context.Context, _ int64) ([]int64, error) {
	return nil, nil
}
func (m *mockFriendRedisRepo) CheckDuplicate(_ context.Context, _ int64, _ string) (bool, error) {
	return false, nil
}
func (m *mockFriendRedisRepo) TrimInbox(_ context.Context, _ int64, _ int) error          { return nil }
func (m *mockFriendRedisRepo) TrimOutbox(_ context.Context, _ int64, _ int) error         { return nil }
func (m *mockFriendRedisRepo) TrimInboxByTime(_ context.Context, _ int64, _ int64) error  { return nil }
func (m *mockFriendRedisRepo) TrimOutboxByTime(_ context.Context, _ int64, _ int64) error { return nil }
func (m *mockFriendRedisRepo) TrimConvListByTime(_ context.Context, _ int64, _ int64) error {
	return nil
}
func (m *mockFriendRedisRepo) TrimTimelineByTime(_ context.Context, _ int64, _ int64) error {
	return nil
}

func (m *mockFriendRedisRepo) ExecPrivateMsgCheck(_ context.Context, _ int64, _ int64, _ string) (*redislua.PrivateMsgCheckResult, error) {
	return nil, nil
}
func (m *mockFriendRedisRepo) ExecGroupMsgCheck(_ context.Context, _ int64, _ int64, _ string) (*redislua.GroupMsgCheckResult, error) {
	return nil, nil
}
func (m *mockFriendRedisRepo) ExecInboxMarkRead(_ context.Context, _ int64, _ string) (int64, error) {
	return 0, nil
}
func (m *mockFriendRedisRepo) ExecRevokeMsg(_ context.Context, _ int64, _ string, _ int64, _ string, _ int64) (bool, error) {
	return false, nil
}
func (m *mockFriendRedisRepo) AddGroupMemberRedis(_ context.Context, _ int64, _ int64) error {
	return nil
}
func (m *mockFriendRedisRepo) RemoveGroupMemberRedis(_ context.Context, _ int64, _ int64) error {
	return nil
}
func (m *mockFriendRedisRepo) PublishMomentFeed(_ context.Context, _ int64, _ int64, _ int64) error {
	return nil
}
func (m *mockFriendRedisRepo) GetMomentFeed(_ context.Context, _ int64, _ int64, _ int) ([]int64, error) {
	return nil, nil
}
func (m *mockFriendRedisRepo) FanoutMomentFeed(_ context.Context, recipientIDs []int64, momentID int64, timestamp int64, _ int) error {
	for _, recipientID := range recipientIDs {
		m.momentFanouts = append(m.momentFanouts, friendMomentFanout{recipientID: recipientID, momentID: momentID, timestamp: timestamp})
	}
	return nil
}
func (m *mockFriendRedisRepo) AddToOutbox(_ context.Context, _ int64, _ int64, _ int64, _ int) error {
	return nil
}
func (m *mockFriendRedisRepo) MarkBigUser(_ context.Context, _ int64) error { return nil }
func (m *mockFriendRedisRepo) FilterBigUsers(_ context.Context, _ []int64) ([]int64, error) {
	return nil, nil
}
func (m *mockFriendRedisRepo) GetTimelinePage(_ context.Context, _ int64, _ int64, _ int64, _ int) ([]model.FeedEntry, error) {
	return nil, nil
}
func (m *mockFriendRedisRepo) GetOutboxPage(_ context.Context, _ int64, _ int64, _ int64, _ int) ([]model.FeedEntry, error) {
	return nil, nil
}
func (m *mockFriendRedisRepo) SetFriendCache(_ context.Context, _ int64, _ int64) error { return nil }
func (m *mockFriendRedisRepo) DeleteFriendCache(_ context.Context, uidA, uidB int64) error {
	m.deletedFriendCaches = append(m.deletedFriendCaches, [2]int64{uidA, uidB})
	return nil
}

// ──────────────────────────────────────────────────────
// 辅助函数：新建测试用 FriendService
// ──────────────────────────────────────────────────────

func newTestFriendService(repo *mockFriendRepo) *FriendService {
	logger := zap.NewNop()
	return NewFriendService(repo, &mockFriendRedisRepo{}, logger)
}

func newTestFriendServiceWithRedis(repo *mockFriendRepo, redisRepo *mockFriendRedisRepo) *FriendService {
	return NewFriendService(repo, redisRepo, zap.NewNop())
}

// ──────────────────────────────────────────────────────
// SendFriendRequest 测试
// ──────────────────────────────────────────────────────

func TestFriend_SendFriendRequest_Success(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 添加两个用户
	repo.mu.Lock()
	repo.users[1] = &model.User{ID: 1, Nickname: "alice", AvatarURL: "avatar1"}
	repo.users[2] = &model.User{ID: 2, Nickname: "bob", AvatarURL: "avatar2"}
	repo.mu.Unlock()

	req, err := svc.SendFriendRequest(context.Background(), 1, 2, "let's be friends")
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.Equal(t, int64(1), req.ID)
	assert.Equal(t, int64(1), req.FromUserID)
	assert.Equal(t, int64(2), req.ToUserID)
	assert.Equal(t, 0, req.Status)
	assert.Equal(t, "let's be friends", req.Message)
}

func TestFriend_SendFriendRequest_SelfRequest(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	_, err := svc.SendFriendRequest(context.Background(), 1, 1, "")
	assert.Error(t, err)
	assert.Equal(t, ErrSelfRequest, err.Error())
}

func TestFriend_SendFriendRequest_AlreadyFriends(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 设置已有好友关系
	repo.mu.Lock()
	repo.friendships[friendKey(1, 2)] = &model.Friendship{UserID: 1, FriendID: 2}
	repo.friendships[friendKey(2, 1)] = &model.Friendship{UserID: 2, FriendID: 1}
	repo.mu.Unlock()

	_, err := svc.SendFriendRequest(context.Background(), 1, 2, "")
	assert.Error(t, err)
	assert.Equal(t, ErrAlreadyFriends, err.Error())
}

func TestFriend_SendFriendRequest_Blocked(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 设置黑名单条目：用户 2 屏蔽了用户 1
	repo.mu.Lock()
	repo.blacklist[blackKey(2, 1)] = &model.Blacklist{UserID: 2, BlockedID: 1}
	repo.mu.Unlock()

	_, err := svc.SendFriendRequest(context.Background(), 1, 2, "")
	assert.Error(t, err)
	assert.Equal(t, ErrFriendBlocked, err.Error())
}

func TestFriend_SendFriendRequest_BlockedBySender(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 设置黑名单条目：用户 1 屏蔽了用户 2
	repo.mu.Lock()
	repo.blacklist[blackKey(1, 2)] = &model.Blacklist{UserID: 1, BlockedID: 2}
	repo.mu.Unlock()

	_, err := svc.SendFriendRequest(context.Background(), 1, 2, "")
	assert.Error(t, err)
	assert.Equal(t, ErrFriendBlocked, err.Error())
}

func TestFriend_SendFriendRequest_DuplicatePending(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 第一次请求成功
	_, err := svc.SendFriendRequest(context.Background(), 1, 2, "hello")
	assert.NoError(t, err)

	// 同一用户向同一目标发送的第二次请求应该失败
	_, err = svc.SendFriendRequest(context.Background(), 1, 2, "hello again")
	assert.Error(t, err)
	assert.Equal(t, ErrDuplicateRequest, err.Error())
}

func TestFriend_SendFriendRequest_DuplicatePendingReverse(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 用户 2 向用户 1 发送请求
	_, err := svc.SendFriendRequest(context.Background(), 2, 1, "hello")
	assert.NoError(t, err)

	// 用户 1 尝试向用户 2 发送请求 — 应该检测到已存在的待处理请求
	_, err = svc.SendFriendRequest(context.Background(), 1, 2, "hello back")
	assert.Error(t, err)
	assert.Equal(t, ErrDuplicateRequest, err.Error())
}

func TestFriend_SendFriendRequest_ReopenRejected(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	first, err := svc.SendFriendRequest(context.Background(), 1, 2, "first request")
	assert.NoError(t, err)
	assert.NoError(t, svc.RejectFriendRequest(context.Background(), 2, first.ID))

	reopened, err := svc.SendFriendRequest(context.Background(), 1, 2, "try again")
	assert.NoError(t, err)
	assert.Equal(t, first.ID, reopened.ID, "应复用同方向的历史申请记录")
	assert.Equal(t, 0, reopened.Status)
	assert.Equal(t, "try again", reopened.Message)

	pending, err := svc.GetFriendRequests(context.Background(), 2)
	assert.NoError(t, err)
	if assert.Len(t, pending, 1) {
		assert.Equal(t, first.ID, pending[0].ID)
		assert.Equal(t, 0, pending[0].Status)
		assert.Equal(t, "try again", pending[0].Message)
	}

	_, err = svc.SendFriendRequest(context.Background(), 1, 2, "duplicate pending")
	assert.EqualError(t, err, ErrDuplicateRequest)
}

// ──────────────────────────────────────────────────────
// AcceptFriendRequest 测试
// ──────────────────────────────────────────────────────

func TestFriend_AcceptFriendRequest_Success(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 先创建一个好友请求
	req, err := svc.SendFriendRequest(context.Background(), 1, 2, "hello")
	assert.NoError(t, err)

	// 以目标用户（用户 2）的身份接受
	fs, err := svc.AcceptFriendRequest(context.Background(), 2, req.ID)
	assert.NoError(t, err)
	assert.NotNil(t, fs)
	assert.Equal(t, int64(1), fs.UserID)
	assert.Equal(t, int64(2), fs.FriendID)

	// 验证请求状态已更新
	repo.mu.Lock()
	storedReq := repo.friendRequests[req.ID]
	repo.mu.Unlock()
	assert.Equal(t, 1, storedReq.Status)

	// 验证好友关系已双向创建
	isFriend, err := repo.IsFriend(context.Background(), 1, 2)
	assert.NoError(t, err)
	assert.True(t, isFriend)
	isFriend, err = repo.IsFriend(context.Background(), 2, 1)
	assert.NoError(t, err)
	assert.True(t, isFriend)
}

func TestFriend_AcceptFriendRequest_CreatesEmptyConversationForBothUsers(t *testing.T) {
	repo := newMockFriendRepo()
	repo.users[1] = &model.User{ID: 1, Username: "alice", AvatarURL: "/alice.png"}
	repo.users[2] = &model.User{ID: 2, Username: "bob", AvatarURL: "/bob.png"}
	redisRepo := &mockFriendRedisRepo{}
	svc := newTestFriendServiceWithRedis(repo, redisRepo)

	req, err := svc.SendFriendRequest(context.Background(), 1, 2, "hello")
	assert.NoError(t, err)
	_, err = svc.AcceptFriendRequest(context.Background(), 2, req.ID)
	assert.NoError(t, err)

	if assert.Len(t, redisRepo.convUpdates, 2) {
		for _, update := range redisRepo.convUpdates {
			assert.Equal(t, "p_1_2", update.convID)
			var summary model.ConvSummary
			assert.NoError(t, json.Unmarshal([]byte(update.summary), &summary))
			assert.Equal(t, model.ConvTypePrivate, summary.ConvType)
			assert.NotZero(t, summary.TargetID)
		}
	}
}

func TestFriend_DeleteFriend_RemovesPrivateMessageAuthorizationCache(t *testing.T) {
	repo := newMockFriendRepo()
	repo.friendships[friendKey(1, 2)] = &model.Friendship{UserID: 1, FriendID: 2}
	repo.friendships[friendKey(2, 1)] = &model.Friendship{UserID: 2, FriendID: 1}
	redisRepo := &mockFriendRedisRepo{}
	svc := newTestFriendServiceWithRedis(repo, redisRepo)

	assert.NoError(t, svc.DeleteFriend(context.Background(), 1, 2))
	assert.Equal(t, [][2]int64{{1, 2}}, redisRepo.deletedFriendCaches)
}

func TestFriend_AcceptFriendRequest_BackfillsVisibleMoments(t *testing.T) {
	repo := newMockFriendRepo()
	redisRepo := &mockFriendRedisRepo{}
	svc := newTestFriendServiceWithRedis(repo, redisRepo)
	now := time.Now()
	repo.momentsByUser[1] = []model.Moment{
		{ID: 101, AuthorID: 1, Visibility: 2, CreatedAt: now.Add(-time.Minute)},
		{ID: 102, AuthorID: 1, Visibility: 3, CreatedAt: now},
	}
	repo.momentsByUser[2] = []model.Moment{
		{ID: 201, AuthorID: 2, Visibility: 1, CreatedAt: now.Add(-2 * time.Minute)},
	}

	req, err := svc.SendFriendRequest(context.Background(), 1, 2, "hello")
	assert.NoError(t, err)
	_, err = svc.AcceptFriendRequest(context.Background(), 2, req.ID)
	assert.NoError(t, err)

	assert.Equal(t, []friendMomentFanout{
		{recipientID: 2, momentID: 101, timestamp: now.Add(-time.Minute).UnixMilli()},
		{recipientID: 1, momentID: 201, timestamp: now.Add(-2 * time.Minute).UnixMilli()},
	}, redisRepo.momentFanouts)
}

func TestFriend_AcceptFriendRequest_WrongTarget(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 创建好友请求：用户 1 → 用户 2
	req, err := svc.SendFriendRequest(context.Background(), 1, 2, "hello")
	assert.NoError(t, err)

	// 尝试以用户 3（非目标用户）的身份接受
	_, err = svc.AcceptFriendRequest(context.Background(), 3, req.ID)
	assert.Error(t, err)
	assert.Equal(t, ErrNotRequestTarget, err.Error())
}

func TestFriend_AcceptFriendRequest_NotFound(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	_, err := svc.AcceptFriendRequest(context.Background(), 2, 999)
	assert.Error(t, err)
	assert.Equal(t, ErrRequestNotFound, err.Error())
}

// ──────────────────────────────────────────────────────
// RejectFriendRequest 测试
// ──────────────────────────────────────────────────────

func TestFriend_RejectFriendRequest_Success(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 创建一个好友请求
	req, err := svc.SendFriendRequest(context.Background(), 1, 2, "hello")
	assert.NoError(t, err)

	// 以目标用户（用户 2）的身份拒绝
	err = svc.RejectFriendRequest(context.Background(), 2, req.ID)
	assert.NoError(t, err)

	// 验证请求状态已更新
	repo.mu.Lock()
	storedReq := repo.friendRequests[req.ID]
	repo.mu.Unlock()
	assert.Equal(t, 2, storedReq.Status)
}

func TestFriend_RejectFriendRequest_WrongTarget(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 创建好友请求：用户 1 → 用户 2
	req, err := svc.SendFriendRequest(context.Background(), 1, 2, "hello")
	assert.NoError(t, err)

	// 尝试以用户 3（非目标用户）的身份拒绝
	err = svc.RejectFriendRequest(context.Background(), 3, req.ID)
	assert.Error(t, err)
	assert.Equal(t, ErrNotRequestTarget, err.Error())
}

func TestFriend_RejectFriendRequest_NotFound(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	err := svc.RejectFriendRequest(context.Background(), 2, 999)
	assert.Error(t, err)
	assert.Equal(t, ErrRequestNotFound, err.Error())
}

// ──────────────────────────────────────────────────────
// GetFriendRequests 测试
// ──────────────────────────────────────────────────────

func TestFriend_GetFriendRequests(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 创建两个涉及用户 1 的好友请求
	_, err := svc.SendFriendRequest(context.Background(), 1, 2, "hello")
	assert.NoError(t, err)
	_, err = svc.SendFriendRequest(context.Background(), 3, 1, "hi")
	assert.NoError(t, err)

	requests, err := svc.GetFriendRequests(context.Background(), 1)
	assert.NoError(t, err)
	assert.Len(t, requests, 2)
}

func TestFriend_GetFriendRequests_OnlyPending(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	repo.friendRequests[1] = &model.FriendRequest{ID: 1, FromUserID: 1, ToUserID: 2, Status: 0}
	repo.friendRequests[2] = &model.FriendRequest{ID: 2, FromUserID: 3, ToUserID: 1, Status: 1}
	repo.friendRequests[3] = &model.FriendRequest{ID: 3, FromUserID: 1, ToUserID: 4, Status: 2}
	repo.friendRequests[4] = &model.FriendRequest{ID: 4, FromUserID: 5, ToUserID: 6, Status: 0}

	requests, err := svc.GetFriendRequests(context.Background(), 1)
	assert.NoError(t, err)
	if assert.Len(t, requests, 1) {
		assert.Equal(t, int64(1), requests[0].ID)
		assert.Equal(t, 0, requests[0].Status)
	}
}

func TestFriend_GetFriendRequestList_IncludesApplicantProfile(t *testing.T) {
	repo := newMockFriendRepo()
	repo.users[2] = &model.User{ID: 2, Username: "applicant", AvatarURL: "/uploads/applicant.png"}
	repo.friendRequests[1] = &model.FriendRequest{ID: 1, FromUserID: 2, ToUserID: 1, Status: 0}
	svc := newTestFriendService(repo)

	requests, err := svc.GetFriendRequestList(context.Background(), 1)
	assert.NoError(t, err)
	if assert.Len(t, requests, 1) {
		assert.Equal(t, "applicant", requests[0].Username)
		assert.Equal(t, "/uploads/applicant.png", requests[0].AvatarURL)
	}
}

// ──────────────────────────────────────────────────────
// DeleteFriend 测试
// ──────────────────────────────────────────────────────

func TestFriend_DeleteFriend_Success(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 设置好友关系
	repo.mu.Lock()
	repo.friendships[friendKey(1, 2)] = &model.Friendship{UserID: 1, FriendID: 2}
	repo.friendships[friendKey(2, 1)] = &model.Friendship{UserID: 2, FriendID: 1}
	repo.mu.Unlock()

	err := svc.DeleteFriend(context.Background(), 1, 2)
	assert.NoError(t, err)

	// 验证双向删除
	repo.mu.Lock()
	_, ok1 := repo.friendships[friendKey(1, 2)]
	_, ok2 := repo.friendships[friendKey(2, 1)]
	repo.mu.Unlock()
	assert.False(t, ok1)
	assert.False(t, ok2)
}

// ──────────────────────────────────────────────────────
// BlockUser / UnblockUser / IsBlocked 测试
// ──────────────────────────────────────────────────────

func TestFriend_BlockUser_Success(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	err := svc.BlockUser(context.Background(), 1, 2)
	assert.NoError(t, err)

	// 验证已屏蔽
	isBlocked, err := svc.IsBlocked(context.Background(), 1, 2)
	assert.NoError(t, err)
	assert.True(t, isBlocked)
}

func TestFriend_BlockUser_AlreadyBlocked(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 第一次屏蔽
	err := svc.BlockUser(context.Background(), 1, 2)
	assert.NoError(t, err)

	// 再次屏蔽应该失败
	err = svc.BlockUser(context.Background(), 1, 2)
	assert.Error(t, err)
	assert.Equal(t, ErrAlreadyBlocked, err.Error())
}

func TestFriend_UnblockUser_Success(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 先屏蔽
	err := svc.BlockUser(context.Background(), 1, 2)
	assert.NoError(t, err)

	// 取消屏蔽
	err = svc.UnblockUser(context.Background(), 1, 2)
	assert.NoError(t, err)

	// 验证已取消屏蔽
	isBlocked, err := svc.IsBlocked(context.Background(), 1, 2)
	assert.NoError(t, err)
	assert.False(t, isBlocked)
}

func TestFriend_IsBlocked_NotBlocked(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	isBlocked, err := svc.IsBlocked(context.Background(), 1, 2)
	assert.NoError(t, err)
	assert.False(t, isBlocked)
}

// ──────────────────────────────────────────────────────
// GetFriendList 测试
// ──────────────────────────────────────────────────────

func TestFriend_GetFriendList_WithProfile(t *testing.T) {
	repo := newMockFriendRepo()
	svc := newTestFriendService(repo)

	// 设置好友关系和用户
	repo.mu.Lock()
	repo.friendships[friendKey(1, 2)] = &model.Friendship{ID: 1, UserID: 1, FriendID: 2, CreatedAt: time.Now()}
	repo.users[2] = &model.User{ID: 2, Nickname: "bob", AvatarURL: "avatar2"}
	repo.blacklist[blackKey(1, 2)] = &model.Blacklist{ID: 1, UserID: 1, BlockedID: 2}
	repo.mu.Unlock()

	friends, err := svc.GetFriendList(context.Background(), 1)
	assert.NoError(t, err)
	assert.Len(t, friends, 1)
	assert.Equal(t, "bob", friends[0].Nickname)
	assert.Equal(t, "avatar2", friends[0].AvatarURL)
	assert.True(t, friends[0].IsBlocked)
}

// ── 高并发点赞新增接口的 mock 桩 ──

func (m *mockFriendRepo) GetMomentLikers(_ context.Context, _ int64) ([]int64, error) {
	return nil, nil
}
func (m *mockFriendRepo) BatchUpsertMomentLikes(_ context.Context, _ []model.MomentLike) error {
	return nil
}
func (m *mockFriendRepo) BatchDeleteMomentLikes(_ context.Context, _ []model.MomentLikeKey) error {
	return nil
}

func (m *mockFriendRedisRepo) LikeMomentAtomic(_ context.Context, _ int64, _ int64) (bool, int64, error) {
	return false, 0, nil
}
func (m *mockFriendRedisRepo) UnlikeMomentAtomic(_ context.Context, _ int64, _ int64) (bool, int64, error) {
	return false, 0, nil
}
func (m *mockFriendRedisRepo) EnsureMomentLikesLoaded(_ context.Context, _ int64, _ func(context.Context) ([]int64, error), _ time.Duration) error {
	return nil
}
func (m *mockFriendRedisRepo) GetMomentLikeStats(_ context.Context, _ int64, _ []int64) (map[int64]int64, map[int64]bool, error) {
	return nil, nil, nil
}
func (m *mockFriendRedisRepo) GetMomentLikerIDs(_ context.Context, _ int64) ([]int64, error) {
	return nil, nil
}
func (m *mockFriendRedisRepo) DeleteMomentLikes(_ context.Context, _ int64) error { return nil }
