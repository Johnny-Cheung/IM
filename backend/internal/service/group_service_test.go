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
	"github.com/goim/goim/internal/redis"
	"github.com/goim/goim/internal/repository"
)

// ──────────────────────────────────────────────────────
// 模拟 MySQLRepo 用于群组测试
// ──────────────────────────────────────────────────────

type mockGroupMySQLRepo struct {
	mu sync.Mutex

	// 按 ID 存储的群组
	groupsByID map[int64]*model.Group
	// 按复合键 "groupID:userID" 存储的群组成员
	membersByKey map[string]*model.GroupMember
	// 按 groupID 存储的成员列表
	membersByGroup map[int64][]model.GroupMember
	// 下一个自增群组 ID
	nextGroupID int64
	// 下一个自增成员 ID
	nextMemberID int64

	// 错误覆盖项
	createGroupErr    error
	getGroupByIDErr   error
	addGroupMemberErr error
}

func newMockGroupMySQLRepo() *mockGroupMySQLRepo {
	return &mockGroupMySQLRepo{
		groupsByID:     make(map[int64]*model.Group),
		membersByKey:   make(map[string]*model.GroupMember),
		membersByGroup: make(map[int64][]model.GroupMember),
		nextGroupID:    1,
		nextMemberID:   1,
	}
}

func (m *mockGroupMySQLRepo) CreateGroup(_ context.Context, group *model.Group) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createGroupErr != nil {
		return 0, m.createGroupErr
	}
	group.ID = m.nextGroupID
	m.nextGroupID++
	group.MaxMembers = 500
	group.CreatedAt = time.Now()
	group.UpdatedAt = time.Now()
	m.groupsByID[group.ID] = group
	return group.ID, nil
}

func (m *mockGroupMySQLRepo) UpdateGroup(_ context.Context, group *model.Group) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	stored, ok := m.groupsByID[group.ID]
	if !ok {
		return fmt.Errorf("群组未找到")
	}
	stored.Name = group.Name
	stored.Notice = group.Notice
	stored.OwnerID = group.OwnerID
	stored.UpdatedAt = time.Now()
	return nil
}

func (m *mockGroupMySQLRepo) GetGroupByID(_ context.Context, groupID int64) (*model.Group, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getGroupByIDErr != nil {
		return nil, m.getGroupByIDErr
	}
	g, ok := m.groupsByID[groupID]
	if !ok {
		return nil, nil
	}
	return g, nil
}

func (m *mockGroupMySQLRepo) AddGroupMember(_ context.Context, member *model.GroupMember) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.addGroupMemberErr != nil {
		return m.addGroupMemberErr
	}
	key := fmt.Sprintf("%d:%d", member.GroupID, member.UserID)
	if _, exists := m.membersByKey[key]; exists {
		return fmt.Errorf("重复成员")
	}
	member.ID = m.nextMemberID
	m.nextMemberID++
	member.JoinedAt = time.Now()
	m.membersByKey[key] = member
	m.membersByGroup[member.GroupID] = append(m.membersByGroup[member.GroupID], *member)
	return nil
}

func (m *mockGroupMySQLRepo) RemoveGroupMember(_ context.Context, groupID, userID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%d:%d", groupID, userID)
	if _, exists := m.membersByKey[key]; !exists {
		return nil // 未找到则无操作
	}
	delete(m.membersByKey, key)
	members := m.membersByGroup[groupID]
	filtered := make([]model.GroupMember, 0, len(members))
	for _, gm := range members {
		if gm.UserID != userID {
			filtered = append(filtered, gm)
		}
	}
	m.membersByGroup[groupID] = filtered
	return nil
}

func (m *mockGroupMySQLRepo) GetGroupMembers(_ context.Context, groupID int64) ([]model.GroupMember, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.membersByGroup[groupID], nil
}

func (m *mockGroupMySQLRepo) UpdateGroupMemberRole(_ context.Context, groupID, userID, role int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%d:%d", groupID, userID)
	member, ok := m.membersByKey[key]
	if !ok {
		return fmt.Errorf("成员未找到")
	}
	member.Role = role
	// 同时更新切片中的数据
	members := m.membersByGroup[int64(groupID)]
	for i := range members {
		if members[i].UserID == int64(userID) {
			members[i].Role = role
		}
	}
	return nil
}

// ── 存根：所有其他 MySQLRepo 方法 ──

func (m *mockGroupMySQLRepo) GetUserByID(_ context.Context, id int64) (*model.User, error) {
	return &model.User{ID: id, Username: fmt.Sprintf("user%d", id)}, nil
}
func (m *mockGroupMySQLRepo) GetUserByUsername(_ context.Context, _ string) (*model.User, error) {
	return nil, nil
}
func (m *mockGroupMySQLRepo) CreateUser(_ context.Context, _ *model.User) error { return nil }
func (m *mockGroupMySQLRepo) UpdateUser(_ context.Context, _ *model.User) error { return nil }
func (m *mockGroupMySQLRepo) DeleteMoment(_ context.Context, _ int64) error     { return nil }

func (m *mockGroupMySQLRepo) InsertPrivateMessage(_ context.Context, _ *model.PrivateMessage) error {
	return nil
}
func (m *mockGroupMySQLRepo) InsertGroupMessage(_ context.Context, _ *model.GroupMessage) error {
	return nil
}
func (m *mockGroupMySQLRepo) InsertMsgRevoked(_ context.Context, _ *model.MsgRevoked) error {
	return nil
}

func (m *mockGroupMySQLRepo) CreateFriendRequest(_ context.Context, _ *model.FriendRequest) error {
	return nil
}
func (m *mockGroupMySQLRepo) UpdateFriendRequest(_ context.Context, _ *model.FriendRequest) error {
	return nil
}
func (m *mockGroupMySQLRepo) GetFriendRequestByID(_ context.Context, _ int64) (*model.FriendRequest, error) {
	return nil, nil
}
func (m *mockGroupMySQLRepo) GetFriendRequestsByUser(_ context.Context, _ int64) ([]model.FriendRequest, error) {
	return nil, nil
}
func (m *mockGroupMySQLRepo) CreateFriendship(_ context.Context, _ *model.Friendship) error {
	return nil
}
func (m *mockGroupMySQLRepo) DeleteFriendship(_ context.Context, _ int64, _ int64) error { return nil }
func (m *mockGroupMySQLRepo) GetFriendList(_ context.Context, _ int64) ([]model.Friendship, error) {
	return nil, nil
}
func (m *mockGroupMySQLRepo) IsFriend(_ context.Context, _ int64, _ int64) (bool, error) {
	return true, nil
}
func (m *mockGroupMySQLRepo) CreateBlacklist(_ context.Context, _ *model.Blacklist) error { return nil }
func (m *mockGroupMySQLRepo) DeleteBlacklist(_ context.Context, _ int64, _ int64) error   { return nil }
func (m *mockGroupMySQLRepo) IsBlocked(_ context.Context, _ int64, _ int64) (bool, error) {
	return false, nil
}

func (m *mockGroupMySQLRepo) CreateMoment(_ context.Context, _ *model.Moment) error { return nil }
func (m *mockGroupMySQLRepo) GetMomentByID(_ context.Context, _ int64) (*model.Moment, error) {
	return nil, nil
}
func (m *mockGroupMySQLRepo) GetMomentsByUser(_ context.Context, _ int64, _ int, _ int) ([]model.Moment, error) {
	return nil, nil
}
func (m *mockGroupMySQLRepo) CreateMomentLike(_ context.Context, _ *model.MomentLike) error {
	return nil
}
func (m *mockGroupMySQLRepo) DeleteMomentLike(_ context.Context, _ int64, _ int64) error { return nil }
func (m *mockGroupMySQLRepo) CreateMomentComment(_ context.Context, _ *model.MomentComment) error {
	return nil
}
func (m *mockGroupMySQLRepo) DeleteMomentComment(_ context.Context, _ int64) error { return nil }
func (m *mockGroupMySQLRepo) CountFriends(_ context.Context, _ int64) (int, error) { return 0, nil }
func (m *mockGroupMySQLRepo) GetMomentsByIDs(_ context.Context, _ []int64) ([]model.Moment, error) {
	return nil, nil
}

func (m *mockGroupMySQLRepo) GetMomentCommentByID(_ context.Context, _ int64) (*model.MomentComment, error) {
	return nil, nil
}
func (m *mockGroupMySQLRepo) GetMomentComments(_ context.Context, _ int64) ([]model.MomentComment, error) {
	return nil, nil
}

func (m *mockGroupMySQLRepo) GetUserSettings(_ context.Context, _ int64) (*model.UserSettings, error) {
	return nil, nil
}
func (m *mockGroupMySQLRepo) CreateOrUpdateUserSettings(_ context.Context, _ *model.UserSettings) error {
	return nil
}
func (m *mockGroupMySQLRepo) SearchPrivateMessages(_ context.Context, _ int64, _ string, _ int, _ int) ([]model.PrivateMessage, error) {
	return nil, nil
}

// ──────────────────────────────────────────────────────
// 模拟 RedisRepo 用于群组测试
// ──────────────────────────────────────────────────────

type mockGroupRedisRepo struct {
	mu sync.Mutex

	// group_members:{groupID} -> userID 字符串集合
	groupMembersSets map[int64]map[string]bool
	// user_groups:{userID} -> groupID 字符串集合
	userGroupsSets map[int64]map[string]bool

	// 错误覆盖项
	addGroupMemberErr    error
	removeGroupMemberErr error
}

func newMockGroupRedisRepo() *mockGroupRedisRepo {
	return &mockGroupRedisRepo{
		groupMembersSets: make(map[int64]map[string]bool),
		userGroupsSets:   make(map[int64]map[string]bool),
	}
}

func (r *mockGroupRedisRepo) GetGroupMemberships(_ context.Context, userID int64) ([]int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	set, ok := r.userGroupsSets[userID]
	if !ok {
		return []int64{}, nil
	}
	result := make([]int64, 0, len(set))
	for k := range set {
		id, _ := fmt.Sscanf(k, "%d", new(int64))
		_ = id
		result = append(result, 0) // 简化处理
	}
	return result, nil
}

func (r *mockGroupRedisRepo) GetGroupMembers(_ context.Context, groupID int64) ([]int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	set, ok := r.groupMembersSets[groupID]
	if !ok {
		return []int64{}, nil
	}
	result := make([]int64, 0, len(set))
	for k := range set {
		var id int64
		fmt.Sscanf(k, "%d", &id)
		result = append(result, id)
	}
	return result, nil
}

func (r *mockGroupRedisRepo) AddGroupMemberRedis(_ context.Context, groupID, userID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.addGroupMemberErr != nil {
		return r.addGroupMemberErr
	}
	userIDStr := fmt.Sprintf("%d", userID)
	groupIDStr := fmt.Sprintf("%d", groupID)

	if r.groupMembersSets[groupID] == nil {
		r.groupMembersSets[groupID] = make(map[string]bool)
	}
	r.groupMembersSets[groupID][userIDStr] = true

	if r.userGroupsSets[userID] == nil {
		r.userGroupsSets[userID] = make(map[string]bool)
	}
	r.userGroupsSets[userID][groupIDStr] = true
	return nil
}

func (r *mockGroupRedisRepo) RemoveGroupMemberRedis(_ context.Context, groupID, userID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.removeGroupMemberErr != nil {
		return r.removeGroupMemberErr
	}
	userIDStr := fmt.Sprintf("%d", userID)
	groupIDStr := fmt.Sprintf("%d", groupID)

	delete(r.groupMembersSets[groupID], userIDStr)
	delete(r.userGroupsSets[userID], groupIDStr)
	return nil
}

// ── 存根：所有其他 RedisRepo 方法 ──

func (r *mockGroupRedisRepo) WriteInbox(_ context.Context, _ int64, _ *model.InboxMessage) error {
	return nil
}
func (r *mockGroupRedisRepo) WriteOutbox(_ context.Context, _ int64, _ *model.InboxMessage) error {
	return nil
}
func (r *mockGroupRedisRepo) ReadInbox(_ context.Context, _ int64, _, _ int64, _ int) ([]model.InboxMessage, error) {
	return nil, nil
}
func (r *mockGroupRedisRepo) ReadOutbox(_ context.Context, _ int64, _, _ int64, _ int) ([]model.InboxMessage, error) {
	return nil, nil
}
func (r *mockGroupRedisRepo) UpdateConvList(_ context.Context, _ int64, _ string, _ string, _ int64) error {
	return nil
}
func (r *mockGroupRedisRepo) GetConvList(_ context.Context, _ int64) ([]model.ConvSummary, error) {
	return nil, nil
}
func (r *mockGroupRedisRepo) IncrementUnread(_ context.Context, _ int64, _ string) error { return nil }
func (r *mockGroupRedisRepo) ClearUnread(_ context.Context, _ int64, _ string) error     { return nil }
func (r *mockGroupRedisRepo) GetUnreadMap(_ context.Context, _ int64) (map[string]int64, error) {
	return nil, nil
}
func (r *mockGroupRedisRepo) SetGroupReadPos(_ context.Context, _ int64, _ string, _ int64) error {
	return nil
}
func (r *mockGroupRedisRepo) GetGroupReadPos(_ context.Context, _ int64, _ string) (int64, error) {
	return 0, nil
}

func (r *mockGroupRedisRepo) GetGroupSeq(_ context.Context, _ int64) (int64, error) { return 0, nil }
func (r *mockGroupRedisRepo) CheckDuplicate(_ context.Context, _ int64, _ string) (bool, error) {
	return false, nil
}
func (r *mockGroupRedisRepo) TrimInbox(_ context.Context, _ int64, _ int) error          { return nil }
func (r *mockGroupRedisRepo) TrimOutbox(_ context.Context, _ int64, _ int) error         { return nil }
func (r *mockGroupRedisRepo) TrimInboxByTime(_ context.Context, _ int64, _ int64) error  { return nil }
func (r *mockGroupRedisRepo) TrimOutboxByTime(_ context.Context, _ int64, _ int64) error { return nil }
func (r *mockGroupRedisRepo) TrimConvListByTime(_ context.Context, _ int64, _ int64) error {
	return nil
}
func (r *mockGroupRedisRepo) TrimTimelineByTime(_ context.Context, _ int64, _ int64) error {
	return nil
}

func (r *mockGroupRedisRepo) ExecPrivateMsgCheck(_ context.Context, _ int64, _ int64, _ string) (*redis.PrivateMsgCheckResult, error) {
	return nil, nil
}
func (r *mockGroupRedisRepo) ExecGroupMsgCheck(_ context.Context, _ int64, _ int64, _ string) (*redis.GroupMsgCheckResult, error) {
	return nil, nil
}
func (r *mockGroupRedisRepo) ExecInboxMarkRead(_ context.Context, _ int64, _ string) (int64, error) {
	return 0, nil
}
func (r *mockGroupRedisRepo) ExecRevokeMsg(_ context.Context, _ int64, _ string, _ int64, _ string, _ int64) (bool, error) {
	return false, nil
}
func (r *mockGroupRedisRepo) PublishMomentFeed(_ context.Context, _ int64, _ int64, _ int64) error {
	return nil
}
func (r *mockGroupRedisRepo) GetMomentFeed(_ context.Context, _ int64, _ int64, _ int) ([]int64, error) {
	return nil, nil
}
func (r *mockGroupRedisRepo) FanoutMomentFeed(_ context.Context, _ []int64, _ int64, _ int64, _ int) error {
	return nil
}
func (r *mockGroupRedisRepo) AddToOutbox(_ context.Context, _ int64, _ int64, _ int64, _ int) error {
	return nil
}
func (r *mockGroupRedisRepo) MarkBigUser(_ context.Context, _ int64) error { return nil }
func (r *mockGroupRedisRepo) FilterBigUsers(_ context.Context, _ []int64) ([]int64, error) {
	return nil, nil
}
func (r *mockGroupRedisRepo) GetTimelinePage(_ context.Context, _ int64, _ int64, _ int64, _ int) ([]model.FeedEntry, error) {
	return nil, nil
}
func (r *mockGroupRedisRepo) GetOutboxPage(_ context.Context, _ int64, _ int64, _ int64, _ int) ([]model.FeedEntry, error) {
	return nil, nil
}
func (r *mockGroupRedisRepo) SetFriendCache(_ context.Context, _ int64, _ int64) error { return nil }

// ──────────────────────────────────────────────────────
// 辅助：验证 mockMySQLRepo 满足 MySQLRepo 接口
// ──────────────────────────────────────────────────────

var _ repository.MySQLRepo = (*mockGroupMySQLRepo)(nil)
var _ repository.RedisRepo = (*mockGroupRedisRepo)(nil)

// ──────────────────────────────────────────────────────
// 辅助：新建测试 GroupService
// ──────────────────────────────────────────────────────

func newTestGroupService(mysqlRepo *mockGroupMySQLRepo, redisRepo *mockGroupRedisRepo) *GroupService {
	logger := zap.NewNop()
	return NewGroupService(mysqlRepo, redisRepo, logger)
}

// ──────────────────────────────────────────────────────
// CreateGroup 测试
// ──────────────────────────────────────────────────────

func TestGroup_CreateGroup_Success(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, err := svc.CreateGroup(context.Background(), 1, "TestGroup", "Welcome!")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), groupID)

	// 验证群组已存储
	mysqlRepo.mu.Lock()
	group := mysqlRepo.groupsByID[1]
	mysqlRepo.mu.Unlock()
	assert.NotNil(t, group)
	assert.Equal(t, "TestGroup", group.Name)
	assert.Equal(t, int64(1), group.OwnerID)
	assert.Equal(t, 500, group.MaxMembers)

	// 验证群主已添加为成员 (role=2)
	mysqlRepo.mu.Lock()
	members := mysqlRepo.membersByGroup[1]
	mysqlRepo.mu.Unlock()
	assert.Len(t, members, 1)
	assert.Equal(t, int64(1), members[0].UserID)
	assert.Equal(t, 2, members[0].Role)

	// 验证 Redis 缓存已更新
	redisRepo.mu.Lock()
	groupMembers := redisRepo.groupMembersSets[1]
	userGroups := redisRepo.userGroupsSets[1]
	redisRepo.mu.Unlock()
	assert.True(t, groupMembers["1"])
	assert.True(t, userGroups["1"])
}

// ──────────────────────────────────────────────────────
// UpdateGroup 测试
// ──────────────────────────────────────────────────────

func TestGroup_UpdateGroup_Success(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	// 先创建群组
	groupID, _ := svc.CreateGroup(context.Background(), 1, "OldName", "OldNotice")

	err := svc.UpdateGroup(context.Background(), 1, groupID, "NewName", "NewNotice")
	assert.NoError(t, err)

	mysqlRepo.mu.Lock()
	group := mysqlRepo.groupsByID[groupID]
	mysqlRepo.mu.Unlock()
	assert.Equal(t, "NewName", group.Name)
	assert.Equal(t, "NewNotice", group.Notice)
}

func TestGroup_UpdateGroup_NotOwner(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	// 创建由用户1拥有的群组
	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")

	// 用户2（普通成员）尝试更新
	err := svc.UpdateGroup(context.Background(), 2, groupID, "HackedName", "")
	assert.Error(t, err)
	assert.Equal(t, ErrNotOwnerOrAdmin, err.Error())
}

func TestGroup_UpdateGroup_GroupNotFound(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	err := svc.UpdateGroup(context.Background(), 1, 999, "Name", "")
	assert.Error(t, err)
	assert.Equal(t, ErrGroupNotFound, err.Error())
}

func TestGroup_UpdateGroup_AdminCanUpdate(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	// 创建由用户1拥有的群组
	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")

	// 将用户2添加为管理员
	mysqlRepo.mu.Lock()
	member := &model.GroupMember{GroupID: groupID, UserID: 2, Role: 1}
	key := fmt.Sprintf("%d:%d", groupID, 2)
	mysqlRepo.membersByKey[key] = member
	mysqlRepo.membersByGroup[groupID] = append(mysqlRepo.membersByGroup[groupID], *member)
	mysqlRepo.mu.Unlock()

	// 管理员（用户2）可以更新
	err := svc.UpdateGroup(context.Background(), 2, groupID, "AdminUpdate", "AdminNotice")
	assert.NoError(t, err)
}

// ──────────────────────────────────────────────────────
// AddMember 测试
// ──────────────────────────────────────────────────────

func TestGroup_AddMember_Success(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")

	err := svc.AddMember(context.Background(), groupID, 1, 2)
	assert.NoError(t, err)

	mysqlRepo.mu.Lock()
	members := mysqlRepo.membersByGroup[groupID]
	mysqlRepo.mu.Unlock()
	assert.Len(t, members, 2) // 群主 + 新成员
}

func TestGroup_AddMember_AlreadyMember(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")

	// 群主尝试再次添加自己
	err := svc.AddMember(context.Background(), groupID, 1, 1)
	assert.Error(t, err)
	assert.Equal(t, ErrAlreadyMember, err.Error())
}

func TestGroup_AddMember_GroupFull(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")

	// 将群组填满至500名成员
	mysqlRepo.mu.Lock()
	for i := int64(2); i <= 500; i++ {
		member := model.GroupMember{GroupID: groupID, UserID: i, Role: 0}
		key := fmt.Sprintf("%d:%d", groupID, i)
		member.ID = i
		member.JoinedAt = time.Now()
		mysqlRepo.membersByKey[key] = &member
		mysqlRepo.membersByGroup[groupID] = append(mysqlRepo.membersByGroup[groupID], member)
	}
	mysqlRepo.mu.Unlock()

	// 尝试添加第501名成员
	err := svc.AddMember(context.Background(), groupID, 1, 501)
	assert.Error(t, err)
	assert.Equal(t, ErrGroupFull, err.Error())
}

func TestGroup_AddMember_NotOwnerOrAdmin(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")

	// 普通成员尝试添加他人
	err := svc.AddMember(context.Background(), groupID, 3, 4)
	assert.Error(t, err)
	assert.Equal(t, ErrNotOwnerOrAdmin, err.Error())
}

// ──────────────────────────────────────────────────────
// RemoveMember 测试
// ──────────────────────────────────────────────────────

func TestGroup_RemoveMember_Success(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")

	// 先添加一名成员
	svc.AddMember(context.Background(), groupID, 1, 2)

	err := svc.RemoveMember(context.Background(), groupID, 1, 2)
	assert.NoError(t, err)

	mysqlRepo.mu.Lock()
	members := mysqlRepo.membersByGroup[groupID]
	mysqlRepo.mu.Unlock()
	assert.Len(t, members, 1) // 只剩群主
}

func TestGroup_RemoveMember_CannotRemoveOwner(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")

	// 尝试移除群主
	err := svc.RemoveMember(context.Background(), groupID, 1, 1)
	assert.Error(t, err)
	assert.Equal(t, ErrCannotRemoveOwner, err.Error())
}

func TestGroup_RemoveMember_SelfLeave(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")
	svc.AddMember(context.Background(), groupID, 1, 2)

	// 用户2自行退出（主动离群）
	err := svc.RemoveMember(context.Background(), groupID, 2, 2)
	assert.NoError(t, err)

	mysqlRepo.mu.Lock()
	members := mysqlRepo.membersByGroup[groupID]
	mysqlRepo.mu.Unlock()
	assert.Len(t, members, 1)
}

func TestGroup_RemoveMember_AdminCannotRemoveAdmin(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)
	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")
	for _, userID := range []int64{2, 3} {
		member := &model.GroupMember{GroupID: groupID, UserID: userID, Role: 1}
		_ = mysqlRepo.AddGroupMember(context.Background(), member)
	}
	err := svc.RemoveMember(context.Background(), groupID, 2, 3)
	assert.EqualError(t, err, ErrCannotRemovePeer)
}

func TestGroup_RemoveMember_MissingMember(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)
	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")
	err := svc.RemoveMember(context.Background(), groupID, 1, 99)
	assert.EqualError(t, err, ErrMemberNotFound)
}

// ──────────────────────────────────────────────────────
// LeaveGroup 测试
// ──────────────────────────────────────────────────────

func TestGroup_LeaveGroup_Success(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")
	svc.AddMember(context.Background(), groupID, 1, 2)

	err := svc.LeaveGroup(context.Background(), groupID, 2)
	assert.NoError(t, err)

	mysqlRepo.mu.Lock()
	members := mysqlRepo.membersByGroup[groupID]
	mysqlRepo.mu.Unlock()
	assert.Len(t, members, 1)
}

func TestGroup_LeaveGroup_OwnerCannotLeave(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")

	err := svc.LeaveGroup(context.Background(), groupID, 1)
	assert.Error(t, err)
	assert.Equal(t, ErrCannotLeaveAsOwner, err.Error())
}

func TestGroup_LeaveGroup_MissingMember(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)
	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")
	err := svc.LeaveGroup(context.Background(), groupID, 2)
	assert.EqualError(t, err, ErrMemberNotFound)
}

// ──────────────────────────────────────────────────────
// UpdateMemberRole 测试
// ──────────────────────────────────────────────────────

func TestGroup_UpdateMemberRole_Success(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")
	svc.AddMember(context.Background(), groupID, 1, 2)

	err := svc.UpdateMemberRole(context.Background(), groupID, 1, 2, 1) // 提升为管理员
	assert.NoError(t, err)

	mysqlRepo.mu.Lock()
	key := fmt.Sprintf("%d:%d", groupID, 2)
	member := mysqlRepo.membersByKey[key]
	mysqlRepo.mu.Unlock()
	assert.Equal(t, 1, member.Role)
}

func TestGroup_UpdateMemberRole_NotOwner(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")
	svc.AddMember(context.Background(), groupID, 1, 2)

	// 管理员（用户2）尝试更改角色 — 仅群主有权操作
	mysqlRepo.mu.Lock()
	key := fmt.Sprintf("%d:%d", groupID, 2)
	mysqlRepo.membersByKey[key].Role = 1
	for i := range mysqlRepo.membersByGroup[groupID] {
		if mysqlRepo.membersByGroup[groupID][i].UserID == 2 {
			mysqlRepo.membersByGroup[groupID][i].Role = 1
		}
	}
	mysqlRepo.mu.Unlock()

	err := svc.UpdateMemberRole(context.Background(), groupID, 2, 3, 1)
	assert.Error(t, err)
	assert.Equal(t, ErrNotOwnerOrAdmin, err.Error())
}

func TestGroup_UpdateMemberRole_CannotChangeOwnerRole(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")

	err := svc.UpdateMemberRole(context.Background(), groupID, 1, 1, 0)
	assert.Error(t, err)
	assert.Equal(t, ErrCannotRemoveOwner, err.Error())
}

func TestGroup_UpdateMemberRole_InvalidRole(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")
	svc.AddMember(context.Background(), groupID, 1, 2)

	err := svc.UpdateMemberRole(context.Background(), groupID, 1, 2, 2) // role=2（群主）不允许
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidRole, err.Error())
}

func TestGroup_UpdateMemberRole_InvalidRoleNegative(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")
	svc.AddMember(context.Background(), groupID, 1, 2)

	err := svc.UpdateMemberRole(context.Background(), groupID, 1, 2, -1)
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidRole, err.Error())
}

// ──────────────────────────────────────────────────────
// GetGroupInfo 测试
// ──────────────────────────────────────────────────────

func TestGroup_GetGroupInfo_Success(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "MyGroup", "MyNotice")

	group, err := svc.GetGroupInfo(context.Background(), groupID)
	assert.NoError(t, err)
	assert.Equal(t, "MyGroup", group.Name)
	assert.Equal(t, int64(1), group.OwnerID)
}

func TestGroup_GetGroupInfo_NotFound(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	group, err := svc.GetGroupInfo(context.Background(), 999)
	assert.Error(t, err)
	assert.Equal(t, ErrGroupNotFound, err.Error())
	assert.Nil(t, group)
}

// ──────────────────────────────────────────────────────
// GetMembers 测试
// ──────────────────────────────────────────────────────

func TestGroup_GetMembers_Success(t *testing.T) {
	mysqlRepo := newMockGroupMySQLRepo()
	redisRepo := newMockGroupRedisRepo()
	svc := newTestGroupService(mysqlRepo, redisRepo)

	groupID, _ := svc.CreateGroup(context.Background(), 1, "Group", "")
	svc.AddMember(context.Background(), groupID, 1, 2)
	svc.AddMember(context.Background(), groupID, 1, 3)

	members, err := svc.GetMembers(context.Background(), groupID)
	assert.NoError(t, err)
	assert.Len(t, members, 3) // 群主 + 2名成员
}

// ── 高并发点赞新增接口的 mock 桩 ──

func (m *mockGroupMySQLRepo) GetMomentLikers(_ context.Context, _ int64) ([]int64, error) {
	return nil, nil
}
func (m *mockGroupMySQLRepo) BatchUpsertMomentLikes(_ context.Context, _ []model.MomentLike) error {
	return nil
}
func (m *mockGroupMySQLRepo) BatchDeleteMomentLikes(_ context.Context, _ []model.MomentLikeKey) error {
	return nil
}

func (r *mockGroupRedisRepo) LikeMomentAtomic(_ context.Context, _ int64, _ int64) (bool, int64, error) {
	return false, 0, nil
}
func (r *mockGroupRedisRepo) UnlikeMomentAtomic(_ context.Context, _ int64, _ int64) (bool, int64, error) {
	return false, 0, nil
}
func (r *mockGroupRedisRepo) EnsureMomentLikesLoaded(_ context.Context, _ int64, _ func(context.Context) ([]int64, error), _ time.Duration) error {
	return nil
}
func (r *mockGroupRedisRepo) GetMomentLikeStats(_ context.Context, _ int64, _ []int64) (map[int64]int64, map[int64]bool, error) {
	return nil, nil, nil
}
func (r *mockGroupRedisRepo) GetMomentLikerIDs(_ context.Context, _ int64) ([]int64, error) {
	return nil, nil
}
func (r *mockGroupRedisRepo) DeleteMomentLikes(_ context.Context, _ int64) error { return nil }
