package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/redis"
)

// ── 模拟实现 ──

// mockMySQLRepo 为动态测试实现 repository.MySQLRepo。
// 仅实现动态相关方法；其他方法会触发 panic。
type mockMySQLRepo struct {
	moments     map[int64]*model.Moment
	likes       map[string]*model.MomentLike // 键："momentID:userID"
	comments    map[int64]*model.MomentComment
	friends     map[int64][]int64 // userID -> friendID 列表
	users       map[int64]*model.User
	friendCount map[int64]int // userID -> 好友数
	nextID      int64
	commentErr  error // CreateMomentComment 的可注入错误
}

func newMockMySQLRepo() *mockMySQLRepo {
	return &mockMySQLRepo{
		moments:     make(map[int64]*model.Moment),
		likes:       make(map[string]*model.MomentLike),
		comments:    make(map[int64]*model.MomentComment),
		friends:     make(map[int64][]int64),
		users:       make(map[int64]*model.User),
		friendCount: make(map[int64]int),
		nextID:      1,
	}
}

func (m *mockMySQLRepo) CreateMoment(ctx context.Context, moment *model.Moment) error {
	moment.ID = m.nextID
	m.nextID++
	m.moments[moment.ID] = moment
	return nil
}

func (m *mockMySQLRepo) GetMomentByID(ctx context.Context, id int64) (*model.Moment, error) {
	moment, ok := m.moments[id]
	if !ok {
		return nil, nil
	}
	return moment, nil
}

func (m *mockMySQLRepo) DeleteMoment(_ context.Context, id int64) error {
	delete(m.moments, id)
	for commentID, comment := range m.comments {
		if comment.MomentID == id {
			delete(m.comments, commentID)
		}
	}
	for key, like := range m.likes {
		if like.MomentID == id {
			delete(m.likes, key)
		}
	}
	return nil
}

func (m *mockMySQLRepo) GetMomentsByUser(ctx context.Context, userID int64, limit, offset int) ([]model.Moment, error) {
	var result []model.Moment
	for _, moment := range m.moments {
		if moment.AuthorID == userID {
			result = append(result, *moment)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].CreatedAt.After(result[j].CreatedAt)
		}
		return result[i].ID > result[j].ID
	})
	if offset >= len(result) {
		return []model.Moment{}, nil
	}
	end := offset + limit
	if end > len(result) {
		end = len(result)
	}
	return result[offset:end], nil
}

func (m *mockMySQLRepo) GetMomentsByIDs(ctx context.Context, ids []int64) ([]model.Moment, error) {
	result := make([]model.Moment, 0, len(ids))
	for _, id := range ids {
		if moment, ok := m.moments[id]; ok {
			result = append(result, *moment)
		}
	}
	return result, nil
}

func (m *mockMySQLRepo) CountFriends(_ context.Context, userID int64) (int, error) {
	return m.friendCount[userID], nil
}

// GetMomentLikers 返回该动态的全部点赞用户ID（用于 warm-up 回源）。
func (m *mockMySQLRepo) GetMomentLikers(_ context.Context, momentID int64) ([]int64, error) {
	prefix := fmt.Sprintf("%d:", momentID)
	var ids []int64
	for k, _ := range m.likes {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			ids = append(ids, m.likes[k].UserID)
		}
	}
	return ids, nil
}

func (m *mockMySQLRepo) BatchUpsertMomentLikes(_ context.Context, likes []model.MomentLike) error {
	for _, l := range likes {
		key := fmt.Sprintf("%d:%d", l.MomentID, l.UserID)
		if _, exists := m.likes[key]; !exists {
			m.likes[key] = &model.MomentLike{MomentID: l.MomentID, UserID: l.UserID, CreatedAt: l.CreatedAt}
		}
	}
	return nil
}

func (m *mockMySQLRepo) BatchDeleteMomentLikes(_ context.Context, keys []model.MomentLikeKey) error {
	for _, k := range keys {
		delete(m.likes, fmt.Sprintf("%d:%d", k.MomentID, k.UserID))
	}
	return nil
}

func (m *mockMySQLRepo) CreateMomentComment(ctx context.Context, comment *model.MomentComment) error {
	if m.commentErr != nil {
		return m.commentErr
	}
	if comment.ID == 0 {
		comment.ID = m.nextID
		m.nextID++
	}
	m.comments[comment.ID] = comment
	return nil
}

func (m *mockMySQLRepo) GetMomentCommentByID(ctx context.Context, id int64) (*model.MomentComment, error) {
	comment, ok := m.comments[id]
	if !ok {
		return nil, nil
	}
	return comment, nil
}

func (m *mockMySQLRepo) GetMomentComments(_ context.Context, momentID int64) ([]model.MomentComment, error) {
	comments := make([]model.MomentComment, 0)
	for _, comment := range m.comments {
		if comment.MomentID == momentID {
			copy := *comment
			if user := m.users[copy.UserID]; user != nil {
				copy.Username = user.Username
				copy.AvatarURL = user.AvatarURL
			}
			comments = append(comments, copy)
		}
	}
	sort.Slice(comments, func(i, j int) bool { return comments[i].ID < comments[j].ID })
	return comments, nil
}

func (m *mockMySQLRepo) DeleteMomentComment(ctx context.Context, id int64) error {
	delete(m.comments, id)
	return nil
}

// 用 panic 桩实现所有其他 MySQLRepo 方法

func (m *mockMySQLRepo) InsertPrivateMessage(_ context.Context, _ *model.PrivateMessage) error {
	panic("未实现")
}
func (m *mockMySQLRepo) InsertGroupMessage(_ context.Context, _ *model.GroupMessage) error {
	panic("未实现")
}
func (m *mockMySQLRepo) InsertMsgRevoked(_ context.Context, _ *model.MsgRevoked) error {
	panic("未实现")
}
func (m *mockMySQLRepo) GetUserByID(_ context.Context, userID int64) (*model.User, error) {
	if user, ok := m.users[userID]; ok {
		return user, nil
	}
	return &model.User{ID: userID, Username: fmt.Sprintf("user_%d", userID)}, nil
}
func (m *mockMySQLRepo) GetUserByUsername(_ context.Context, _ string) (*model.User, error) {
	panic("未实现")
}
func (m *mockMySQLRepo) CreateUser(_ context.Context, _ *model.User) error {
	panic("未实现")
}
func (m *mockMySQLRepo) UpdateUser(_ context.Context, _ *model.User) error {
	panic("未实现")
}
func (m *mockMySQLRepo) CreateFriendRequest(_ context.Context, _ *model.FriendRequest) error {
	panic("未实现")
}
func (m *mockMySQLRepo) UpdateFriendRequest(_ context.Context, _ *model.FriendRequest) error {
	panic("未实现")
}
func (m *mockMySQLRepo) GetFriendRequestByID(_ context.Context, _ int64) (*model.FriendRequest, error) {
	panic("未实现")
}
func (m *mockMySQLRepo) GetFriendRequestsByUser(_ context.Context, _ int64) ([]model.FriendRequest, error) {
	panic("未实现")
}
func (m *mockMySQLRepo) CreateFriendship(_ context.Context, _ *model.Friendship) error {
	panic("未实现")
}
func (m *mockMySQLRepo) DeleteFriendship(_ context.Context, _ int64, _ int64) error {
	panic("未实现")
}
func (m *mockMySQLRepo) GetFriendList(_ context.Context, userID int64) ([]model.Friendship, error) {
	fids := m.friends[userID]
	out := make([]model.Friendship, 0, len(fids))
	for _, fid := range fids {
		out = append(out, model.Friendship{UserID: userID, FriendID: fid})
	}
	return out, nil
}
func (m *mockMySQLRepo) IsFriend(_ context.Context, userID, friendID int64) (bool, error) {
	for _, id := range m.friends[userID] {
		if id == friendID {
			return true, nil
		}
	}
	for _, id := range m.friends[friendID] {
		if id == userID {
			return true, nil
		}
	}
	return false, nil
}
func (m *mockMySQLRepo) CreateBlacklist(_ context.Context, _ *model.Blacklist) error {
	panic("未实现")
}
func (m *mockMySQLRepo) DeleteBlacklist(_ context.Context, _ int64, _ int64) error {
	panic("未实现")
}
func (m *mockMySQLRepo) IsBlocked(_ context.Context, _ int64, _ int64) (bool, error) {
	panic("未实现")
}
func (m *mockMySQLRepo) CreateGroup(_ context.Context, _ *model.Group) (int64, error) {
	panic("未实现")
}
func (m *mockMySQLRepo) UpdateGroup(_ context.Context, _ *model.Group) error {
	panic("未实现")
}
func (m *mockMySQLRepo) GetGroupByID(_ context.Context, _ int64) (*model.Group, error) {
	panic("未实现")
}
func (m *mockMySQLRepo) AddGroupMember(_ context.Context, _ *model.GroupMember) error {
	panic("未实现")
}
func (m *mockMySQLRepo) RemoveGroupMember(_ context.Context, _ int64, _ int64) error {
	panic("未实现")
}
func (m *mockMySQLRepo) GetGroupMembers(_ context.Context, _ int64) ([]model.GroupMember, error) {
	panic("未实现")
}
func (m *mockMySQLRepo) UpdateGroupMemberRole(_ context.Context, _ int, _ int, _ int) error {
	panic("未实现")
}
func (m *mockMySQLRepo) GetUserSettings(_ context.Context, _ int64) (*model.UserSettings, error) {
	panic("未实现")
}
func (m *mockMySQLRepo) CreateOrUpdateUserSettings(_ context.Context, _ *model.UserSettings) error {
	panic("未实现")
}
func (m *mockMySQLRepo) SearchPrivateMessages(_ context.Context, _ int64, _ string, _ int, _ int) ([]model.PrivateMessage, error) {
	panic("未实现")
}

// momentMockRedisRepo 为动态测试实现 repository.RedisRepo。
// 仅实现动态信息流方法；其他方法会触发 panic。
type momentMockRedisRepo struct {
	timelines  map[int64][]model.FeedEntry // userID -> 收件箱条目
	outboxes   map[int64][]model.FeedEntry // userID -> 寄件箱条目
	bigUsers   map[int64]bool
	likeLoaded map[int64]bool           // momentID -> 是否已预热
	likeSets   map[int64]map[int64]bool // momentID -> set of userIDs
	likeCounts map[int64]int64          // momentID -> count
}

func newMomentMockRedisRepo() *momentMockRedisRepo {
	return &momentMockRedisRepo{
		timelines:  make(map[int64][]model.FeedEntry),
		outboxes:   make(map[int64][]model.FeedEntry),
		bigUsers:   make(map[int64]bool),
		likeLoaded: make(map[int64]bool),
		likeSets:   make(map[int64]map[int64]bool),
		likeCounts: make(map[int64]int64),
	}
}

func (m *momentMockRedisRepo) PublishMomentFeed(ctx context.Context, userID int64, momentID int64, timestamp int64) error {
	m.timelines[userID] = append(m.timelines[userID], model.FeedEntry{MomentID: momentID, Ts: timestamp})
	return nil
}

func (m *momentMockRedisRepo) GetMomentFeed(ctx context.Context, userID int64, lastSyncTime int64, limit int) ([]int64, error) {
	entries := m.timelines[userID]
	ids := make([]int64, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, e.MomentID)
	}
	if limit < len(ids) {
		ids = ids[:limit]
	}
	return ids, nil
}

func (m *momentMockRedisRepo) AddToOutbox(_ context.Context, authorID int64, momentID int64, timestamp int64, _ int) error {
	m.outboxes[authorID] = append(m.outboxes[authorID], model.FeedEntry{MomentID: momentID, Ts: timestamp})
	return nil
}

func (m *momentMockRedisRepo) FanoutMomentFeed(_ context.Context, friendIDs []int64, momentID int64, timestamp int64, _ int) error {
	for _, fid := range friendIDs {
		m.timelines[fid] = append(m.timelines[fid], model.FeedEntry{MomentID: momentID, Ts: timestamp})
	}
	return nil
}

func (m *momentMockRedisRepo) MarkBigUser(_ context.Context, userID int64) error {
	m.bigUsers[userID] = true
	return nil
}

func (m *momentMockRedisRepo) FilterBigUsers(_ context.Context, userIDs []int64) ([]int64, error) {
	var out []int64
	for _, id := range userIDs {
		if m.bigUsers[id] {
			out = append(out, id)
		}
	}
	return out, nil
}

func (m *momentMockRedisRepo) GetTimelinePage(_ context.Context, userID int64, maxTs int64, maxID int64, limit int) ([]model.FeedEntry, error) {
	return pageOf(m.timelines[userID], maxTs, maxID, limit), nil
}

func (m *momentMockRedisRepo) GetOutboxPage(_ context.Context, userID int64, maxTs int64, maxID int64, limit int) ([]model.FeedEntry, error) {
	return pageOf(m.outboxes[userID], maxTs, maxID, limit), nil
}

// ── 高并发点赞方法（有状态 mock）──

func (m *momentMockRedisRepo) LikeMomentAtomic(_ context.Context, momentID, userID int64) (bool, int64, error) {
	set := m.likeSets[momentID]
	if set == nil {
		set = make(map[int64]bool)
		m.likeSets[momentID] = set
	}
	if set[userID] {
		return false, m.likeCounts[momentID], nil // 已赞，幂等
	}
	set[userID] = true
	m.likeCounts[momentID]++
	return true, m.likeCounts[momentID], nil
}

func (m *momentMockRedisRepo) UnlikeMomentAtomic(_ context.Context, momentID, userID int64) (bool, int64, error) {
	set := m.likeSets[momentID]
	if set == nil || !set[userID] {
		return false, m.likeCounts[momentID], nil // 未赞，幂等
	}
	delete(set, userID)
	c := m.likeCounts[momentID] - 1
	if c < 0 {
		c = 0
	}
	m.likeCounts[momentID] = c
	return true, c, nil
}

func (m *momentMockRedisRepo) EnsureMomentLikesLoaded(_ context.Context, momentID int64, loader func(context.Context) ([]int64, error), _ time.Duration) error {
	if m.likeLoaded[momentID] {
		return nil
	}
	m.likeLoaded[momentID] = true
	if m.likeSets[momentID] == nil {
		m.likeSets[momentID] = make(map[int64]bool)
	}
	// 如果 loader 能拉取到数据，载入（模拟 warm-up）
	if loader != nil {
		ids, err := loader(context.Background())
		if err != nil {
			return err
		}
		for _, uid := range ids {
			m.likeSets[momentID][uid] = true
		}
		m.likeCounts[momentID] = int64(len(ids))
	}
	return nil
}

func (m *momentMockRedisRepo) GetMomentLikeStats(_ context.Context, viewerID int64, momentIDs []int64) (map[int64]int64, map[int64]bool, error) {
	counts := make(map[int64]int64, len(momentIDs))
	liked := make(map[int64]bool, len(momentIDs))
	for _, mid := range momentIDs {
		counts[mid] = m.likeCounts[mid]
		if set := m.likeSets[mid]; set != nil {
			liked[mid] = set[viewerID]
		}
	}
	return counts, liked, nil
}

func (m *momentMockRedisRepo) GetMomentLikerIDs(_ context.Context, momentID int64) ([]int64, error) {
	set := m.likeSets[momentID]
	ids := make([]int64, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	return ids, nil
}

func (m *momentMockRedisRepo) DeleteMomentLikes(_ context.Context, momentID int64) error {
	delete(m.likeSets, momentID)
	delete(m.likeCounts, momentID)
	delete(m.likeLoaded, momentID)
	return nil
}

// pageOf 模拟 getFeedPage 的契约：按 (ts,id) 降序，返回"严格早于游标 (maxTs,maxID)"的前 limit 条。
// maxTs<0 表示首页（不限游标）。
func pageOf(entries []model.FeedEntry, maxTs int64, maxID int64, limit int) []model.FeedEntry {
	filtered := make([]model.FeedEntry, 0, len(entries))
	for _, e := range entries {
		if maxTs < 0 || e.Ts < maxTs || (e.Ts == maxTs && e.MomentID < maxID) {
			filtered = append(filtered, e)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Ts != filtered[j].Ts {
			return filtered[i].Ts > filtered[j].Ts
		}
		return filtered[i].MomentID > filtered[j].MomentID
	})
	if limit < len(filtered) {
		filtered = filtered[:limit]
	}
	return filtered
}

// 用 panic 桩实现所有其他 RedisRepo 方法

func (m *momentMockRedisRepo) WriteInbox(_ context.Context, _ int64, _ *model.InboxMessage) error {
	panic("未实现")
}
func (m *momentMockRedisRepo) WriteOutbox(_ context.Context, _ int64, _ *model.InboxMessage) error {
	panic("未实现")
}
func (m *momentMockRedisRepo) ReadInbox(_ context.Context, _ int64, _, _ int64, _ int) ([]model.InboxMessage, error) {
	panic("未实现")
}
func (m *momentMockRedisRepo) ReadOutbox(_ context.Context, _ int64, _, _ int64, _ int) ([]model.InboxMessage, error) {
	panic("未实现")
}
func (m *momentMockRedisRepo) UpdateConvList(_ context.Context, _ int64, _ string, _ string, _ int64) error {
	panic("未实现")
}
func (m *momentMockRedisRepo) GetConvList(_ context.Context, _ int64) ([]model.ConvSummary, error) {
	panic("未实现")
}
func (m *momentMockRedisRepo) IncrementUnread(_ context.Context, _ int64, _ string) error {
	panic("未实现")
}
func (m *momentMockRedisRepo) ClearUnread(_ context.Context, _ int64, _ string) error {
	panic("未实现")
}
func (m *momentMockRedisRepo) GetUnreadMap(_ context.Context, _ int64) (map[string]int64, error) {
	panic("未实现")
}
func (m *momentMockRedisRepo) SetGroupReadPos(_ context.Context, _ int64, _ string, _ int64) error {
	panic("未实现")
}
func (m *momentMockRedisRepo) GetGroupReadPos(_ context.Context, _ int64, _ string) (int64, error) {
	panic("未实现")
}

func (m *momentMockRedisRepo) GetGroupSeq(_ context.Context, _ int64) (int64, error) { return 0, nil }
func (m *momentMockRedisRepo) GetGroupMemberships(_ context.Context, _ int64) ([]int64, error) {
	panic("未实现")
}
func (m *momentMockRedisRepo) GetGroupMembers(_ context.Context, _ int64) ([]int64, error) {
	panic("未实现")
}
func (m *momentMockRedisRepo) CheckDuplicate(_ context.Context, _ int64, _ string) (bool, error) {
	panic("未实现")
}
func (m *momentMockRedisRepo) TrimInbox(_ context.Context, _ int64, _ int) error {
	panic("未实现")
}
func (m *momentMockRedisRepo) TrimOutbox(_ context.Context, _ int64, _ int) error {
	panic("未实现")
}
func (m *momentMockRedisRepo) TrimInboxByTime(_ context.Context, _ int64, _ int64) error {
	panic("未实现")
}
func (m *momentMockRedisRepo) TrimOutboxByTime(_ context.Context, _ int64, _ int64) error {
	panic("未实现")
}
func (m *momentMockRedisRepo) TrimConvListByTime(_ context.Context, _ int64, _ int64) error {
	panic("未实现")
}
func (m *momentMockRedisRepo) TrimTimelineByTime(_ context.Context, _ int64, _ int64) error {
	panic("未实现")
}
func (m *momentMockRedisRepo) ExecPrivateMsgCheck(_ context.Context, _ int64, _ int64, _ string) (*redis.PrivateMsgCheckResult, error) {
	panic("未实现")
}
func (m *momentMockRedisRepo) ExecGroupMsgCheck(_ context.Context, _ int64, _ int64, _ string) (*redis.GroupMsgCheckResult, error) {
	panic("未实现")
}
func (m *momentMockRedisRepo) ExecInboxMarkRead(_ context.Context, _ int64, _ string) (int64, error) {
	panic("未实现")
}
func (m *momentMockRedisRepo) ExecRevokeMsg(_ context.Context, _ int64, _ string, _ int64, _ string, _ int64) (bool, error) {
	panic("未实现")
}
func (m *momentMockRedisRepo) AddGroupMemberRedis(_ context.Context, _ int64, _ int64) error {
	return nil
}
func (m *momentMockRedisRepo) RemoveGroupMemberRedis(_ context.Context, _ int64, _ int64) error {
	return nil
}
func (m *momentMockRedisRepo) SetFriendCache(_ context.Context, _ int64, _ int64) error { return nil }

// momentMockMQRepo 为动态测试实现 repository.MQRepo。
type momentMockMQRepo struct {
	publishedMoments []*model.Moment
	likeEvents       []*model.LikeEvent
}

func newMomentMockMQRepo() *momentMockMQRepo {
	return &momentMockMQRepo{
		publishedMoments: make([]*model.Moment, 0),
		likeEvents:       make([]*model.LikeEvent, 0),
	}
}

func (m *momentMockMQRepo) PublishPrivateMsg(_ context.Context, _ *model.PrivateMessage) error {
	panic("未实现")
}
func (m *momentMockMQRepo) PublishGroupMsg(_ context.Context, _ *model.GroupMessage) error {
	panic("未实现")
}
func (m *momentMockMQRepo) PublishMomentPush(_ context.Context, moment *model.Moment) error {
	m.publishedMoments = append(m.publishedMoments, moment)
	return nil
}

func (m *momentMockMQRepo) PublishLikeEvent(_ context.Context, evt *model.LikeEvent) error {
	m.likeEvents = append(m.likeEvents, evt)
	return nil
}

// ── 测试 ──

func TestPublishMoment_Success(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	momentID, err := svc.PublishMoment(context.Background(), 100, "Hello world!", nil, 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), momentID)

	// 验证动态已存储到 MySQL
	moment, ok := mysqlRepo.moments[momentID]
	assert.True(t, ok)
	assert.Equal(t, int64(100), moment.AuthorID)
	assert.Equal(t, "Hello world!", moment.Content)
	assert.Equal(t, 2, moment.Visibility)

	// 验证动态已发布到 MQ
	assert.Len(t, mqRepo.publishedMoments, 1)
	assert.Equal(t, momentID, mqRepo.publishedMoments[0].ID)
}

func TestPublishMoment_EmptyContent(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	_, err := svc.PublishMoment(context.Background(), 100, "", nil, 2)
	assert.Error(t, err)
	assert.Equal(t, ErrMomentContentEmpty, err.Error())
}

func TestPublishMoment_WhitespaceAndOverlongContent(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	svc := NewMomentService(mysqlRepo, newMomentMockRedisRepo(), newMomentMockMQRepo(), zap.NewNop(), time.Hour)

	_, err := svc.PublishMoment(context.Background(), 100, " \n\t ", nil, 2)
	assert.EqualError(t, err, ErrMomentContentEmpty)

	_, err = svc.PublishMoment(context.Background(), 100, strings.Repeat("你", maxMomentContentRunes+1), nil, 2)
	assert.EqualError(t, err, ErrMomentContentTooLong)
	assert.Empty(t, mysqlRepo.moments, "rejected content must not be persisted")
}

func TestPublishMoment_InvalidVisibility(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	_, err := svc.PublishMoment(context.Background(), 100, "Hello", nil, 4)
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidVisibility, err.Error())
}

func TestGetMoment_Success(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	// 预填充一条动态
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{
		AuthorID:   100,
		Content:    "Test moment",
		Visibility: 1,
		CreatedAt:  time.Now(),
	})

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	moment, err := svc.GetMoment(context.Background(), 100, 1)
	assert.NoError(t, err)
	assert.NotNil(t, moment)
	assert.Equal(t, int64(100), moment.AuthorID)
	assert.Equal(t, "Test moment", moment.Content)
	// LikeCount/LikedByMe 默认 0/false（无点赞）
	assert.Equal(t, int64(0), moment.LikeCount)
	assert.False(t, moment.LikedByMe)
}

func TestGetMoment_NotFound(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	moment, err := svc.GetMoment(context.Background(), 100, 999)
	assert.Error(t, err)
	assert.Equal(t, ErrMomentNotFound, err.Error())
	assert.Nil(t, moment)
}

func TestGetMoment_Visibility(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()
	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	mysqlRepo.CreateMoment(context.Background(), &model.Moment{AuthorID: 100, Content: "public", Visibility: 1, CreatedAt: time.Now()})
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{AuthorID: 100, Content: "friends", Visibility: 2, CreatedAt: time.Now()})
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{AuthorID: 100, Content: "private", Visibility: 3, CreatedAt: time.Now()})
	mysqlRepo.friends[200] = []int64{100}

	for _, id := range []int64{1, 2, 3} {
		moment, err := svc.GetMoment(context.Background(), 100, id)
		assert.NoError(t, err, "作者应能查看动态 %d", id)
		assert.NotNil(t, moment)
	}
	for _, id := range []int64{1, 2} {
		moment, err := svc.GetMoment(context.Background(), 200, id)
		assert.NoError(t, err, "好友应能查看动态 %d", id)
		assert.NotNil(t, moment)
	}
	_, err := svc.GetMoment(context.Background(), 200, 3)
	assert.EqualError(t, err, ErrMomentNotFound)

	for _, id := range []int64{1, 2, 3} {
		_, err = svc.GetMoment(context.Background(), 300, id)
		assert.EqualError(t, err, ErrMomentNotFound)
	}
}

func TestGetUserMoments_FiltersBeforePagination(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()
	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	// 非好友看不到任何动态；好友可见动态参与分页，私密动态不能占用名额。
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{AuthorID: 100, Content: "public-1", Visibility: 1, CreatedAt: time.Now()})
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{AuthorID: 100, Content: "friends", Visibility: 2, CreatedAt: time.Now()})
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{AuthorID: 100, Content: "private", Visibility: 3, CreatedAt: time.Now()})
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{AuthorID: 100, Content: "public-2", Visibility: 1, CreatedAt: time.Now()})
	mysqlRepo.friends[200] = []int64{100}

	page1, err := svc.GetUserMoments(context.Background(), 300, 100, 1, 0)
	assert.NoError(t, err)
	assert.Empty(t, page1)

	friendPage, err := svc.GetUserMoments(context.Background(), 200, 100, 10, 0)
	assert.NoError(t, err)
	assert.Len(t, friendPage, 3)
	for _, moment := range friendPage {
		assert.NotEqual(t, 3, moment.Visibility)
	}
}

func TestLikeMoment_Success(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	// 预填充一条动态
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{
		AuthorID:   100,
		Content:    "Test moment",
		Visibility: 1,
		CreatedAt:  time.Now(),
	})
	mysqlRepo.friends[200] = []int64{100}

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	count, err := svc.LikeMoment(context.Background(), 200, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// 验证 MQ 事件已发布
	assert.Len(t, mqRepo.likeEvents, 1)
	assert.Equal(t, model.LikeActionLike, mqRepo.likeEvents[0].Action)
	assert.Equal(t, int64(200), mqRepo.likeEvents[0].UserID)
	assert.Equal(t, int64(1), mqRepo.likeEvents[0].MomentID)
}

func TestLikeMoment_MomentNotFound(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	_, err := svc.LikeMoment(context.Background(), 200, 999)
	assert.Error(t, err)
	assert.Equal(t, ErrMomentNotFound, err.Error())
}

func TestLikeMoment_Idempotent(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	// 预填充一条动态
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{
		AuthorID:   100,
		Content:    "Test moment",
		Visibility: 1,
		CreatedAt:  time.Now(),
	})
	mysqlRepo.friends[200] = []int64{100}

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	// 第一次点赞成功，count=1
	count1, err := svc.LikeMoment(context.Background(), 200, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count1)

	// 第二次点赞幂等成功，count 不变，不报错
	count2, err := svc.LikeMoment(context.Background(), 200, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count2)

	// 只有第一次产生持久化事件
	assert.Len(t, mqRepo.likeEvents, 1)
}

func TestUnlikeMoment_Success(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	// 预填充一条动态及其点赞明细（MySQL 侧已有历史赞）
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{
		AuthorID:   100,
		Content:    "Test moment",
		Visibility: 1,
		CreatedAt:  time.Now(),
	})
	mysqlRepo.BatchUpsertMomentLikes(context.Background(), []model.MomentLike{
		{MomentID: 1, UserID: 200, CreatedAt: time.Now()},
	})

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	count, err := svc.UnlikeMoment(context.Background(), 200, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count) // 1→0

	// 验证 MQ 事件已发布
	assert.Len(t, mqRepo.likeEvents, 1)
	assert.Equal(t, model.LikeActionUnlike, mqRepo.likeEvents[0].Action)
}

func TestGetMomentLikers_ReturnsCurrentRedisLikes(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	svc := NewMomentService(mysqlRepo, redisRepo, newMomentMockMQRepo(), zap.NewNop(), time.Hour)
	moment := &model.Moment{AuthorID: 100, Content: "hello", Visibility: 2, CreatedAt: time.Now()}
	assert.NoError(t, mysqlRepo.CreateMoment(context.Background(), moment))
	mysqlRepo.friends[100] = []int64{200}
	mysqlRepo.users[200] = &model.User{ID: 200, Username: "liker", AvatarURL: "/uploads/liker.png"}
	_, err := svc.LikeMoment(context.Background(), 200, moment.ID)
	assert.NoError(t, err)

	likers, err := svc.GetMomentLikers(context.Background(), 100, moment.ID)
	assert.NoError(t, err)
	assert.Equal(t, []model.MomentLiker{{UserID: 200, Username: "liker", AvatarURL: "/uploads/liker.png"}}, likers)
}

func TestDeleteMoment_OnlyAllowsAuthor(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	svc := NewMomentService(mysqlRepo, redisRepo, newMomentMockMQRepo(), zap.NewNop(), time.Hour)
	moment := &model.Moment{AuthorID: 100, Content: "delete me", Visibility: 2, CreatedAt: time.Now()}
	assert.NoError(t, mysqlRepo.CreateMoment(context.Background(), moment))

	err := svc.DeleteMoment(context.Background(), 200, moment.ID)
	assert.Equal(t, ErrNotMomentOwner, err.Error())
	assert.NotNil(t, mysqlRepo.moments[moment.ID])
	err = svc.DeleteMoment(context.Background(), 100, moment.ID)
	assert.NoError(t, err)
	assert.Nil(t, mysqlRepo.moments[moment.ID])
}

func TestCommentMoment_Success(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	// 预填充一条动态
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{
		AuthorID:   100,
		Content:    "Test moment",
		Visibility: 1,
		CreatedAt:  time.Now(),
	})
	mysqlRepo.friends[200] = []int64{100}

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	commentID, err := svc.CommentMoment(context.Background(), 200, 1, "Great post!")
	assert.NoError(t, err)
	assert.NotZero(t, commentID)

	// 验证评论已存储
	comment, ok := mysqlRepo.comments[commentID]
	assert.True(t, ok)
	assert.Equal(t, int64(200), comment.UserID)
	assert.Equal(t, int64(1), comment.MomentID)
	assert.Equal(t, "Great post!", comment.Content)
}

func TestCommentMoment_MomentNotFound(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	_, err := svc.CommentMoment(context.Background(), 200, 999, "comment")
	assert.Error(t, err)
	assert.Equal(t, ErrMomentNotFound, err.Error())
}

func TestCommentMoment_EmptyContent(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	// 预填充一条动态
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{
		AuthorID:   100,
		Content:    "Test moment",
		Visibility: 1,
		CreatedAt:  time.Now(),
	})

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	_, err := svc.CommentMoment(context.Background(), 200, 1, "")
	assert.Error(t, err)
	assert.Equal(t, ErrMomentContentEmpty, err.Error())
}

func TestDeleteComment_OwnershipValidation(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	// 预填充一条动态和用户 200 的评论
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{
		AuthorID:   100,
		Content:    "Test moment",
		Visibility: 1,
		CreatedAt:  time.Now(),
	})
	commentID := time.Now().UnixNano()
	mysqlRepo.CreateMomentComment(context.Background(), &model.MomentComment{
		ID:        commentID,
		MomentID:  1,
		UserID:    200, // 评论属于用户 200
		Content:   "Great post!",
		CreatedAt: time.Now(),
	})

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	// 用户 200（所有者）应该能够删除自己的评论
	err := svc.DeleteComment(context.Background(), 200, commentID)
	assert.NoError(t, err)

	// 验证评论已移除
	_, ok := mysqlRepo.comments[commentID]
	assert.False(t, ok)
}

func TestDeleteComment_NotOwner(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	// 预填充一条动态和用户 200 的评论
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{
		AuthorID:   100,
		Content:    "Test moment",
		Visibility: 1,
		CreatedAt:  time.Now(),
	})
	commentID := time.Now().UnixNano()
	mysqlRepo.CreateMomentComment(context.Background(), &model.MomentComment{
		ID:        commentID,
		MomentID:  1,
		UserID:    200, // 评论属于用户 200
		Content:   "Great post!",
		CreatedAt: time.Now(),
	})

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	// 用户 300（非所有者）不应能够删除用户 200 的评论
	err := svc.DeleteComment(context.Background(), 300, commentID)
	assert.Error(t, err)
	assert.Equal(t, ErrNotCommentOwner, err.Error())

	// 验证评论未被移除
	_, ok := mysqlRepo.comments[commentID]
	assert.True(t, ok)
}

func TestDeleteComment_NotFound(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	err := svc.DeleteComment(context.Background(), 200, 9999999)
	assert.Error(t, err)
	assert.Equal(t, ErrCommentNotFound, err.Error())
}

func TestGetFeed_Success(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	// 预填充动态
	base := time.Now().UnixMilli()
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{AuthorID: 100, Content: "Moment 1", Visibility: 2, CreatedAt: time.Now()})
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{AuthorID: 101, Content: "Moment 2", Visibility: 2, CreatedAt: time.Now()})
	mysqlRepo.friends[200] = []int64{100, 101}

	// 用户 200 的收件箱（普通好友推来的两条）
	redisRepo.PublishMomentFeed(context.Background(), 200, 1, base+1)
	redisRepo.PublishMomentFeed(context.Background(), 200, 2, base+2)

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	moments, next, err := svc.GetFeed(context.Background(), 200, "", 10)
	assert.NoError(t, err)
	assert.Len(t, moments, 2)
	assert.Empty(t, next) // 未超过 limit，无下一页
	// 降序：最新(id=2)在前
	assert.Equal(t, int64(2), moments[0].ID)
	assert.Equal(t, int64(1), moments[1].ID)
}

func TestGetFeed_Empty(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	moments, next, err := svc.GetFeed(context.Background(), 200, "", 10)
	assert.NoError(t, err)
	assert.Len(t, moments, 0)
	assert.Empty(t, next)
}

// TestGetFeed_Hybrid 验证推拉合并：自己寄件箱 + 普通好友收件箱 + 大V好友寄件箱三源归并去重。
func TestGetFeed_Hybrid(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	base := time.Now().UnixMilli()
	// 动态 1=自己(200)发的, 2=普通好友(101)推来的, 3=大V好友(300)的
	for i := int64(1); i <= 3; i++ {
		mysqlRepo.CreateMoment(context.Background(), &model.Moment{AuthorID: 100 + i, Content: fmt.Sprintf("m%d", i), Visibility: 2, CreatedAt: time.Now()})
	}
	// 用户 200 的好友：101（普通）、300（大V）
	mysqlRepo.friends[200] = []int64{101, 102, 103, 300}
	redisRepo.bigUsers[300] = true

	redisRepo.AddToOutbox(context.Background(), 200, 1, base+1, 0)    // 自己寄件箱
	redisRepo.PublishMomentFeed(context.Background(), 200, 2, base+2) // 普通好友推入收件箱
	redisRepo.AddToOutbox(context.Background(), 300, 3, base+3, 0)    // 大V好友寄件箱（拉取）

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	moments, _, err := svc.GetFeed(context.Background(), 200, "", 10)
	assert.NoError(t, err)
	assert.Len(t, moments, 3)
	// 降序 by ts: 3,2,1
	assert.Equal(t, int64(3), moments[0].ID)
	assert.Equal(t, int64(2), moments[1].ID)
	assert.Equal(t, int64(1), moments[2].ID)
}

// TestGetFeed_CursorPagination 验证游标分页：连续两页无重复无漏读。
func TestGetFeed_CursorPagination(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	base := time.Now().UnixMilli()
	// 5 条动态进入用户 200 收件箱，ts 递增
	for i := int64(1); i <= 5; i++ {
		mysqlRepo.CreateMoment(context.Background(), &model.Moment{AuthorID: 100, Content: fmt.Sprintf("m%d", i), Visibility: 2, CreatedAt: time.Now()})
		redisRepo.PublishMomentFeed(context.Background(), 200, i, base+i)
	}
	mysqlRepo.friends[200] = []int64{100}

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	// 第一页：limit=2 → 应返回 id 5,4，且有 next_cursor
	page1, next1, err := svc.GetFeed(context.Background(), 200, "", 2)
	assert.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, next1)
	assert.Equal(t, int64(5), page1[0].ID)
	assert.Equal(t, int64(4), page1[1].ID)

	// 第二页：用 next_cursor → 应返回 id 3,2，无重复
	page2, next2, err := svc.GetFeed(context.Background(), 200, next1, 2)
	assert.NoError(t, err)
	assert.Len(t, page2, 2)
	assert.NotEmpty(t, next2)
	assert.Equal(t, int64(3), page2[0].ID)
	assert.Equal(t, int64(2), page2[1].ID)

	// 第三页：应返回最后一条 id 1，无下一页
	page3, next3, err := svc.GetFeed(context.Background(), 200, next2, 2)
	assert.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, next3)
	assert.Equal(t, int64(1), page3[0].ID)
}

// TestGetFeed_PrivateFiltered 验证他人的私密动态即便出现在源里也被读时过滤。
func TestGetFeed_PrivateFiltered(t *testing.T) {
	mysqlRepo := newMockMySQLRepo()
	redisRepo := newMomentMockRedisRepo()
	mqRepo := newMomentMockMQRepo()
	logger, _ := zap.NewDevelopment()

	base := time.Now().UnixMilli()
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{AuthorID: 300, Content: "私密", Visibility: 3, CreatedAt: time.Now()}) // id=1, 大V好友的私密
	mysqlRepo.CreateMoment(context.Background(), &model.Moment{AuthorID: 300, Content: "公开", Visibility: 1, CreatedAt: time.Now()}) // id=2

	mysqlRepo.friends[200] = []int64{300}
	redisRepo.bigUsers[300] = true
	// 两条都在大V寄件箱（模拟消费者也把私密写进了作者寄件箱）
	redisRepo.AddToOutbox(context.Background(), 300, 1, base+1, 0)
	redisRepo.AddToOutbox(context.Background(), 300, 2, base+2, 0)

	svc := NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Hour)

	moments, _, err := svc.GetFeed(context.Background(), 200, "", 10)
	assert.NoError(t, err)
	assert.Len(t, moments, 1) // 私密(id=1)被过滤
	assert.Equal(t, int64(2), moments[0].ID)
}
