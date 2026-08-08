package service

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

// ── 群组服务错误常量 ──

const (
	ErrNotOwnerOrAdmin    = "只有群主或管理员才能执行此操作"
	ErrGroupNotFound      = "群组不存在"
	ErrAlreadyMember      = "用户已是群组成员"
	ErrGroupFull          = "群组已满（最多 500 人）"
	ErrCannotRemoveOwner  = "无法移除群主"
	ErrCannotLeaveAsOwner = "群主无法退出群组；请先转让群主身份或解散群组"
	ErrInvalidRole        = "角色值必须为 0（普通成员）或 1（管理员）"
	ErrMemberNotFound     = "用户未找到"
	ErrMemberNotFriend    = "只能邀请好友加入群组"
	ErrCannotRemovePeer   = "管理员只能移除普通成员"
)

const maxGroupMembers = 500

// GroupService 处理群组相关的业务逻辑。
type GroupService struct {
	mysqlRepo repository.MySQLRepo
	redisRepo repository.RedisRepo
	logger    *zap.Logger
}

// NewGroupService 使用给定的仓库和日志记录器创建一个 GroupService。
func NewGroupService(mysqlRepo repository.MySQLRepo, redisRepo repository.RedisRepo, logger *zap.Logger) *GroupService {
	return &GroupService{
		mysqlRepo: mysqlRepo,
		redisRepo: redisRepo,
		logger:    logger,
	}
}

// CreateGroup 创建一个新群组，将群主添加为 role=2（群主）的成员，
// 并更新 Redis 缓存。返回新的 groupID。
func (s *GroupService) CreateGroup(ctx context.Context, ownerID int64, name, notice string) (int64, error) {
	group := &model.Group{
		Name:    name,
		Notice:  notice,
		OwnerID: ownerID,
	}
	groupID, err := s.mysqlRepo.CreateGroup(ctx, group)
	if err != nil {
		return 0, fmt.Errorf("create group in mysql: %w", err)
	}

	// 将群主添加为群组成员，role=2（群主）
	ownerMember := &model.GroupMember{
		GroupID: groupID,
		UserID:  ownerID,
		Role:    2, // 群主
	}
	if err := s.mysqlRepo.AddGroupMember(ctx, ownerMember); err != nil {
		return 0, fmt.Errorf("add owner as group member: %w", err)
	}

	// 更新 Redis 缓存
	if err := s.redisRepo.AddGroupMemberRedis(ctx, groupID, ownerID); err != nil {
		return 0, fmt.Errorf("sync owner membership to redis: %w", err)
	}

	return groupID, nil
}

// UpdateGroup 更新群组名称和公告。仅群主或管理员可以更新。
func (s *GroupService) UpdateGroup(ctx context.Context, userID, groupID int64, name, notice string) error {
	group, err := s.mysqlRepo.GetGroupByID(ctx, groupID)
	if err != nil {
		return fmt.Errorf("get group: %w", err)
	}
	if group == nil {
		return fmt.Errorf(ErrGroupNotFound)
	}

	// 验证 userID 是否为群主或管理员
	if !s.isOwnerOrAdmin(ctx, groupID, userID) {
		return fmt.Errorf(ErrNotOwnerOrAdmin)
	}

	group.Name = name
	group.Notice = notice
	if err := s.mysqlRepo.UpdateGroup(ctx, group); err != nil {
		return fmt.Errorf("update group: %w", err)
	}
	return nil
}

// GetGroupInfo 通过 ID 返回群组详情。
func (s *GroupService) GetGroupInfo(ctx context.Context, groupID int64) (*model.Group, error) {
	group, err := s.mysqlRepo.GetGroupByID(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("get group: %w", err)
	}
	if group == nil {
		return nil, fmt.Errorf(ErrGroupNotFound)
	}
	return group, nil
}

func (s *GroupService) ListUserGroups(ctx context.Context, userID int64) ([]model.Group, error) {
	ids, err := s.redisRepo.GetGroupMemberships(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("get user groups: %w", err)
	}
	groups := make([]model.Group, 0, len(ids))
	for _, id := range ids {
		group, err := s.mysqlRepo.GetGroupByID(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("get group: %w", err)
		}
		if group != nil {
			groups = append(groups, *group)
		}
	}
	return groups, nil
}

// AddMember 向群组添加新成员。userID 必须是群主或管理员。
// 检查最大成员上限（500）以及 newMemberID 是否已是成员。
func (s *GroupService) AddMember(ctx context.Context, groupID, userID, newMemberID int64) error {
	group, err := s.mysqlRepo.GetGroupByID(ctx, groupID)
	if err != nil {
		return fmt.Errorf("get group: %w", err)
	}
	if group == nil {
		return fmt.Errorf(ErrGroupNotFound)
	}

	// 验证 userID 是否为群主或管理员
	if !s.isOwnerOrAdmin(ctx, groupID, userID) {
		return fmt.Errorf(ErrNotOwnerOrAdmin)
	}

	user, err := s.mysqlRepo.GetUserByID(ctx, newMemberID)
	if err != nil {
		return fmt.Errorf("get member user: %w", err)
	}
	if user == nil {
		return fmt.Errorf(ErrMemberNotFound)
	}
	isFriend, err := s.mysqlRepo.IsFriend(ctx, userID, newMemberID)
	if err != nil {
		return fmt.Errorf("check member friendship: %w", err)
	}
	if !isFriend {
		return fmt.Errorf(ErrMemberNotFriend)
	}

	// 检查群组容量
	members, err := s.mysqlRepo.GetGroupMembers(ctx, groupID)
	if err != nil {
		return fmt.Errorf("get group members: %w", err)
	}
	if len(members) >= maxGroupMembers {
		return fmt.Errorf(ErrGroupFull)
	}

	// 检查是否已是成员
	for _, m := range members {
		if m.UserID == newMemberID {
			return fmt.Errorf(ErrAlreadyMember)
		}
	}

	// 在 MySQL 中添加成员
	newMember := &model.GroupMember{
		GroupID: groupID,
		UserID:  newMemberID,
		Role:    0, // 普通成员
	}
	if err := s.mysqlRepo.AddGroupMember(ctx, newMember); err != nil {
		return fmt.Errorf("add group member: %w", err)
	}

	// 更新 Redis 缓存
	if err := s.redisRepo.AddGroupMemberRedis(ctx, groupID, newMemberID); err != nil {
		// Returning success here would leave the send-time Lua authorization
		// cache stale.  The caller must retry/repair instead of believing the
		// member can already send messages.
		return fmt.Errorf("sync group membership to redis: %w", err)
	}

	return nil
}

// RemoveMember 从群组中移除成员。
// userID 必须是群主/管理员，或者 removeMemberID==userID（自行退出）。
// 群主不能被移除。
func (s *GroupService) RemoveMember(ctx context.Context, groupID, userID, removeMemberID int64) error {
	group, err := s.mysqlRepo.GetGroupByID(ctx, groupID)
	if err != nil {
		return fmt.Errorf("get group: %w", err)
	}
	if group == nil {
		return fmt.Errorf(ErrGroupNotFound)
	}

	members, err := s.mysqlRepo.GetGroupMembers(ctx, groupID)
	if err != nil {
		return fmt.Errorf("get group members: %w", err)
	}
	var actor, target *model.GroupMember
	for i := range members {
		if members[i].UserID == userID {
			actor = &members[i]
		}
		if members[i].UserID == removeMemberID {
			target = &members[i]
		}
	}
	if target == nil {
		return fmt.Errorf(ErrMemberNotFound)
	}
	if userID != removeMemberID {
		if actor == nil || (actor.Role != 1 && actor.Role != 2) {
			return fmt.Errorf(ErrNotOwnerOrAdmin)
		}
		if actor.Role == 1 && target.Role != 0 {
			return fmt.Errorf(ErrCannotRemovePeer)
		}
	}

	if target.Role == 2 || removeMemberID == group.OwnerID {
		return fmt.Errorf(ErrCannotRemoveOwner)
	}

	// Remove Redis authorization first.  Once this returns, a removed user can
	// no longer pass the send-time Lua membership check, even if the following
	// MySQL write is temporarily unavailable.
	if err := s.redisRepo.RemoveGroupMemberRedis(ctx, groupID, removeMemberID); err != nil {
		return fmt.Errorf("sync group member removal to redis: %w", err)
	}
	if err := s.mysqlRepo.RemoveGroupMember(ctx, groupID, removeMemberID); err != nil {
		return fmt.Errorf("remove group member after redis authorization removal: %w", err)
	}

	return nil
}

// KickMember 移除成员并通过 WebSocket 发送踢出通知。
// userID 必须是群主/管理员。群主不能被踢出。
func (s *GroupService) KickMember(ctx context.Context, groupID, userID, kickMemberID int64, cm interface {
	Get(int64) (interface{}, bool)
}) error {
	// 委托移除逻辑给 RemoveMember
	if err := s.RemoveMember(ctx, groupID, userID, kickMemberID); err != nil {
		return err
	}

	// 如果被踢出的用户有活跃连接，则通过 WebSocket 发送踢出通知
	if cm != nil {
		client, ok := cm.Get(kickMemberID)
		if ok {
			// 类型断言以访问 SendCh —— 接口是灵活的
			// 实际的 ConnectionManager 返回 *ClientConnection
			if sendCh, hasCh := tryGetSendCh(client); hasCh {
				kickMsg := []byte(`{"type":"group_kick","group_id":` + fmt.Sprintf("%d", groupID) + `}`)
				select {
				case sendCh <- kickMsg:
				default:
					s.logger.Warn("踢出通知被丢弃：发送缓冲区已满", zap.Int64("userID", kickMemberID))
				}
			}
		}
	}

	return nil
}

// tryGetSendCh 试图从客户端连接对象中提取 []byte 发送通道。
// 如果提取成功则返回通道和 true，否则返回 nil 和 false。
func tryGetSendCh(client interface{}) (chan []byte, bool) {
	// 我们期望一个带有 SendCh chan []byte 的结构体
	type sendChHolder interface {
		GetSendCh() chan []byte
	}
	if h, ok := client.(sendChHolder); ok {
		return h.GetSendCh(), true
	}
	return nil, false
}

// GetMembers 返回群组成员列表。
type GroupMemberListItem struct {
	model.GroupMember
	Username  string `json:"username"`
	AvatarURL string `json:"avatar_url"`
}

func (s *GroupService) GetMembers(ctx context.Context, groupID int64) ([]GroupMemberListItem, error) {
	members, err := s.mysqlRepo.GetGroupMembers(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("get group members: %w", err)
	}
	items := make([]GroupMemberListItem, 0, len(members))
	for _, member := range members {
		user, err := s.mysqlRepo.GetUserByID(ctx, member.UserID)
		if err != nil {
			return nil, fmt.Errorf("get group member profile: %w", err)
		}
		item := GroupMemberListItem{GroupMember: member}
		if user != nil {
			item.Username = user.Username
			item.AvatarURL = user.AvatarURL
		}
		items = append(items, item)
	}
	return items, nil
}

// TransferOwnership transfers owner role to an existing member.
func (s *GroupService) TransferOwnership(ctx context.Context, groupID, ownerID, newOwnerID int64) error {
	group, err := s.mysqlRepo.GetGroupByID(ctx, groupID)
	if err != nil {
		return fmt.Errorf("get group: %w", err)
	}
	if group == nil {
		return fmt.Errorf(ErrGroupNotFound)
	}
	if group.OwnerID != ownerID {
		return fmt.Errorf(ErrNotOwnerOrAdmin)
	}
	members, err := s.mysqlRepo.GetGroupMembers(ctx, groupID)
	if err != nil {
		return fmt.Errorf("get group members: %w", err)
	}
	found := false
	for _, member := range members {
		if member.UserID == newOwnerID {
			found = true
			break
		}
	}
	if !found || newOwnerID == ownerID {
		return fmt.Errorf(ErrMemberNotFound)
	}
	if err := s.mysqlRepo.UpdateGroupMemberRole(ctx, int(groupID), int(ownerID), 0); err != nil {
		return fmt.Errorf("demote old owner: %w", err)
	}
	if err := s.mysqlRepo.UpdateGroupMemberRole(ctx, int(groupID), int(newOwnerID), 2); err != nil {
		return fmt.Errorf("promote new owner: %w", err)
	}
	group.OwnerID = newOwnerID
	if err := s.mysqlRepo.UpdateGroup(ctx, group); err != nil {
		return fmt.Errorf("update group owner: %w", err)
	}
	return nil
}

// UpdateMemberRole 更改成员的角色。userID 必须是群主。
// targetUserID 不能是群主。newRole 必须为 0（普通成员）或 1（管理员）。
func (s *GroupService) UpdateMemberRole(ctx context.Context, groupID, userID, targetUserID, newRole int64) error {
	group, err := s.mysqlRepo.GetGroupByID(ctx, groupID)
	if err != nil {
		return fmt.Errorf("get group: %w", err)
	}
	if group == nil {
		return fmt.Errorf(ErrGroupNotFound)
	}

	// 只有群主可以更改角色
	if userID != group.OwnerID {
		return fmt.Errorf(ErrNotOwnerOrAdmin)
	}

	// 不能更改群主的角色
	if targetUserID == group.OwnerID {
		return fmt.Errorf(ErrCannotRemoveOwner)
	}

	// 验证角色值
	if newRole != 0 && newRole != 1 {
		return fmt.Errorf(ErrInvalidRole)
	}

	if err := s.mysqlRepo.UpdateGroupMemberRole(ctx, int(groupID), int(targetUserID), int(newRole)); err != nil {
		return fmt.Errorf("update member role: %w", err)
	}
	return nil
}

// LeaveGroup 允许用户退出群组。群主不能退出（必须先转让群主或解散群组）。
func (s *GroupService) LeaveGroup(ctx context.Context, groupID, userID int64) error {
	group, err := s.mysqlRepo.GetGroupByID(ctx, groupID)
	if err != nil {
		return fmt.Errorf("get group: %w", err)
	}
	if group == nil {
		return fmt.Errorf(ErrGroupNotFound)
	}

	// 群主不能退出
	if userID == group.OwnerID {
		return fmt.Errorf(ErrCannotLeaveAsOwner)
	}
	members, err := s.mysqlRepo.GetGroupMembers(ctx, groupID)
	if err != nil {
		return fmt.Errorf("get group members: %w", err)
	}
	found := false
	for _, member := range members {
		if member.UserID == userID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf(ErrMemberNotFound)
	}

	// Same ordering as kick/removal: authorization must be revoked first.
	if err := s.redisRepo.RemoveGroupMemberRedis(ctx, groupID, userID); err != nil {
		return fmt.Errorf("sync group leave to redis: %w", err)
	}
	if err := s.mysqlRepo.RemoveGroupMember(ctx, groupID, userID); err != nil {
		return fmt.Errorf("leave group after redis authorization removal: %w", err)
	}

	return nil
}

// isOwnerOrAdmin 检查给定的 userID 是否在群组中具有群主（role=2）或管理员（role=1）身份。
// 如果是则返回 true，否则返回 false。
func (s *GroupService) isOwnerOrAdmin(ctx context.Context, groupID, userID int64) bool {
	members, err := s.mysqlRepo.GetGroupMembers(ctx, groupID)
	if err != nil {
		s.logger.Error("获取群组成员以进行权限检查失败", zap.Error(err))
		return false
	}
	for _, m := range members {
		if m.UserID == userID && (m.Role == 1 || m.Role == 2) {
			return true
		}
	}
	return false
}
