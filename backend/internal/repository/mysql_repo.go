package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/goim/goim/internal/model"
)

// MySQLRepo 定义了服务和消费者所需的所有 MySQL CRUD 操作。
// 该接口允许在测试中进行模拟。方法将在任务 12-17 中逐步完善。
type MySQLRepo interface {
	// ── 消息 ──
	InsertPrivateMessage(ctx context.Context, msg *model.PrivateMessage) error
	InsertGroupMessage(ctx context.Context, msg *model.GroupMessage) error
	InsertMsgRevoked(ctx context.Context, revoked *model.MsgRevoked) error

	// ── 用户 ──
	GetUserByID(ctx context.Context, userID int64) (*model.User, error)
	GetUserByUsername(ctx context.Context, username string) (*model.User, error)
	CreateUser(ctx context.Context, user *model.User) error
	UpdateUser(ctx context.Context, user *model.User) error

	// ── 好友 ──
	CreateFriendRequest(ctx context.Context, req *model.FriendRequest) error
	UpdateFriendRequest(ctx context.Context, req *model.FriendRequest) error
	GetFriendRequestByID(ctx context.Context, id int64) (*model.FriendRequest, error)
	GetFriendRequestsByUser(ctx context.Context, userID int64) ([]model.FriendRequest, error)
	CreateFriendship(ctx context.Context, fs *model.Friendship) error
	DeleteFriendship(ctx context.Context, userID, friendID int64) error
	GetFriendList(ctx context.Context, userID int64) ([]model.Friendship, error)
	CountFriends(ctx context.Context, userID int64) (int, error)
	IsFriend(ctx context.Context, userID, friendID int64) (bool, error)
	CreateBlacklist(ctx context.Context, bl *model.Blacklist) error
	DeleteBlacklist(ctx context.Context, userID, blockedID int64) error
	IsBlocked(ctx context.Context, userID, blockedID int64) (bool, error)

	// ── 群组 ──
	CreateGroup(ctx context.Context, group *model.Group) (int64, error)
	UpdateGroup(ctx context.Context, group *model.Group) error
	GetGroupByID(ctx context.Context, groupID int64) (*model.Group, error)
	AddGroupMember(ctx context.Context, member *model.GroupMember) error
	RemoveGroupMember(ctx context.Context, groupID, userID int64) error
	GetGroupMembers(ctx context.Context, groupID int64) ([]model.GroupMember, error)
	UpdateGroupMemberRole(ctx context.Context, groupID, userID, role int) error

	// ── 朋友圈 ──
	CreateMoment(ctx context.Context, moment *model.Moment) error
	GetMomentByID(ctx context.Context, id int64) (*model.Moment, error)
	DeleteMoment(ctx context.Context, id int64) error
	GetMomentsByIDs(ctx context.Context, ids []int64) ([]model.Moment, error)
	GetMomentsByUser(ctx context.Context, userID int64, limit, offset int) ([]model.Moment, error)
	// 点赞明细的持久化：写路径走 Redis，MySQL 由 like_persist 消费者批量镜像。
	GetMomentLikers(ctx context.Context, momentID int64) ([]int64, error)         // 预热回源：某动态的全部点赞用户
	BatchUpsertMomentLikes(ctx context.Context, likes []model.MomentLike) error   // 批量落库（INSERT IGNORE）
	BatchDeleteMomentLikes(ctx context.Context, keys []model.MomentLikeKey) error // 批量删除取消赞
	CreateMomentComment(ctx context.Context, comment *model.MomentComment) error
	GetMomentCommentByID(ctx context.Context, id int64) (*model.MomentComment, error)
	GetMomentComments(ctx context.Context, momentID int64) ([]model.MomentComment, error)
	DeleteMomentComment(ctx context.Context, id int64) error

	// ── 用户设置 ──
	GetUserSettings(ctx context.Context, userID int64) (*model.UserSettings, error)
	CreateOrUpdateUserSettings(ctx context.Context, settings *model.UserSettings) error

	// ── 消息搜索 ──
	SearchPrivateMessages(ctx context.Context, userID int64, query string, limit, offset int) ([]model.PrivateMessage, error)
}

// ──────────────────────────────────────────────────────
// MySQLRepoImpl — 基于 database/sql 的具体实现
// ──────────────────────────────────────────────────────

type MySQLRepoImpl struct {
	db *sql.DB
}

func NewMySQLRepo(db *sql.DB) *MySQLRepoImpl {
	return &MySQLRepoImpl{db: db}
}

// ── 消息 ──

func (m *MySQLRepoImpl) InsertPrivateMessage(ctx context.Context, msg *model.PrivateMessage) error {
	query := `INSERT INTO private_messages (id, client_msg_id, sender_id, receiver_id, content, msg_type, created_at)
	          VALUES (?, ?, ?, ?, ?, ?, ?)
	          ON DUPLICATE KEY UPDATE id = id`
	result, err := m.db.ExecContext(ctx, query,
		msg.ID,
		msg.ClientMsgID,
		msg.SenderID,
		msg.ReceiverID,
		msg.Content,
		msg.MsgType,
		msg.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("插入 private_messages: %w", err)
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		var stored model.PrivateMessage
		var clientID sql.NullString
		err := m.db.QueryRowContext(ctx, `SELECT client_msg_id, sender_id, receiver_id, content, msg_type FROM private_messages WHERE id = ?`, msg.ID).Scan(&clientID, &stored.SenderID, &stored.ReceiverID, &stored.Content, &stored.MsgType)
		if err != nil {
			return fmt.Errorf("verify duplicate private message: %w", err)
		}
		if clientID.Valid {
			stored.ClientMsgID = clientID.String
		}
		if stored.ClientMsgID != msg.ClientMsgID || stored.SenderID != msg.SenderID || stored.ReceiverID != msg.ReceiverID || stored.Content != msg.Content || stored.MsgType != msg.MsgType {
			return fmt.Errorf("private message id %d conflicts with existing content", msg.ID)
		}
	}
	return nil
}

func (m *MySQLRepoImpl) InsertGroupMessage(ctx context.Context, msg *model.GroupMessage) error {
	query := `INSERT INTO group_messages (id, client_msg_id, group_id, sender_id, content, msg_type, group_seq, created_at)
	          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	          ON DUPLICATE KEY UPDATE id = id`
	result, err := m.db.ExecContext(ctx, query,
		msg.ID,
		msg.ClientMsgID,
		msg.GroupID,
		msg.SenderID,
		msg.Content,
		msg.MsgType,
		msg.GroupSeq,
		msg.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("插入 group_messages: %w", err)
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		var stored model.GroupMessage
		var clientID sql.NullString
		err := m.db.QueryRowContext(ctx, `SELECT client_msg_id, group_id, sender_id, content, msg_type, group_seq FROM group_messages WHERE id = ?`, msg.ID).Scan(&clientID, &stored.GroupID, &stored.SenderID, &stored.Content, &stored.MsgType, &stored.GroupSeq)
		if err != nil {
			return fmt.Errorf("verify duplicate group message: %w", err)
		}
		if clientID.Valid {
			stored.ClientMsgID = clientID.String
		}
		if stored.ClientMsgID != msg.ClientMsgID || stored.GroupID != msg.GroupID || stored.SenderID != msg.SenderID || stored.Content != msg.Content || stored.MsgType != msg.MsgType || stored.GroupSeq != msg.GroupSeq {
			return fmt.Errorf("group message id %d conflicts with existing content", msg.ID)
		}
	}
	return nil
}

func (m *MySQLRepoImpl) InsertMsgRevoked(ctx context.Context, revoked *model.MsgRevoked) error {
	query := `INSERT INTO msg_revoked (msg_id, conv_id, operator_id, revoked_at)
	          VALUES (?, ?, ?, ?)`
	result, err := m.db.ExecContext(ctx, query,
		revoked.MsgID,
		revoked.ConvID,
		revoked.OperatorID,
		revoked.RevokedAt,
	)
	if err != nil {
		return fmt.Errorf("插入 msg_revoked: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("获取 msg_revoked 最后插入ID: %w", err)
	}
	revoked.ID = id
	return nil
}

// ── 用户（在任务 12 中完善） ──

func (m *MySQLRepoImpl) GetUserByID(ctx context.Context, userID int64) (*model.User, error) {
	query := `SELECT id, username, password_hash, nickname, avatar_url, sign, gender, created_at, updated_at
	          FROM users WHERE id = ?`
	row := m.db.QueryRowContext(ctx, query, userID)
	var u model.User
	err := row.Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Nickname, &u.AvatarURL, &u.Sign, &u.Gender, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("按ID获取用户: %w", err)
	}
	return &u, nil
}

func (m *MySQLRepoImpl) GetUserByUsername(ctx context.Context, username string) (*model.User, error) {
	query := `SELECT id, username, password_hash, nickname, avatar_url, sign, gender, created_at, updated_at
	          FROM users WHERE username = ?`
	row := m.db.QueryRowContext(ctx, query, username)
	var u model.User
	err := row.Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Nickname, &u.AvatarURL, &u.Sign, &u.Gender, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("按用户名获取用户: %w", err)
	}
	return &u, nil
}

func (m *MySQLRepoImpl) CreateUser(ctx context.Context, user *model.User) error {
	query := `INSERT INTO users (username, password_hash, nickname, avatar_url, sign, gender)
	          VALUES (?, ?, ?, ?, ?, ?)`
	result, err := m.db.ExecContext(ctx, query,
		user.Username,
		user.PasswordHash,
		user.Nickname,
		user.AvatarURL,
		user.Sign,
		user.Gender,
	)
	if err != nil {
		return fmt.Errorf("创建用户: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("创建用户 获取最后插入ID: %w", err)
	}
	user.ID = id
	return nil
}

func (m *MySQLRepoImpl) UpdateUser(ctx context.Context, user *model.User) error {
	query := `UPDATE users SET username=?, password_hash=?, nickname=?, avatar_url=?, sign=?, gender=? WHERE id=?`
	_, err := m.db.ExecContext(ctx, query, user.Username, user.PasswordHash, user.Nickname, user.AvatarURL, user.Sign, user.Gender, user.ID)
	if err != nil {
		return fmt.Errorf("更新用户: %w", err)
	}
	return nil
}

// ── 好友（在任务 13 中实现） ──

func (m *MySQLRepoImpl) CreateFriendRequest(ctx context.Context, req *model.FriendRequest) error {
	query := `INSERT INTO friend_requests (from_user_id, to_user_id, message, status)
	          VALUES (?, ?, ?, ?)
	          ON DUPLICATE KEY UPDATE
	              id = LAST_INSERT_ID(id),
	              message = IF(status = 0, message, VALUES(message)),
	              created_at = IF(status = 0, created_at, NOW()),
	              updated_at = IF(status = 0, updated_at, NOW()),
	              status = IF(status = 0, status, VALUES(status))`
	result, err := m.db.ExecContext(ctx, query,
		req.FromUserID,
		req.ToUserID,
		req.Message,
		req.Status,
	)
	if err != nil {
		return fmt.Errorf("插入 friend_requests: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("插入 friend_requests 获取最后插入ID: %w", err)
	}
	req.ID = id
	return nil
}

func (m *MySQLRepoImpl) UpdateFriendRequest(ctx context.Context, req *model.FriendRequest) error {
	query := `UPDATE friend_requests SET status=?, updated_at=NOW() WHERE id=?`
	_, err := m.db.ExecContext(ctx, query, req.Status, req.ID)
	if err != nil {
		return fmt.Errorf("更新 friend_requests: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) GetFriendRequestByID(ctx context.Context, id int64) (*model.FriendRequest, error) {
	query := `SELECT id, from_user_id, to_user_id, message, status, created_at, updated_at
	          FROM friend_requests WHERE id = ?`
	row := m.db.QueryRowContext(ctx, query, id)
	var r model.FriendRequest
	err := row.Scan(&r.ID, &r.FromUserID, &r.ToUserID, &r.Message, &r.Status, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("按ID获取好友请求: %w", err)
	}
	return &r, nil
}

func (m *MySQLRepoImpl) GetFriendRequestsByUser(ctx context.Context, userID int64) ([]model.FriendRequest, error) {
	query := `SELECT id, from_user_id, to_user_id, message, status, created_at, updated_at
	          FROM friend_requests
	          WHERE to_user_id = ? AND status = 0
	          ORDER BY created_at DESC`
	rows, err := m.db.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("按用户获取好友请求: %w", err)
	}
	defer rows.Close()

	var results []model.FriendRequest
	for rows.Next() {
		var r model.FriendRequest
		if err := rows.Scan(&r.ID, &r.FromUserID, &r.ToUserID, &r.Message, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("扫描好友请求: %w", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历好友请求: %w", err)
	}
	return results, nil
}

func (m *MySQLRepoImpl) CreateFriendship(ctx context.Context, fs *model.Friendship) error {
	// 插入双向记录：user->friend 和 friend->user
	query := `INSERT INTO friendships (user_id, friend_id) VALUES (?, ?)`
	_, err := m.db.ExecContext(ctx, query, fs.UserID, fs.FriendID)
	if err != nil {
		return fmt.Errorf("插入好友关系 user->friend: %w", err)
	}
	_, err = m.db.ExecContext(ctx, query, fs.FriendID, fs.UserID)
	if err != nil {
		return fmt.Errorf("插入好友关系 friend->user: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) DeleteFriendship(ctx context.Context, userID, friendID int64) error {
	// 删除双向记录：user->friend 和 friend->user
	query := `DELETE FROM friendships WHERE (user_id = ? AND friend_id = ?) OR (user_id = ? AND friend_id = ?)`
	_, err := m.db.ExecContext(ctx, query, userID, friendID, friendID, userID)
	if err != nil {
		return fmt.Errorf("删除好友关系: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) GetFriendList(ctx context.Context, userID int64) ([]model.Friendship, error) {
	query := `SELECT f.id, f.user_id, f.friend_id, f.created_at
	          FROM friendships f
	          WHERE f.user_id = ?`
	rows, err := m.db.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("获取好友列表: %w", err)
	}
	defer rows.Close()

	var results []model.Friendship
	for rows.Next() {
		var fs model.Friendship
		if err := rows.Scan(&fs.ID, &fs.UserID, &fs.FriendID, &fs.CreatedAt); err != nil {
			return nil, fmt.Errorf("扫描好友关系: %w", err)
		}
		results = append(results, fs)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历好友关系: %w", err)
	}
	return results, nil
}

func (m *MySQLRepoImpl) CountFriends(ctx context.Context, userID int64) (int, error) {
	// 走 friendships 的 idx_user 索引，成本低
	query := `SELECT COUNT(*) FROM friendships WHERE user_id = ?`
	var count int
	if err := m.db.QueryRowContext(ctx, query, userID).Scan(&count); err != nil {
		return 0, fmt.Errorf("统计好友数: %w", err)
	}
	return count, nil
}

func (m *MySQLRepoImpl) IsFriend(ctx context.Context, userID, friendID int64) (bool, error) {
	query := `SELECT COUNT(*) FROM friendships WHERE user_id = ? AND friend_id = ?`
	var count int
	err := m.db.QueryRowContext(ctx, query, userID, friendID).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("检查是否为好友: %w", err)
	}
	return count > 0, nil
}

func (m *MySQLRepoImpl) CreateBlacklist(ctx context.Context, bl *model.Blacklist) error {
	query := `INSERT INTO blacklist (user_id, blocked_id) VALUES (?, ?)`
	result, err := m.db.ExecContext(ctx, query, bl.UserID, bl.BlockedID)
	if err != nil {
		return fmt.Errorf("插入黑名单: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("插入黑名单 获取最后插入ID: %w", err)
	}
	bl.ID = id
	return nil
}

func (m *MySQLRepoImpl) DeleteBlacklist(ctx context.Context, userID, blockedID int64) error {
	query := `DELETE FROM blacklist WHERE user_id = ? AND blocked_id = ?`
	_, err := m.db.ExecContext(ctx, query, userID, blockedID)
	if err != nil {
		return fmt.Errorf("删除黑名单: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) IsBlocked(ctx context.Context, userID, blockedID int64) (bool, error) {
	query := `SELECT COUNT(*) FROM blacklist WHERE user_id = ? AND blocked_id = ?`
	var count int
	err := m.db.QueryRowContext(ctx, query, userID, blockedID).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("检查是否已拉黑: %w", err)
	}
	return count > 0, nil
}

// ── 群组（在任务 14 中完善） ──

func (m *MySQLRepoImpl) CreateGroup(ctx context.Context, group *model.Group) (int64, error) {
	query := "INSERT INTO `groups` (name, notice, owner_id, max_members, created_at, updated_at) VALUES (?, ?, ?, 500, NOW(), NOW())"
	result, err := m.db.ExecContext(ctx, query,
		group.Name,
		group.Notice,
		group.OwnerID,
	)
	if err != nil {
		return 0, fmt.Errorf("创建群组: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("创建群组 获取最后插入ID: %w", err)
	}
	return id, nil
}

func (m *MySQLRepoImpl) UpdateGroup(ctx context.Context, group *model.Group) error {
	query := "UPDATE `groups` SET name=?, notice=?, owner_id=?, updated_at=NOW() WHERE id=?"
	_, err := m.db.ExecContext(ctx, query, group.Name, group.Notice, group.OwnerID, group.ID)
	if err != nil {
		return fmt.Errorf("更新群组: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) GetGroupByID(ctx context.Context, groupID int64) (*model.Group, error) {
	query := "SELECT id, name, notice, owner_id, max_members, created_at, updated_at FROM `groups` WHERE id = ?"
	row := m.db.QueryRowContext(ctx, query, groupID)
	var g model.Group
	err := row.Scan(&g.ID, &g.Name, &g.Notice, &g.OwnerID, &g.MaxMembers, &g.CreatedAt, &g.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("按ID获取群组: %w", err)
	}
	return &g, nil
}

func (m *MySQLRepoImpl) AddGroupMember(ctx context.Context, member *model.GroupMember) error {
	query := `INSERT INTO group_members (group_id, user_id, role, muted_until, joined_at)
	          VALUES (?, ?, ?, ?, NOW())`
	_, err := m.db.ExecContext(ctx, query,
		member.GroupID,
		member.UserID,
		member.Role,
		member.MutedUntil,
	)
	if err != nil {
		return fmt.Errorf("添加群成员: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) RemoveGroupMember(ctx context.Context, groupID, userID int64) error {
	query := `DELETE FROM group_members WHERE group_id=? AND user_id=?`
	_, err := m.db.ExecContext(ctx, query, groupID, userID)
	if err != nil {
		return fmt.Errorf("移除群成员: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) GetGroupMembers(ctx context.Context, groupID int64) ([]model.GroupMember, error) {
	query := `SELECT id, group_id, user_id, role, muted_until, joined_at
	          FROM group_members WHERE group_id = ?`
	rows, err := m.db.QueryContext(ctx, query, groupID)
	if err != nil {
		return nil, fmt.Errorf("获取群成员: %w", err)
	}
	defer rows.Close()

	members := make([]model.GroupMember, 0)
	for rows.Next() {
		var gm model.GroupMember
		err := rows.Scan(&gm.ID, &gm.GroupID, &gm.UserID, &gm.Role, &gm.MutedUntil, &gm.JoinedAt)
		if err != nil {
			return nil, fmt.Errorf("扫描群成员: %w", err)
		}
		members = append(members, gm)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历群成员: %w", err)
	}
	return members, nil
}

func (m *MySQLRepoImpl) UpdateGroupMemberRole(ctx context.Context, groupID, userID, role int) error {
	query := `UPDATE group_members SET role=? WHERE group_id=? AND user_id=?`
	_, err := m.db.ExecContext(ctx, query, role, groupID, userID)
	if err != nil {
		return fmt.Errorf("更新群成员角色: %w", err)
	}
	return nil
}

// ── 朋友圈 ──

func (m *MySQLRepoImpl) CreateMoment(ctx context.Context, moment *model.Moment) error {
	query := `INSERT INTO moments (author_id, content, media_urls, visibility, created_at)
	          VALUES (?, ?, ?, ?, ?)`
	result, err := m.db.ExecContext(ctx, query,
		moment.AuthorID,
		moment.Content,
		moment.MediaUrls,
		moment.Visibility,
		moment.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("插入朋友圈动态: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("插入朋友圈动态 获取最后插入ID: %w", err)
	}
	moment.ID = id
	return nil
}

func (m *MySQLRepoImpl) GetMomentByID(ctx context.Context, id int64) (*model.Moment, error) {
	query := `SELECT id, author_id, content, media_urls, visibility, created_at
	          FROM moments WHERE id = ?`
	row := m.db.QueryRowContext(ctx, query, id)
	var moment model.Moment
	err := row.Scan(&moment.ID, &moment.AuthorID, &moment.Content, &moment.MediaUrls, &moment.Visibility, &moment.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("按ID获取朋友圈动态: %w", err)
	}
	return &moment, nil
}

// DeleteMoment 删除动态及其关联评论、持久化点赞明细。三张表不设外键，因此在事务内显式清理。
func (m *MySQLRepoImpl) DeleteMoment(ctx context.Context, id int64) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开启删除动态事务: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, query := range []string{
		`DELETE FROM moment_comments WHERE moment_id = ?`,
		`DELETE FROM moment_likes WHERE moment_id = ?`,
		`DELETE FROM moments WHERE id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, query, id); err != nil {
			return fmt.Errorf("删除动态关联数据: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交删除动态事务: %w", err)
	}
	return nil
}

// GetMomentsByIDs 批量按 ID 查询动态，用于 Feed 补全，消除 N+1 查询。
// 返回的顺序不保证与入参一致；调用方应按需自行重排（例如按 Feed 游标顺序）。
func (m *MySQLRepoImpl) GetMomentsByIDs(ctx context.Context, ids []int64) ([]model.Moment, error) {
	if len(ids) == 0 {
		return []model.Moment{}, nil
	}

	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`SELECT id, author_id, content, media_urls, visibility, created_at
	          FROM moments WHERE id IN (%s)`, strings.Join(placeholders, ","))
	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("按ID批量获取朋友圈动态: %w", err)
	}
	defer rows.Close()

	moments := make([]model.Moment, 0, len(ids))
	for rows.Next() {
		var moment model.Moment
		if err := rows.Scan(&moment.ID, &moment.AuthorID, &moment.Content, &moment.MediaUrls, &moment.Visibility, &moment.CreatedAt); err != nil {
			return nil, fmt.Errorf("扫描朋友圈动态: %w", err)
		}
		moments = append(moments, moment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历结果行错误: %w", err)
	}
	return moments, nil
}

func (m *MySQLRepoImpl) GetMomentsByUser(ctx context.Context, userID int64, limit, offset int) ([]model.Moment, error) {
	query := `SELECT id, author_id, content, media_urls, visibility, created_at
	          FROM moments WHERE author_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`
	rows, err := m.db.QueryContext(ctx, query, userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("按用户获取朋友圈动态: %w", err)
	}
	defer rows.Close()

	moments := make([]model.Moment, 0)
	for rows.Next() {
		var moment model.Moment
		if err := rows.Scan(&moment.ID, &moment.AuthorID, &moment.Content, &moment.MediaUrls, &moment.Visibility, &moment.CreatedAt); err != nil {
			return nil, fmt.Errorf("扫描朋友圈动态: %w", err)
		}
		moments = append(moments, moment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历结果行错误: %w", err)
	}
	return moments, nil
}

// GetMomentLikers 返回某条动态的全部点赞用户 ID，用于 Redis 点赞缓存的冷启动预热。走 idx_moment。
func (m *MySQLRepoImpl) GetMomentLikers(ctx context.Context, momentID int64) ([]int64, error) {
	query := `SELECT user_id FROM moment_likes WHERE moment_id = ?`
	rows, err := m.db.QueryContext(ctx, query, momentID)
	if err != nil {
		return nil, fmt.Errorf("查询动态点赞用户: %w", err)
	}
	defer rows.Close()

	userIDs := make([]int64, 0)
	for rows.Next() {
		var uid int64
		if err := rows.Scan(&uid); err != nil {
			return nil, fmt.Errorf("扫描点赞用户: %w", err)
		}
		userIDs = append(userIDs, uid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历点赞用户结果: %w", err)
	}
	return userIDs, nil
}

// BatchUpsertMomentLikes 批量写入点赞明细。INSERT IGNORE 依赖唯一键 uk_moment_user 幂等去重，
// 因此消费者重投（requeue）重复执行安全。
func (m *MySQLRepoImpl) BatchUpsertMomentLikes(ctx context.Context, likes []model.MomentLike) error {
	if len(likes) == 0 {
		return nil
	}
	placeholders := make([]string, len(likes))
	args := make([]interface{}, 0, len(likes)*3)
	for i, lk := range likes {
		placeholders[i] = "(?, ?, ?)"
		args = append(args, lk.MomentID, lk.UserID, lk.CreatedAt)
	}
	query := fmt.Sprintf(`INSERT IGNORE INTO moment_likes (moment_id, user_id, created_at) VALUES %s`,
		strings.Join(placeholders, ","))
	if _, err := m.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("批量插入朋友圈点赞: %w", err)
	}
	return nil
}

// BatchDeleteMomentLikes 批量删除点赞明细（取消赞）。DELETE 幂等，重投安全。
func (m *MySQLRepoImpl) BatchDeleteMomentLikes(ctx context.Context, keys []model.MomentLikeKey) error {
	if len(keys) == 0 {
		return nil
	}
	placeholders := make([]string, len(keys))
	args := make([]interface{}, 0, len(keys)*2)
	for i, k := range keys {
		placeholders[i] = "(?, ?)"
		args = append(args, k.MomentID, k.UserID)
	}
	query := fmt.Sprintf(`DELETE FROM moment_likes WHERE (moment_id, user_id) IN (%s)`,
		strings.Join(placeholders, ","))
	if _, err := m.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("批量删除朋友圈点赞: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) CreateMomentComment(ctx context.Context, comment *model.MomentComment) error {
	query := `INSERT INTO moment_comments (moment_id, user_id, content, created_at) VALUES (?, ?, ?, ?)`
	result, err := m.db.ExecContext(ctx, query, comment.MomentID, comment.UserID, comment.Content, comment.CreatedAt)
	if err != nil {
		return fmt.Errorf("插入朋友圈评论: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("获取朋友圈评论最后插入 ID: %w", err)
	}
	comment.ID = id
	return nil
}

func (m *MySQLRepoImpl) GetMomentCommentByID(ctx context.Context, id int64) (*model.MomentComment, error) {
	query := `SELECT id, moment_id, user_id, content, created_at
	          FROM moment_comments WHERE id = ?`
	row := m.db.QueryRowContext(ctx, query, id)
	var comment model.MomentComment
	err := row.Scan(&comment.ID, &comment.MomentID, &comment.UserID, &comment.Content, &comment.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("按ID获取朋友圈评论: %w", err)
	}
	return &comment, nil
}

func (m *MySQLRepoImpl) GetMomentComments(ctx context.Context, momentID int64) ([]model.MomentComment, error) {
	query := `SELECT c.id, c.moment_id, c.user_id, c.content, c.created_at, u.username, u.avatar_url
	          FROM moment_comments c JOIN users u ON u.id = c.user_id
	          WHERE c.moment_id = ? ORDER BY c.id ASC`
	rows, err := m.db.QueryContext(ctx, query, momentID)
	if err != nil {
		return nil, fmt.Errorf("获取朋友圈评论列表: %w", err)
	}
	defer rows.Close()
	comments := make([]model.MomentComment, 0)
	for rows.Next() {
		var comment model.MomentComment
		if err := rows.Scan(&comment.ID, &comment.MomentID, &comment.UserID, &comment.Content, &comment.CreatedAt, &comment.Username, &comment.AvatarURL); err != nil {
			return nil, fmt.Errorf("扫描朋友圈评论: %w", err)
		}
		comments = append(comments, comment)
	}
	return comments, rows.Err()
}

func (m *MySQLRepoImpl) DeleteMomentComment(ctx context.Context, id int64) error {
	query := `DELETE FROM moment_comments WHERE id = ?`
	_, err := m.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("删除朋友圈评论: %w", err)
	}
	return nil
}

// ── 用户设置（在任务 17 中完善） ──

func (m *MySQLRepoImpl) GetUserSettings(ctx context.Context, userID int64) (*model.UserSettings, error) {
	query := `SELECT id, user_id, notification_enabled, msg_preview_enabled, mute_list, created_at, updated_at
	          FROM user_settings WHERE user_id = ?`
	row := m.db.QueryRowContext(ctx, query, userID)
	var s model.UserSettings
	var muteList sql.NullString
	err := row.Scan(&s.ID, &s.UserID, &s.NotificationEnabled, &s.MsgPreviewEnabled, &muteList, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("获取用户设置: %w", err)
	}
	if muteList.Valid {
		s.MuteList = muteList.String
	}
	return &s, nil
}

func (m *MySQLRepoImpl) CreateOrUpdateUserSettings(ctx context.Context, settings *model.UserSettings) error {
	query := `INSERT INTO user_settings (user_id, notification_enabled, msg_preview_enabled, mute_list)
	          VALUES (?, ?, ?, ?)
	          ON DUPLICATE KEY UPDATE notification_enabled=VALUES(notification_enabled),
	          msg_preview_enabled=VALUES(msg_preview_enabled), mute_list=VALUES(mute_list)`
	var muteList interface{}
	if settings.MuteList == "" {
		muteList = nil
	} else {
		muteList = settings.MuteList
	}
	result, err := m.db.ExecContext(ctx, query,
		settings.UserID,
		settings.NotificationEnabled,
		settings.MsgPreviewEnabled,
		muteList,
	)
	if err != nil {
		return fmt.Errorf("创建或更新用户设置: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		// ON DUPLICATE KEY UPDATE: LastInsertId 在存在更新时可能无意义，但忽略该错误
		return nil
	}
	settings.ID = id
	return nil
}

// ── 消息搜索（在任务 17 中完善） ──

func (m *MySQLRepoImpl) SearchPrivateMessages(ctx context.Context, userID int64, query string, limit, offset int) ([]model.PrivateMessage, error) {
	sqlQuery := `SELECT id, sender_id, receiver_id, content, msg_type, created_at
	             FROM private_messages
	             WHERE (sender_id = ? OR receiver_id = ?) AND content LIKE ?
	             ORDER BY created_at DESC LIMIT ? OFFSET ?`
	likeQuery := "%" + query + "%"
	rows, err := m.db.QueryContext(ctx, sqlQuery, userID, userID, likeQuery, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("搜索私聊消息: %w", err)
	}
	defer rows.Close()

	var results []model.PrivateMessage
	for rows.Next() {
		var msg model.PrivateMessage
		if err := rows.Scan(&msg.ID, &msg.SenderID, &msg.ReceiverID, &msg.Content, &msg.MsgType, &msg.CreatedAt); err != nil {
			return nil, fmt.Errorf("扫描私聊消息: %w", err)
		}
		results = append(results, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历私聊消息: %w", err)
	}
	return results, nil
}
