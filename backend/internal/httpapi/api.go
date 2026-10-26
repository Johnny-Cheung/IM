package httpapi

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/auth"
	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
)

const (
	sessionCookieName   = "im_session"
	sessionCookieMaxAge = int(auth.SessionTTL / time.Second)
	maxRequestBodyBytes = 64 << 10
)

// Store is the subset of persistence used by the HTTP API.
type Store interface {
	CreateUser(
		ctx context.Context,
		username, usernameKey, nickname, passwordHash string,
	) (store.User, error)
	GetUserByUsernameKey(ctx context.Context, usernameKey string) (store.User, error)
	GetUserByID(ctx context.Context, userID uint64) (store.User, error)
	UpdateNickname(ctx context.Context, userID uint64, nickname string) error
	CreateSession(
		ctx context.Context,
		userID uint64,
		tokenHash []byte,
		createdAt, expiresAt time.Time,
	) error
	GetSessionWithUser(ctx context.Context, tokenHash []byte) (store.SessionUser, error)
	RevokeSession(
		ctx context.Context,
		tokenHash []byte,
		reason string,
		revokedAt time.Time,
	) error
	UpdatePasswordAndRevokeSessions(
		ctx context.Context,
		userID uint64,
		passwordHash string,
		revokedAt time.Time,
	) error
	GetRelationship(ctx context.Context, viewerID, targetID uint64) (store.Relationship, error)
	ListFriends(ctx context.Context, userID uint64) ([]store.PublicUser, error)
	ListFriendRequests(ctx context.Context, userID uint64) (store.FriendRequests, error)
	CreateFriendRequest(
		ctx context.Context,
		fromID, toID uint64,
		now time.Time,
	) (store.FriendRequestResult, error)
	AcceptFriendRequest(
		ctx context.Context,
		userID, requestID uint64,
		now time.Time,
	) (store.PublicUser, error)
	RejectFriendRequest(
		ctx context.Context,
		userID, requestID uint64,
		now time.Time,
	) error
	DeleteFriend(ctx context.Context, userID, friendID uint64) error
	ListConversations(ctx context.Context, userID uint64) ([]store.Conversation, error)
	ListPublicUsersByIDs(ctx context.Context, ids []uint64) (map[uint64]store.PublicUser, error)
	ListMessages(
		ctx context.Context,
		userID, otherID, beforeID uint64,
		limit int,
	) (store.MessagePage, error)
	ListMessagesAfter(
		ctx context.Context,
		userID, otherID, afterID uint64,
		limit int,
	) (store.MessagePage, error)
	MarkConversationRead(
		ctx context.Context,
		userID, peerID, throughID uint64,
		now time.Time,
	) (uint64, bool, error)
	CreateMessage(
		ctx context.Context,
		senderID, recipientID uint64,
		clientMessageID, body string,
		now time.Time,
	) (store.Message, bool, error)
	CreateGroup(
		ctx context.Context,
		ownerID uint64,
		name string,
		memberIDs []uint64,
		now time.Time,
	) (store.GroupDetail, error)
	ListGroupConversations(
		ctx context.Context,
		userID uint64,
	) ([]store.GroupConversation, error)
	GetGroup(ctx context.Context, userID, groupID uint64) (store.GroupDetail, error)
	RenameGroup(ctx context.Context, ownerID, groupID uint64, name string) error
	InviteGroupMembers(
		ctx context.Context,
		groupID, inviterID uint64,
		memberIDs []uint64,
		now time.Time,
	) error
	KickGroupMember(ctx context.Context, ownerID, groupID, targetID uint64) error
	LeaveGroup(ctx context.Context, userID, groupID uint64, now time.Time) (bool, error)
	ListGroupMemberIDs(ctx context.Context, groupID uint64) ([]uint64, error)
	IsGroupMember(ctx context.Context, userID, groupID uint64) (bool, error)
	ListGroupMessages(
		ctx context.Context,
		userID, groupID, beforeID uint64,
		limit int,
	) (store.GroupMessagePage, error)
	ListGroupMessagesAfter(
		ctx context.Context,
		userID, groupID, afterID uint64,
		limit int,
	) (store.GroupMessagePage, error)
	CreateGroupMessage(
		ctx context.Context,
		senderID, groupID uint64,
		clientMessageID, body string,
		now time.Time,
	) (store.GroupMessage, bool, error)
	MarkGroupRead(ctx context.Context, userID, groupID, throughID uint64) (uint64, bool, error)
}

// DirectMessageAcceptor is the optional v1.2 message acceptance path.
type DirectMessageAcceptor interface {
	Accept(context.Context, messaging.DirectMessageRequest) (messaging.DirectMessageResult, error)
}

// FriendshipProjector mirrors committed MySQL friendship changes in Redis.
type FriendshipProjector interface {
	EnsurePair(context.Context, uint64, uint64) error
	RemovePair(context.Context, uint64, uint64) error
}

// DirectMessageReader reads one conversation from the Redis window.
type DirectMessageReader interface {
	ReadConversation(context.Context, uint64, uint64, uint64, int) (messaging.ConversationPage, error)
	ReadConversationAfter(context.Context, uint64, uint64, uint64, int) (messaging.ConversationPage, error)
	ReadFeed(context.Context, uint64, uint64, int) (messaging.FeedPage, error)
	LastConversationMessages(context.Context, uint64) (map[uint64]store.Message, error)
}

// GroupMessageAcceptor is the optional Redis group acceptance path.
type GroupMessageAcceptor interface {
	Accept(context.Context, messaging.GroupMessageRequest) (messaging.GroupMessageResult, error)
}

// GroupMessageReader reads one group from the Redis window.
type GroupMessageReader interface {
	ReadGroup(context.Context, uint64, uint64, uint64, int) (messaging.GroupConversationPage, error)
	ReadGroupAfter(context.Context, uint64, uint64, uint64, int) (messaging.GroupConversationPage, error)
}

// GroupAcceptedWatermarkReader reads the highest group message accepted by Redis.
type GroupAcceptedWatermarkReader interface {
	LastAcceptedID(context.Context, uint64) (uint64, error)
}

// GroupProjector mirrors committed group membership changes in Redis.
type GroupProjector interface {
	Replace(context.Context, uint64, []uint64, bool) error
	BeginMutation(context.Context, uint64) error
	EndMutation(context.Context, uint64) error
}

// API registers the versioned JSON endpoints.
type API struct {
	store                Store
	hub                  *Hub
	directAcceptor       DirectMessageAcceptor
	directReader         DirectMessageReader
	friendshipProject    FriendshipProjector
	groupAcceptor        GroupMessageAcceptor
	groupReader          GroupMessageReader
	groupWatermarkReader GroupAcceptedWatermarkReader
	groupProjector       GroupProjector
}

type Option func(*API)

func WithDirectMessageAcceptor(acceptor DirectMessageAcceptor) Option {
	return func(api *API) {
		api.directAcceptor = acceptor
	}
}

func WithFriendshipProjector(projector FriendshipProjector) Option {
	return func(api *API) {
		api.friendshipProject = projector
	}
}

func WithDirectMessageReader(reader DirectMessageReader) Option {
	return func(api *API) {
		api.directReader = reader
	}
}

func WithGroupMessageAcceptor(acceptor GroupMessageAcceptor) Option {
	return func(api *API) {
		api.groupAcceptor = acceptor
	}
}

func WithGroupMessageReader(reader GroupMessageReader) Option {
	return func(api *API) {
		api.groupReader = reader
	}
}

func WithGroupAcceptedWatermarkReader(reader GroupAcceptedWatermarkReader) Option {
	return func(api *API) {
		api.groupWatermarkReader = reader
	}
}

func WithGroupProjector(projector GroupProjector) Option {
	return func(api *API) {
		api.groupProjector = projector
	}
}

func New(dataStore Store, options ...Option) *API {
	api := &API{store: dataStore, hub: NewHub()}
	for _, option := range options {
		option(api)
	}
	return api
}

// Register mounts authentication and current-user routes under /api.
func (a *API) Register(router *gin.Engine) {
	group := router.Group("/api")
	group.Use(limitRequestBody)
	group.POST("/auth/register", a.register)
	group.POST("/auth/login", a.login)
	group.POST("/auth/logout", a.logout)
	group.GET("/me", a.requireUser, a.me)
	group.PATCH("/me", a.requireUser, a.updateMe)
	group.PUT("/me/password", a.requireUser, a.updatePassword)

	authed := group.Group("")
	authed.Use(a.requireUser)
	authed.GET("/users/:username", a.lookupUser)
	authed.GET("/friends", a.listFriends)
	authed.DELETE("/friends/:username", a.deleteFriend)
	authed.POST("/friend-requests", a.sendFriendRequest)
	authed.GET("/friend-requests", a.listFriendRequests)
	authed.POST("/friend-requests/:id/accept", a.acceptFriendRequest)
	authed.POST("/friend-requests/:id/reject", a.rejectFriendRequest)
	authed.GET("/conversations", a.listConversations)
	authed.GET("/messages/sync", a.syncMessages)
	authed.GET("/conversations/:username/messages", a.listMessages)
	authed.POST("/conversations/:username/messages", a.sendMessage)
	authed.POST("/conversations/:username/read", a.markConversationRead)
	authed.POST("/groups", a.createGroup)
	authed.GET("/groups/:id", a.getGroup)
	authed.PATCH("/groups/:id", a.renameGroup)
	authed.POST("/groups/:id/members", a.inviteGroupMembers)
	authed.DELETE("/groups/:id/members/:username", a.kickGroupMember)
	authed.POST("/groups/:id/leave", a.leaveGroup)
	authed.GET("/groups/:id/messages", a.listGroupMessages)
	authed.POST("/groups/:id/messages", a.sendGroupMessage)
	authed.POST("/groups/:id/read", a.markGroupRead)
	authed.GET("/ws", a.websocket)
}

type registerRequest struct {
	Username string `json:"username"`
	Nickname string `json:"nickname"`
	Password string `json:"password"`
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type updateNicknameRequest struct {
	Nickname string `json:"nickname"`
}

type updatePasswordRequest struct {
	OldPassword string `json:"old_password"`
	NewPassword string `json:"new_password"`
}

type userResponse struct {
	Username string `json:"username"`
	Nickname string `json:"nickname"`
}

type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(c *gin.Context, status int, code, message string) {
	c.AbortWithStatusJSON(status, errorResponse{
		Error: errorDetail{Code: code, Message: message},
	})
}

func limitRequestBody(c *gin.Context) {
	if c.Request.Body != nil {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxRequestBodyBytes)
	}
	c.Next()
}

func (a *API) register(c *gin.Context) {
	var request registerRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "请求格式不正确")
		return
	}

	username, err := auth.CleanUsername(request.Username)
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_username", "用户名需为 3–20 位字母、数字或下划线")
		return
	}
	nickname, err := auth.CleanNickname(request.Nickname)
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_nickname", "昵称需为 1–20 个字符，且不能只包含空白")
		return
	}
	if err := auth.ValidatePassword(request.Password); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_password", "密码至少 8 个字符，且不超过 72 个字节")
		return
	}

	passwordHash, err := auth.HashPassword(request.Password)
	if err != nil {
		log.Printf("hash password: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	user, err := a.store.CreateUser(
		c.Request.Context(),
		username,
		auth.UsernameKey(username),
		nickname,
		passwordHash,
	)
	if errors.Is(err, store.ErrUsernameTaken) {
		writeError(c, http.StatusConflict, "username_taken", "用户名已被使用")
		return
	}
	if err != nil {
		log.Printf("create user: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	if err := a.startSession(c, user.ID); err != nil {
		log.Printf("create session after register: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	c.JSON(http.StatusCreated, userResponse{
		Username: user.Username,
		Nickname: user.Nickname,
	})
}

func (a *API) login(c *gin.Context) {
	var request loginRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "请求格式不正确")
		return
	}

	usernameKey := auth.UsernameKey(request.Username)
	if usernameKey == "" || request.Password == "" {
		writeError(c, http.StatusUnauthorized, "invalid_credentials", "用户名或密码错误")
		return
	}

	user, err := a.store.GetUserByUsernameKey(c.Request.Context(), usernameKey)
	if errors.Is(err, store.ErrUserNotFound) {
		auth.SpendDummyPasswordCheck(request.Password)
		writeError(c, http.StatusUnauthorized, "invalid_credentials", "用户名或密码错误")
		return
	}
	if err != nil {
		log.Printf("get user for login: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	if !auth.CheckPassword(user.PasswordHash, request.Password) {
		writeError(c, http.StatusUnauthorized, "invalid_credentials", "用户名或密码错误")
		return
	}

	if err := a.startSession(c, user.ID); err != nil {
		log.Printf("create login session: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	c.JSON(http.StatusOK, userResponse{
		Username: user.Username,
		Nickname: user.Nickname,
	})
}

func (a *API) logout(c *gin.Context) {
	if token, err := c.Cookie(sessionCookieName); err == nil && token != "" {
		tokenHash := auth.HashSessionToken(token)
		if session, err := a.store.GetSessionWithUser(c.Request.Context(), tokenHash); err == nil {
			a.hub.CloseUserWith(session.User.ID, wsCloseSession, "session_revoked")
		}
		if err := a.store.RevokeSession(
			c.Request.Context(),
			tokenHash,
			store.RevokeReasonLogout,
			time.Now().UTC(),
		); err != nil {
			log.Printf("revoke session on logout: %v", err)
		}
	}
	clearSessionCookie(c)
	c.Status(http.StatusNoContent)
}

func (a *API) me(c *gin.Context) {
	user, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	c.JSON(http.StatusOK, userResponse{
		Username: user.Username,
		Nickname: user.Nickname,
	})
}

func (a *API) updateMe(c *gin.Context) {
	user, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	var request updateNicknameRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "请求格式不正确")
		return
	}
	nickname, err := auth.CleanNickname(request.Nickname)
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_nickname", "昵称需为 1–20 个字符，且不能只包含空白")
		return
	}

	if err := a.store.UpdateNickname(c.Request.Context(), user.ID, nickname); err != nil {
		log.Printf("update nickname: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	c.JSON(http.StatusOK, userResponse{
		Username: user.Username,
		Nickname: nickname,
	})
}

func (a *API) updatePassword(c *gin.Context) {
	user, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	var request updatePasswordRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "请求格式不正确")
		return
	}
	if err := auth.ValidatePassword(request.NewPassword); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_password", "密码至少 8 个字符，且不超过 72 个字节")
		return
	}

	fullUser, err := a.store.GetUserByID(c.Request.Context(), user.ID)
	if errors.Is(err, store.ErrUserNotFound) {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if err != nil {
		log.Printf("load user for password change: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	if !auth.CheckPassword(fullUser.PasswordHash, request.OldPassword) {
		writeError(c, http.StatusBadRequest, "invalid_old_password", "旧密码不正确")
		return
	}

	passwordHash, err := auth.HashPassword(request.NewPassword)
	if err != nil {
		log.Printf("hash new password: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	if err := a.store.UpdatePasswordAndRevokeSessions(
		c.Request.Context(),
		user.ID,
		passwordHash,
		time.Now().UTC(),
	); err != nil {
		log.Printf("update password: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	a.hub.CloseUserWith(user.ID, wsCloseSession, "session_password_changed")
	clearSessionCookie(c)
	c.Status(http.StatusNoContent)
}

func currentUser(c *gin.Context) (store.User, bool) {
	value, ok := c.Get("currentUser")
	if !ok {
		return store.User{}, false
	}
	user, ok := value.(store.User)
	return user, ok
}

func currentSessionExpiresAt(c *gin.Context) time.Time {
	value, ok := c.Get("sessionExpiresAt")
	if !ok {
		return time.Time{}
	}
	expiresAt, ok := value.(time.Time)
	if !ok {
		return time.Time{}
	}
	return expiresAt
}

func (a *API) requireUser(c *gin.Context) {
	token, err := c.Cookie(sessionCookieName)
	if err != nil || token == "" {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	session, err := a.store.GetSessionWithUser(
		c.Request.Context(),
		auth.HashSessionToken(token),
	)
	if errors.Is(err, store.ErrSessionNotFound) {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if err != nil {
		log.Printf("get session: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	if session.RevokedAt != nil {
		switch session.RevokeReason {
		case store.RevokeReasonReplaced:
			writeError(c, http.StatusUnauthorized, "session_replaced", "账号已在其他设备登录，请重新登录")
		case store.RevokeReasonPasswordChanged:
			writeError(c, http.StatusUnauthorized, "session_password_changed", "密码已修改，请重新登录")
		default:
			writeError(c, http.StatusUnauthorized, "session_revoked", "登录已失效，请重新登录")
		}
		return
	}
	if !session.ExpiresAt.After(time.Now().UTC()) {
		writeError(c, http.StatusUnauthorized, "session_expired", "登录已过期，请重新登录")
		return
	}

	c.Set("currentUser", session.User)
	c.Set("sessionExpiresAt", session.ExpiresAt)
	c.Next()
}

func (a *API) startSession(c *gin.Context, userID uint64) error {
	token, tokenHash, err := auth.NewSessionToken()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if err := a.store.CreateSession(
		c.Request.Context(),
		userID,
		tokenHash,
		now,
		now.Add(auth.SessionTTL),
	); err != nil {
		return err
	}
	a.hub.CloseUserWith(userID, wsCloseSession, "session_replaced")
	setSessionCookie(c, token)
	return nil
}

func setSessionCookie(c *gin.Context, token string) {
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		MaxAge:   sessionCookieMaxAge,
		Expires:  time.Now().Add(auth.SessionTTL),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

func clearSessionCookie(c *gin.Context) {
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		Expires:  time.Unix(1, 0),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}
