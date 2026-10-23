package httpapi

import (
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/auth"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
)

type publicUserResponse struct {
	Username string `json:"username"`
	Nickname string `json:"nickname"`
}

type userLookupResponse struct {
	Username     string `json:"username"`
	Nickname     string `json:"nickname"`
	Relationship string `json:"relationship"`
}

type friendRequestResponse struct {
	ID        uint64             `json:"id"`
	User      publicUserResponse `json:"user"`
	CreatedAt time.Time          `json:"created_at"`
}

type friendRequestRecordResponse struct {
	ID        uint64             `json:"id"`
	User      publicUserResponse `json:"user"`
	Direction string             `json:"direction"`
	Status    string             `json:"status"`
	CreatedAt time.Time          `json:"created_at"`
	HandledAt *time.Time         `json:"handled_at,omitempty"`
}

type friendRequestsResponse struct {
	Incoming []friendRequestResponse       `json:"incoming"`
	Outgoing []friendRequestResponse       `json:"outgoing"`
	History  []friendRequestRecordResponse `json:"history"`
}

type friendsResponse struct {
	Friends []publicUserResponse `json:"friends"`
}

type sendFriendRequestRequest struct {
	Username string `json:"username"`
}

type sendFriendRequestResponse struct {
	Status    string              `json:"status"`
	RequestID uint64              `json:"request_id,omitempty"`
	Friend    *publicUserResponse `json:"friend,omitempty"`
}

type acceptFriendResponse struct {
	Friend publicUserResponse `json:"friend"`
}

func (a *API) lookupUser(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	target, ok := a.findUser(c, c.Param("username"))
	if !ok {
		return
	}

	relationship, err := a.store.GetRelationship(
		c.Request.Context(),
		current.ID,
		target.ID,
	)
	if err != nil {
		log.Printf("get relationship: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	c.JSON(http.StatusOK, userLookupResponse{
		Username:     target.Username,
		Nickname:     target.Nickname,
		Relationship: string(relationship),
	})
}

func (a *API) sendFriendRequest(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	var request sendFriendRequestRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "请求格式不正确")
		return
	}

	target, ok := a.findUser(c, request.Username)
	if !ok {
		return
	}
	if target.ID == current.ID {
		writeError(c, http.StatusBadRequest, "cannot_add_self", "不能添加自己为好友")
		return
	}

	result, err := a.store.CreateFriendRequest(
		c.Request.Context(),
		current.ID,
		target.ID,
		time.Now().UTC(),
	)
	if err != nil {
		a.writeFriendError(c, err)
		return
	}

	if result.Accepted {
		friend := toPublicUserResponse(result.Friend)
		c.JSON(http.StatusOK, sendFriendRequestResponse{
			Status: "accepted",
			Friend: &friend,
		})
		return
	}

	c.JSON(http.StatusCreated, sendFriendRequestResponse{
		Status:    "pending",
		RequestID: result.RequestID,
	})
}

func (a *API) listFriendRequests(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	requests, err := a.store.ListFriendRequests(c.Request.Context(), current.ID)
	if err != nil {
		log.Printf("list friend requests: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	response := friendRequestsResponse{
		Incoming: make([]friendRequestResponse, 0, len(requests.Incoming)),
		Outgoing: make([]friendRequestResponse, 0, len(requests.Outgoing)),
		History:  make([]friendRequestRecordResponse, 0, len(requests.History)),
	}
	for _, request := range requests.Incoming {
		response.Incoming = append(response.Incoming, toFriendRequestResponse(request))
	}
	for _, request := range requests.Outgoing {
		response.Outgoing = append(response.Outgoing, toFriendRequestResponse(request))
	}
	for _, record := range requests.History {
		response.History = append(response.History, toFriendRequestRecordResponse(record))
	}

	c.JSON(http.StatusOK, response)
}

func (a *API) acceptFriendRequest(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	requestID, ok := parseFriendRequestID(c)
	if !ok {
		return
	}

	friend, err := a.store.AcceptFriendRequest(
		c.Request.Context(),
		current.ID,
		requestID,
		time.Now().UTC(),
	)
	if err != nil {
		a.writeFriendError(c, err)
		return
	}

	c.JSON(http.StatusOK, acceptFriendResponse{
		Friend: toPublicUserResponse(friend),
	})
}

func (a *API) rejectFriendRequest(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	requestID, ok := parseFriendRequestID(c)
	if !ok {
		return
	}

	if err := a.store.RejectFriendRequest(
		c.Request.Context(),
		current.ID,
		requestID,
		time.Now().UTC(),
	); err != nil {
		a.writeFriendError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (a *API) listFriends(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	friends, err := a.store.ListFriends(c.Request.Context(), current.ID)
	if err != nil {
		log.Printf("list friends: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	response := friendsResponse{Friends: make([]publicUserResponse, 0, len(friends))}
	for _, friend := range friends {
		response.Friends = append(response.Friends, toPublicUserResponse(friend))
	}
	c.JSON(http.StatusOK, response)
}

func (a *API) deleteFriend(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	target, ok := a.findUser(c, c.Param("username"))
	if !ok {
		return
	}
	if err := a.store.DeleteFriend(c.Request.Context(), current.ID, target.ID); err != nil {
		a.writeFriendError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (a *API) findUser(c *gin.Context, username string) (store.User, bool) {
	usernameKey := auth.UsernameKey(username)
	if usernameKey == "" {
		writeError(c, http.StatusBadRequest, "invalid_username", "请输入用户名")
		return store.User{}, false
	}

	user, err := a.store.GetUserByUsernameKey(c.Request.Context(), usernameKey)
	if errors.Is(err, store.ErrUserNotFound) {
		writeError(c, http.StatusNotFound, "user_not_found", "未找到该用户")
		return store.User{}, false
	}
	if err != nil {
		log.Printf("find user: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return store.User{}, false
	}
	return user, true
}

func (a *API) writeFriendError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, store.ErrCannotAddSelf):
		writeError(c, http.StatusBadRequest, "cannot_add_self", "不能添加自己为好友")
	case errors.Is(err, store.ErrAlreadyFriends):
		writeError(c, http.StatusConflict, "already_friends", "你们已经是好友")
	case errors.Is(err, store.ErrFriendRequestExists):
		writeError(c, http.StatusConflict, "request_exists", "已有待处理的好友申请")
	case errors.Is(err, store.ErrFriendRequestNotFound):
		writeError(c, http.StatusNotFound, "request_not_found", "好友申请不存在或已处理")
	case errors.Is(err, store.ErrFriendNotFound):
		writeError(c, http.StatusNotFound, "friend_not_found", "好友关系不存在")
	case errors.Is(err, store.ErrUserNotFound):
		writeError(c, http.StatusNotFound, "user_not_found", "未找到该用户")
	default:
		log.Printf("friend operation: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
	}
}

func parseFriendRequestID(c *gin.Context) (uint64, bool) {
	requestID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || requestID == 0 {
		writeError(c, http.StatusBadRequest, "invalid_request", "好友申请编号不正确")
		return 0, false
	}
	return requestID, true
}

func toPublicUserResponse(user store.PublicUser) publicUserResponse {
	return publicUserResponse{
		Username: user.Username,
		Nickname: user.Nickname,
	}
}

func toFriendRequestResponse(request store.FriendRequest) friendRequestResponse {
	return friendRequestResponse{
		ID:        request.ID,
		User:      toPublicUserResponse(request.User),
		CreatedAt: request.CreatedAt,
	}
}

func toFriendRequestRecordResponse(record store.FriendRequestRecord) friendRequestRecordResponse {
	return friendRequestRecordResponse{
		ID:        record.ID,
		User:      toPublicUserResponse(record.User),
		Direction: string(record.Direction),
		Status:    string(record.Status),
		CreatedAt: record.CreatedAt,
		HandledAt: record.HandledAt,
	}
}
