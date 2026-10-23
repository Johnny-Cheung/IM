package httpapi

import (
	"errors"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
)

const (
	defaultMessagePageSize = 30
	maxMessagePageSize     = 50
	maxMessageRunes        = 2000
)

var clientMessageIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{8,64}$`)

type messageResponse struct {
	ID              uint64             `json:"id"`
	Sender          publicUserResponse `json:"sender"`
	Recipient       publicUserResponse `json:"recipient"`
	Body            string             `json:"body"`
	ClientMessageID string             `json:"client_message_id"`
	CreatedAt       time.Time          `json:"created_at"`
}

type conversationResponse struct {
	User        publicUserResponse `json:"user"`
	IsFriend    bool               `json:"is_friend"`
	LastMessage *messageResponse   `json:"last_message"`
}

type conversationListResponse struct {
	Conversations []conversationResponse `json:"conversations"`
}

type messagePageResponse struct {
	Messages []messageResponse `json:"messages"`
	HasMore  bool              `json:"has_more"`
}

type sendMessageRequest struct {
	Body            string `json:"body"`
	ClientMessageID string `json:"client_message_id"`
}

type sendMessageResponse struct {
	Message messageResponse `json:"message"`
}

func (a *API) listConversations(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	conversations, err := a.store.ListConversations(c.Request.Context(), current.ID)
	if err != nil {
		log.Printf("list conversations: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	response := conversationListResponse{
		Conversations: make([]conversationResponse, 0, len(conversations)),
	}
	for _, conversation := range conversations {
		other := publicUserResponse{
			Username: conversation.User.Username,
			Nickname: conversation.User.Nickname,
		}
		item := conversationResponse{
			User:     other,
			IsFriend: conversation.IsFriend,
		}
		if conversation.LastMessage != nil {
			message := toMessageResponse(*conversation.LastMessage, current, other)
			item.LastMessage = &message
		}
		response.Conversations = append(response.Conversations, item)
	}

	c.JSON(http.StatusOK, response)
}

func (a *API) listMessages(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	target, ok := a.findUser(c, c.Param("username"))
	if !ok {
		return
	}

	beforeID, err := parseBeforeID(c.Query("before_id"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "消息分页参数不正确")
		return
	}
	limit, err := parseMessageLimit(c.Query("limit"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "消息分页参数不正确")
		return
	}

	page, err := a.store.ListMessages(
		c.Request.Context(),
		current.ID,
		target.ID,
		beforeID,
		limit,
	)
	if err != nil {
		log.Printf("list messages: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	other := publicUserResponse{Username: target.Username, Nickname: target.Nickname}
	response := messagePageResponse{
		Messages: make([]messageResponse, 0, len(page.Messages)),
		HasMore:  page.HasMore,
	}
	for _, message := range page.Messages {
		response.Messages = append(response.Messages, toMessageResponse(message, current, other))
	}
	c.JSON(http.StatusOK, response)
}

func (a *API) sendMessage(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	target, ok := a.findUser(c, c.Param("username"))
	if !ok {
		return
	}
	if target.ID == current.ID {
		writeError(c, http.StatusBadRequest, "cannot_message_self", "不能给自己发送消息")
		return
	}

	var request sendMessageRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_message", "消息格式不正确")
		return
	}

	body := strings.TrimSpace(request.Body)
	if body == "" || strings.ContainsRune(body, '\x00') {
		writeError(c, http.StatusBadRequest, "invalid_message", "消息不能为空")
		return
	}
	if utf8.RuneCountInString(body) > maxMessageRunes {
		writeError(c, http.StatusBadRequest, "message_too_long", "消息不能超过 2000 个字符")
		return
	}
	if !clientMessageIDPattern.MatchString(request.ClientMessageID) {
		writeError(c, http.StatusBadRequest, "invalid_client_message_id", "消息标识不正确")
		return
	}

	message, created, err := a.store.CreateMessage(
		c.Request.Context(),
		current.ID,
		target.ID,
		request.ClientMessageID,
		body,
		time.Now().UTC(),
	)
	if err != nil {
		a.writeMessageError(c, err)
		return
	}

	other := publicUserResponse{Username: target.Username, Nickname: target.Nickname}
	response := toMessageResponse(message, current, other)
	if created {
		a.PublishMessage(current.ID, target.ID, response)
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	c.JSON(status, sendMessageResponse{Message: response})
}

func (a *API) writeMessageError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, store.ErrNotFriends):
		writeError(c, http.StatusForbidden, "not_friends", "你们已不是好友，不能发送新消息")
	case errors.Is(err, store.ErrCannotMessageSelf):
		writeError(c, http.StatusBadRequest, "cannot_message_self", "不能给自己发送消息")
	case errors.Is(err, store.ErrUserNotFound):
		writeError(c, http.StatusNotFound, "user_not_found", "未找到该用户")
	default:
		log.Printf("message operation: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
	}
}

func parseBeforeID(raw string) (uint64, error) {
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func parseMessageLimit(raw string) (int, error) {
	if raw == "" {
		return defaultMessagePageSize, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > maxMessagePageSize {
		return 0, errors.New("invalid message limit")
	}
	return value, nil
}

func toMessageResponse(
	message store.Message,
	current store.User,
	other publicUserResponse,
) messageResponse {
	currentUser := publicUserResponse{
		Username: current.Username,
		Nickname: current.Nickname,
	}
	sender, recipient := currentUser, other
	if message.SenderID != current.ID {
		sender, recipient = other, currentUser
	}
	return messageResponse{
		ID:              message.ID,
		Sender:          sender,
		Recipient:       recipient,
		Body:            message.Body,
		ClientMessageID: message.ClientMessageID,
		CreatedAt:       message.CreatedAt,
	}
}
