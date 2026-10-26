package httpapi

import (
	"errors"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
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

type conversationMessageResponse struct {
	ID              uint64              `json:"id"`
	GroupID         uint64              `json:"group_id,omitempty"`
	Sender          publicUserResponse  `json:"sender"`
	Recipient       *publicUserResponse `json:"recipient,omitempty"`
	Body            string              `json:"body"`
	ClientMessageID string              `json:"client_message_id"`
	CreatedAt       time.Time           `json:"created_at"`
}

type conversationResponse struct {
	Type                  string                       `json:"type"`
	User                  *publicUserResponse          `json:"user,omitempty"`
	IsFriend              bool                         `json:"is_friend"`
	Group                 *groupSummaryResponse        `json:"group,omitempty"`
	IsOwner               bool                         `json:"is_owner"`
	LastMessage           *conversationMessageResponse `json:"last_message"`
	UnreadCount           int                          `json:"unread_count"`
	PeerLastReadMessageID uint64                       `json:"peer_last_read_message_id,omitempty"`
}

type conversationListResponse struct {
	Conversations []conversationResponse `json:"conversations"`
}

type messagePageResponse struct {
	Messages []messageResponse `json:"messages"`
	HasMore  bool              `json:"has_more"`
}

type syncMessagePageResponse struct {
	Messages []messageResponse `json:"messages"`
	HasMore  bool              `json:"has_more"`
	Complete bool              `json:"complete"`
}

type sendMessageRequest struct {
	Body            string `json:"body"`
	ClientMessageID string `json:"client_message_id"`
}

type sendMessageResponse struct {
	Message messageResponse `json:"message"`
}

type readStateResponse struct {
	LastReadMessageID uint64 `json:"last_read_message_id"`
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
	if a.directReader != nil {
		hotMessages, hotErr := a.directReader.LastConversationMessages(c.Request.Context(), current.ID)
		if hotErr != nil {
			log.Printf("read hot conversation messages: %v", hotErr)
		} else {
			for index := range conversations {
				message, ok := hotMessages[conversations[index].UserID]
				if !ok {
					continue
				}
				if conversations[index].LastMessage == nil || message.ID > conversations[index].LastMessage.ID {
					messageCopy := message
					conversations[index].LastMessage = &messageCopy
				}
			}
		}
	}
	groups, err := a.store.ListGroupConversations(c.Request.Context(), current.ID)
	if err != nil {
		log.Printf("list group conversations: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	response := conversationListResponse{
		Conversations: make([]conversationResponse, 0, len(conversations)+len(groups)),
	}
	for _, conversation := range conversations {
		other := publicUserResponse{
			Username: conversation.User.Username,
			Nickname: conversation.User.Nickname,
		}
		item := conversationResponse{
			Type:                  "direct",
			User:                  &other,
			IsFriend:              conversation.IsFriend,
			UnreadCount:           conversation.UnreadCount,
			PeerLastReadMessageID: conversation.PeerLastReadMessageID,
		}
		if conversation.LastMessage != nil {
			message := toConversationMessage(toMessageResponse(*conversation.LastMessage, current, other))
			item.LastMessage = &message
		}
		response.Conversations = append(response.Conversations, item)
	}
	for _, conversation := range groups {
		summary := toGroupSummaryResponse(conversation.Group, conversation.MemberCount)
		item := conversationResponse{
			Type:        "group",
			Group:       &summary,
			IsOwner:     conversation.IsOwner,
			UnreadCount: conversation.UnreadCount,
		}
		if conversation.LastMessage != nil {
			message := toConversationMessageFromGroup(toGroupMessageResponse(*conversation.LastMessage))
			item.LastMessage = &message
		}
		response.Conversations = append(response.Conversations, item)
	}

	sortConversations(response.Conversations)
	c.JSON(http.StatusOK, response)
}

func sortConversations(conversations []conversationResponse) {
	sort.SliceStable(conversations, func(i, j int) bool {
		left, right := conversations[i], conversations[j]
		leftAt, leftHas := conversationLastMessageTime(left)
		rightAt, rightHas := conversationLastMessageTime(right)
		switch {
		case leftHas && rightHas:
			if !leftAt.Equal(rightAt) {
				return leftAt.After(rightAt)
			}
			return conversationTieKey(left) < conversationTieKey(right)
		case leftHas:
			return true
		case rightHas:
			return false
		}

		leftDirect := left.Type == "direct"
		rightDirect := right.Type == "direct"
		if leftDirect != rightDirect {
			return leftDirect
		}
		if leftDirect {
			leftTitle := conversationTieKey(left)
			rightTitle := conversationTieKey(right)
			if leftTitle != rightTitle {
				return leftTitle < rightTitle
			}
			return left.User.Username < right.User.Username
		}
		if left.Group.CreatedAt.Equal(right.Group.CreatedAt) {
			return left.Group.ID < right.Group.ID
		}
		return left.Group.CreatedAt.After(right.Group.CreatedAt)
	})
}

func conversationLastMessageTime(item conversationResponse) (time.Time, bool) {
	if item.LastMessage == nil {
		return time.Time{}, false
	}
	return item.LastMessage.CreatedAt, true
}

func toConversationMessage(message messageResponse) conversationMessageResponse {
	recipient := message.Recipient
	return conversationMessageResponse{
		ID:              message.ID,
		Sender:          message.Sender,
		Recipient:       &recipient,
		Body:            message.Body,
		ClientMessageID: message.ClientMessageID,
		CreatedAt:       message.CreatedAt,
	}
}

func toConversationMessageFromGroup(message groupMessageResponse) conversationMessageResponse {
	return conversationMessageResponse{
		ID:              message.ID,
		GroupID:         message.GroupID,
		Sender:          message.Sender,
		Body:            message.Body,
		ClientMessageID: message.ClientMessageID,
		CreatedAt:       message.CreatedAt,
	}
}

func conversationTieKey(item conversationResponse) string {
	if item.User != nil {
		nickname := strings.ToLower(item.User.Nickname)
		if nickname != "" {
			return nickname
		}
		return strings.ToLower(item.User.Username)
	}
	if item.Group != nil {
		return strings.ToLower(item.Group.Name)
	}
	return ""
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
	afterID, err := parseAfterID(c.Query("after_id"))
	if err != nil || (beforeID > 0 && afterID > 0) {
		writeError(c, http.StatusBadRequest, "invalid_request", "消息分页参数不正确")
		return
	}
	limit, err := parseMessageLimit(c.Query("limit"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "消息分页参数不正确")
		return
	}

	var page store.MessagePage
	servedFromRedis := false
	var hotPage store.MessagePage
	hotAvailable := false
	if a.directReader != nil {
		var hot messaging.ConversationPage
		if afterID > 0 {
			hot, err = a.directReader.ReadConversationAfter(
				c.Request.Context(),
				current.ID,
				target.ID,
				afterID,
				limit,
			)
		} else {
			hot, err = a.directReader.ReadConversation(
				c.Request.Context(),
				current.ID,
				target.ID,
				beforeID,
				limit,
			)
		}
		if err != nil {
			log.Printf("read hot messages: %v", err)
		} else {
			hotPage = hot.Page
			hotAvailable = true
			if hot.Complete {
				page = hot.Page
				servedFromRedis = true
			}
		}
	}

	if !servedFromRedis {
		if afterID > 0 {
			page, err = a.store.ListMessagesAfter(
				c.Request.Context(),
				current.ID,
				target.ID,
				afterID,
				limit,
			)
		} else {
			page, err = a.store.ListMessages(
				c.Request.Context(),
				current.ID,
				target.ID,
				beforeID,
				limit,
			)
		}
		if err != nil {
			log.Printf("list messages: %v", err)
			writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
			return
		}
		if hotAvailable {
			page = mergeMessagePages(page, hotPage, beforeID > 0 || afterID == 0, limit)
		}
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

func mergeMessagePages(primary, hot store.MessagePage, beforeDirection bool, limit int) store.MessagePage {
	byID := make(map[uint64]store.Message, len(primary.Messages)+len(hot.Messages))
	for _, message := range primary.Messages {
		byID[message.ID] = message
	}
	for _, message := range hot.Messages {
		if _, exists := byID[message.ID]; !exists {
			byID[message.ID] = message
		}
	}

	messages := make([]store.Message, 0, len(byID))
	for _, message := range byID {
		messages = append(messages, message)
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].ID < messages[j].ID })

	hasMore := primary.HasMore || hot.HasMore || len(messages) > limit
	if len(messages) > limit {
		if beforeDirection {
			messages = messages[len(messages)-limit:]
		} else {
			messages = messages[:limit]
		}
	}
	return store.MessagePage{Messages: messages, HasMore: hasMore}
}

// syncMessages returns every direct message after the cursor across all
// conversations. It backs client-side incremental sync.
func (a *API) syncMessages(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	afterID, err := parseAfterID(c.Query("after_id"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "同步游标不正确")
		return
	}
	limit, err := parseMessageLimit(c.Query("limit"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "消息分页参数不正确")
		return
	}
	if a.directReader == nil {
		writeError(c, http.StatusServiceUnavailable, "message_service_unavailable", "消息服务暂不可用")
		return
	}

	feed, err := a.directReader.ReadFeed(c.Request.Context(), current.ID, afterID, limit)
	if err != nil {
		log.Printf("read message feed: %v", err)
		writeError(c, http.StatusServiceUnavailable, "message_service_unavailable", "消息服务暂不可用")
		return
	}

	peerIDs := make(map[uint64]struct{}, len(feed.Page.Messages))
	for _, message := range feed.Page.Messages {
		if message.SenderID == current.ID {
			peerIDs[message.RecipientID] = struct{}{}
			continue
		}
		peerIDs[message.SenderID] = struct{}{}
	}
	ids := make([]uint64, 0, len(peerIDs))
	for id := range peerIDs {
		ids = append(ids, id)
	}
	peers, err := a.store.ListPublicUsersByIDs(c.Request.Context(), ids)
	if err != nil {
		log.Printf("list sync participants: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	response := syncMessagePageResponse{
		Messages: make([]messageResponse, 0, len(feed.Page.Messages)),
		HasMore:  feed.Page.HasMore,
		Complete: feed.Complete,
	}
	for _, message := range feed.Page.Messages {
		peerID := message.RecipientID
		if message.SenderID != current.ID {
			peerID = message.SenderID
		}
		peer := peers[peerID]
		response.Messages = append(response.Messages, toMessageResponse(
			message,
			current,
			publicUserResponse{Username: peer.Username, Nickname: peer.Nickname},
		))
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

	body, clientMessageID, ok := parseMessagePayload(c)
	if !ok {
		return
	}

	var message store.Message
	var created bool
	var err error
	if a.directAcceptor != nil {
		result, acceptErr := a.directAcceptor.Accept(
			c.Request.Context(),
			messaging.DirectMessageRequest{
				SenderID:        current.ID,
				RecipientID:     target.ID,
				ClientMessageID: clientMessageID,
				Body:            body,
				CreatedAt:       time.Now().UTC(),
			},
		)
		message = result.Message
		created = result.Created
		err = acceptErr
	} else {
		message, created, err = a.store.CreateMessage(
			c.Request.Context(),
			current.ID,
			target.ID,
			clientMessageID,
			body,
			time.Now().UTC(),
		)
	}
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

func parseMessagePayload(c *gin.Context) (string, string, bool) {
	var request sendMessageRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_message", "消息格式不正确")
		return "", "", false
	}
	body := strings.TrimSpace(request.Body)
	if body == "" || strings.ContainsRune(body, '\x00') {
		writeError(c, http.StatusBadRequest, "invalid_message", "消息不能为空")
		return "", "", false
	}
	if utf8.RuneCountInString(body) > maxMessageRunes {
		writeError(c, http.StatusBadRequest, "message_too_long", "消息不能超过 2000 个字符")
		return "", "", false
	}
	if !clientMessageIDPattern.MatchString(request.ClientMessageID) {
		writeError(c, http.StatusBadRequest, "invalid_client_message_id", "消息标识不正确")
		return "", "", false
	}
	return body, request.ClientMessageID, true
}

func (a *API) markConversationRead(c *gin.Context) {
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
		writeError(c, http.StatusBadRequest, "cannot_message_self", "不能给自己标记已读")
		return
	}

	throughID, ok := parseReadThroughID(c)
	if !ok {
		return
	}
	lastReadMessageID, advanced, err := a.store.MarkConversationRead(
		c.Request.Context(),
		current.ID,
		target.ID,
		throughID,
		time.Now().UTC(),
	)
	if err != nil {
		a.writeMessageError(c, err)
		return
	}
	if advanced {
		a.PublishRead(
			current.ID,
			target.ID,
			current.Username,
			target.Username,
			lastReadMessageID,
		)
	}

	c.JSON(http.StatusOK, readStateResponse{LastReadMessageID: lastReadMessageID})
}

func (a *API) writeMessageError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, store.ErrNotFriends):
		writeError(c, http.StatusForbidden, "not_friends", "你们已不是好友，不能发送新消息")
	case errors.Is(err, store.ErrCannotMessageSelf):
		writeError(c, http.StatusBadRequest, "cannot_message_self", "不能给自己发送消息")
	case errors.Is(err, store.ErrUserNotFound):
		writeError(c, http.StatusNotFound, "user_not_found", "未找到该用户")
	case errors.Is(err, store.ErrMessageConflict):
		writeError(c, http.StatusConflict, "client_message_id_conflict", "消息标识对应的内容不一致")
	case errors.Is(err, store.ErrMessageIDExhausted):
		writeError(c, http.StatusServiceUnavailable, "message_id_exhausted", "消息服务暂时不可用")
	case errors.Is(err, messaging.ErrMessageServiceUnavailable):
		writeError(c, http.StatusServiceUnavailable, "message_service_unavailable", "消息服务暂时不可用")
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

func parseAfterID(raw string) (uint64, error) {
	return parseBeforeID(raw)
}

func parseReadThroughID(c *gin.Context) (uint64, bool) {
	throughID, err := parseBeforeID(c.Query("through_id"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "已读消息位置不正确")
		return 0, false
	}
	return throughID, true
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
