package httpapi

import (
	"context"
	"errors"
	"fmt"
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
	defaultSyncPageSize    = 200
	maxSyncPageSize        = 200
	maxMessageRunes        = 2000
)

var clientMessageIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{8,64}$`)

type messageResponse struct {
	ID              uint64             `json:"id"`
	Seq             uint64             `json:"seq"`
	Sender          publicUserResponse `json:"sender"`
	Recipient       publicUserResponse `json:"recipient"`
	Body            string             `json:"body"`
	ClientMessageID string             `json:"client_message_id"`
	CreatedAt       time.Time          `json:"created_at"`
}

type conversationMessageResponse struct {
	ID              uint64              `json:"id"`
	Seq             uint64              `json:"seq"`
	GroupID         uint64              `json:"group_id,omitempty"`
	Sender          publicUserResponse  `json:"sender"`
	Recipient       *publicUserResponse `json:"recipient,omitempty"`
	Body            string              `json:"body"`
	ClientMessageID string              `json:"client_message_id"`
	CreatedAt       time.Time           `json:"created_at"`
}

type conversationResponse struct {
	Type            string                       `json:"type"`
	User            *publicUserResponse          `json:"user,omitempty"`
	IsFriend        bool                         `json:"is_friend"`
	Group           *groupSummaryResponse        `json:"group,omitempty"`
	IsOwner         bool                         `json:"is_owner"`
	LastMessage     *conversationMessageResponse `json:"last_message"`
	UnreadCount     int                          `json:"unread_count"`
	LastSeq         uint64                       `json:"last_seq"`
	PeerLastReadSeq uint64                       `json:"peer_last_read_seq,omitempty"`
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
	Cursor   uint64            `json:"cursor"`
}

var errMessageServiceUnavailable = errors.New("message service unavailable")
var errReadModelNotReady = errors.New("direct read model not ready")
var errGroupReadModelNotReady = errors.New("group read model not ready")

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

	conversations, err := a.buildConversations(c.Request.Context(), current)
	if err != nil {
		switch {
		case errors.Is(err, errReadModelNotReady), errors.Is(err, errGroupReadModelNotReady):
			writeError(c, http.StatusServiceUnavailable, "read_model_not_ready", "已读服务尚未就绪")
		case errors.Is(err, messaging.ErrMessageServiceUnavailable):
			writeError(c, http.StatusServiceUnavailable, "message_service_unavailable", "消息服务暂时不可用")
		default:
			log.Printf("list conversations: %v", err)
			writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		}
		return
	}
	c.JSON(http.StatusOK, conversationListResponse{Conversations: conversations})
}

func (a *API) buildConversations(
	ctx context.Context,
	current store.User,
) ([]conversationResponse, error) {
	var conversations []store.Conversation
	var err error
	if a.directReads != nil {
		conversations, err = a.store.ListConversationBasics(ctx, current.ID)
	} else {
		conversations, err = a.store.ListConversations(ctx, current.ID)
	}
	if err != nil {
		return nil, fmt.Errorf("list direct conversations: %w", err)
	}
	if a.directReads != nil {
		if err := a.applyDirectReadState(ctx, current.ID, conversations); err != nil {
			return nil, fmt.Errorf("apply direct read state: %w", err)
		}
	}
	if a.directReader != nil {
		hotMessages, hotErr := a.directReader.LastConversationMessages(ctx, current.ID)
		if hotErr != nil {
			log.Printf("read hot conversation messages: %v", hotErr)
		} else {
			for index := range conversations {
				message, ok := hotMessages[conversations[index].UserID]
				if !ok {
					continue
				}
				// The counter snapshot above and this preview read are separate
				// calls: a message accepted in between must not leave last_seq
				// behind last_message.seq, or clients treat the list as a
				// server-side rollback and drop it.
				newest := message.Seq
				if current := conversations[index].LastMessage; current != nil && current.Seq > newest {
					newest = current.Seq
				}
				if newest > conversations[index].LastSeq {
					conversations[index].UnreadCount += int(newest - conversations[index].LastSeq)
					conversations[index].LastSeq = newest
				}
				if conversations[index].LastMessage == nil || message.Seq > conversations[index].LastMessage.Seq {
					messageCopy := message
					conversations[index].LastMessage = &messageCopy
				}
			}
		}
	}
	groupConversations, err := a.listGroupConversations(ctx, current.ID)
	if err != nil {
		return nil, fmt.Errorf("list group conversations: %w", err)
	}
	groups := groupConversations
	if a.groupReads != nil {
		if err := a.applyGroupReadState(ctx, current.ID, groups); err != nil {
			return nil, fmt.Errorf("apply group read state: %w", err)
		}
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
			Type:            "direct",
			User:            &other,
			IsFriend:        conversation.IsFriend,
			UnreadCount:     conversation.UnreadCount,
			LastSeq:         conversation.LastSeq,
			PeerLastReadSeq: conversation.PeerLastReadSeq,
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
			LastSeq:     conversation.LastSeq,
		}
		if conversation.LastMessage != nil {
			message := toConversationMessageFromGroup(toGroupMessageResponse(*conversation.LastMessage))
			item.LastMessage = &message
		}
		response.Conversations = append(response.Conversations, item)
	}

	sortConversations(response.Conversations)
	return response.Conversations, nil
}

// applyDirectReadState fills unread counts, the conversation counter and the
// peer read position from one Redis read-model snapshot. It never consults
// MySQL for read state.
func (a *API) applyDirectReadState(
	ctx context.Context,
	userID uint64,
	conversations []store.Conversation,
) error {
	ready, err := a.directReads.ReadModelReady(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", messaging.ErrMessageServiceUnavailable, err)
	}
	if !ready {
		return errReadModelNotReady
	}

	peerIDs := make([]uint64, 0, len(conversations))
	seen := make(map[uint64]struct{}, len(conversations))
	for _, conversation := range conversations {
		if conversation.UserID == 0 {
			continue
		}
		if _, ok := seen[conversation.UserID]; ok {
			continue
		}
		seen[conversation.UserID] = struct{}{}
		peerIDs = append(peerIDs, conversation.UserID)
	}
	if len(peerIDs) == 0 {
		return nil
	}

	states, err := a.directReads.States(ctx, userID, peerIDs)
	if err != nil {
		return fmt.Errorf("%w: %v", messaging.ErrMessageServiceUnavailable, err)
	}
	for index := range conversations {
		state := states[conversations[index].UserID]
		conversations[index].UnreadCount = int(state.Unread)
		// The MySQL-derived MAX(seq) is a floor: a counter that is behind
		// MySQL (lost AOF tail, in-flight calibration) must not make
		// last_seq fall below last_message.seq, which clients read as a
		// server-side rollback.
		if state.LastSeq > conversations[index].LastSeq {
			conversations[index].LastSeq = state.LastSeq
		}
		conversations[index].PeerLastReadSeq = state.PeerReadSeq
	}
	return nil
}

func (a *API) listGroupConversations(
	ctx context.Context,
	userID uint64,
) ([]store.GroupConversation, error) {
	if a.groupReads == nil {
		return a.store.ListGroupConversations(ctx, userID)
	}
	return a.store.ListGroupConversationsWithoutUnread(ctx, userID)
}

// applyGroupReadState fills unread counts and the group counter from one Redis
// read-model snapshot. It never consults MySQL for read state.
func (a *API) applyGroupReadState(
	ctx context.Context,
	userID uint64,
	conversations []store.GroupConversation,
) error {
	ready, err := a.groupReads.ReadModelReady(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", messaging.ErrMessageServiceUnavailable, err)
	}
	if !ready {
		return errGroupReadModelNotReady
	}
	if len(conversations) == 0 {
		return nil
	}

	groupIDs := make([]uint64, 0, len(conversations))
	seen := make(map[uint64]struct{}, len(conversations))
	for _, conversation := range conversations {
		if conversation.Group.ID == 0 {
			continue
		}
		if _, ok := seen[conversation.Group.ID]; ok {
			continue
		}
		seen[conversation.Group.ID] = struct{}{}
		groupIDs = append(groupIDs, conversation.Group.ID)
	}
	if len(groupIDs) == 0 {
		return nil
	}

	states, err := a.groupReads.States(ctx, userID, groupIDs)
	if err != nil {
		return fmt.Errorf("%w: %v", messaging.ErrMessageServiceUnavailable, err)
	}
	for index := range conversations {
		state := states[conversations[index].Group.ID]
		conversations[index].UnreadCount = int(state.Unread)
		// Same floor as private conversations: keep MySQL's MAX(seq) when
		// the group counter is behind it.
		if state.LastSeq > conversations[index].LastSeq {
			conversations[index].LastSeq = state.LastSeq
		}
	}
	return nil
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
		Seq:             message.Seq,
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
		Seq:             message.Seq,
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

	beforeRaw, hasBefore := c.GetQuery("before_seq")
	beforeSeq, err := parseBeforeSeq(beforeRaw)
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "消息分页参数不正确")
		return
	}
	afterRaw, hasAfter := c.GetQuery("after_seq")
	afterSeq, err := parseAfterSeq(afterRaw)
	if err != nil || (hasBefore && hasAfter) {
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
		if hasAfter {
			hot, err = a.directReader.ReadConversationAfter(
				c.Request.Context(),
				current.ID,
				target.ID,
				afterSeq,
				limit,
			)
		} else {
			hot, err = a.directReader.ReadConversation(
				c.Request.Context(),
				current.ID,
				target.ID,
				beforeSeq,
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
		if hasAfter {
			page, err = a.store.ListMessagesAfter(
				c.Request.Context(),
				current.ID,
				target.ID,
				afterSeq,
				limit,
			)
		} else {
			page, err = a.store.ListMessages(
				c.Request.Context(),
				current.ID,
				target.ID,
				beforeSeq,
				limit,
			)
		}
		if err != nil {
			log.Printf("list messages: %v", err)
			writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
			return
		}
		if hotAvailable {
			page = mergeMessagePages(page, hotPage, !hasAfter, limit)
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

// mergeMessagePages merges the persisted page with the Redis page of the same
// seq range. The Redis copy wins, because a message that only exists in Redis
// has not been persisted yet and must stay visible.
func mergeMessagePages(primary, hot store.MessagePage, beforeDirection bool, limit int) store.MessagePage {
	byID := make(map[uint64]store.Message, len(primary.Messages)+len(hot.Messages))
	for _, message := range primary.Messages {
		byID[message.ID] = message
	}
	for _, message := range hot.Messages {
		byID[message.ID] = message
	}

	messages := make([]store.Message, 0, len(byID))
	for _, message := range byID {
		messages = append(messages, message)
	}
	sort.Slice(messages, func(i, j int) bool {
		if messages[i].Seq != messages[j].Seq {
			return messages[i].Seq < messages[j].Seq
		}
		return messages[i].ID < messages[j].ID
	})

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
	afterID, err := parseUintQuery(c.Query("after_id"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "同步游标不正确")
		return
	}
	limit, err := parseSyncLimit(c.Query("limit"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "消息分页参数不正确")
		return
	}

	response, err := a.buildDirectSync(c.Request.Context(), current, afterID, limit)
	if err != nil {
		if errors.Is(err, errMessageServiceUnavailable) {
			log.Printf("read message feed: %v", err)
			writeError(c, http.StatusServiceUnavailable, "message_service_unavailable", "消息服务暂不可用")
			return
		}
		log.Printf("sync messages: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	c.JSON(http.StatusOK, response)
}

func (a *API) buildDirectSync(
	ctx context.Context,
	current store.User,
	afterID uint64,
	limit int,
) (syncMessagePageResponse, error) {
	if a.directReader == nil {
		return syncMessagePageResponse{}, errMessageServiceUnavailable
	}

	feed, err := a.directReader.ReadFeed(ctx, current.ID, afterID, limit)
	if err != nil {
		return syncMessagePageResponse{}, fmt.Errorf(
			"%w: read message feed: %v", errMessageServiceUnavailable, err,
		)
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
	peers, err := a.store.ListPublicUsersByIDs(ctx, ids)
	if err != nil {
		return syncMessagePageResponse{}, fmt.Errorf("list sync participants: %w", err)
	}

	cursor := afterID
	response := syncMessagePageResponse{
		Messages: make([]messageResponse, 0, len(feed.Page.Messages)),
		HasMore:  feed.Page.HasMore,
		Complete: feed.Complete,
	}
	for _, message := range feed.Page.Messages {
		if message.ID > cursor {
			cursor = message.ID
		}
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
	response.Cursor = cursor
	return response, nil
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
		a.scheduleDirectImplicitRead(
			current.ID,
			target.ID,
			current.Username,
			target.Username,
			message.Seq,
		)
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

func parseUintQuery(raw string) (uint64, error) {
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func parseBeforeSeq(raw string) (uint64, error) {
	return parseUintQuery(raw)
}

func parseAfterSeq(raw string) (uint64, error) {
	return parseUintQuery(raw)
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

func parseSyncLimit(raw string) (int, error) {
	if raw == "" {
		return defaultSyncPageSize, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > maxSyncPageSize {
		return 0, errors.New("invalid sync limit")
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
		Seq:             message.Seq,
		Sender:          sender,
		Recipient:       recipient,
		Body:            message.Body,
		ClientMessageID: message.ClientMessageID,
		CreatedAt:       message.CreatedAt,
	}
}
