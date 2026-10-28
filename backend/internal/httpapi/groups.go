package httpapi

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Johnny-Cheung/IM/backend/internal/auth"
	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
)

const maxGroupNameRunes = 30

type groupSummaryResponse struct {
	ID          uint64             `json:"id"`
	Name        string             `json:"name"`
	Owner       publicUserResponse `json:"owner"`
	MemberCount int                `json:"member_count"`
	CreatedAt   time.Time          `json:"created_at"`
}

type groupMemberResponse struct {
	User     publicUserResponse `json:"user"`
	IsOwner  bool               `json:"is_owner"`
	JoinedAt time.Time          `json:"joined_at"`
}

type groupDetailResponse struct {
	Group   groupSummaryResponse  `json:"group"`
	Members []groupMemberResponse `json:"members"`
	IsOwner bool                  `json:"is_owner"`
}

type groupMessageResponse struct {
	ID              uint64             `json:"id"`
	GroupID         uint64             `json:"group_id"`
	Sender          publicUserResponse `json:"sender"`
	Body            string             `json:"body"`
	ClientMessageID string             `json:"client_message_id"`
	CreatedAt       time.Time          `json:"created_at"`
}

type groupMessagePageResponse struct {
	Messages []groupMessageResponse `json:"messages"`
	HasMore  bool                   `json:"has_more"`
}

type sendGroupMessageResponse struct {
	Message groupMessageResponse `json:"message"`
}

type createGroupRequest struct {
	Name            string   `json:"name"`
	MemberUsernames []string `json:"member_usernames"`
}

type inviteGroupMembersRequest struct {
	Usernames []string `json:"usernames"`
}

type renameGroupRequest struct {
	Name string `json:"name"`
}

type leaveGroupResponse struct {
	Dissolved bool `json:"dissolved"`
}

func (a *API) createGroup(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	var request createGroupRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "群聊信息格式不正确")
		return
	}
	name := strings.TrimSpace(request.Name)
	if !validGroupName(name) {
		writeError(c, http.StatusBadRequest, "invalid_group_name", "群名需要 1–30 个字符")
		return
	}

	memberIDs, ok := a.resolveGroupMemberIDs(c, current, request.MemberUsernames)
	if !ok {
		return
	}

	detail, err := a.store.CreateGroup(
		c.Request.Context(),
		current.ID,
		name,
		memberIDs,
		time.Now().UTC(),
	)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}

	release := a.groupWrites.acquire(detail.Group.ID)
	defer release()
	syncCtx, cancel := groupCommitContext(c.Request.Context())
	defer cancel()
	createdIDs, listErr := a.store.ListGroupMemberIDs(syncCtx, detail.Group.ID)
	if listErr != nil {
		log.Printf("list created group members: %v", listErr)
		createdIDs = append([]uint64{current.ID}, memberIDs...)
	}
	if a.groupReads != nil {
		for _, memberID := range createdIDs {
			if err := a.groupReads.SeedMember(
				syncCtx, detail.Group.ID, memberID, 0,
			); err != nil {
				log.Printf("seed created group %d member %d read: %v",
					detail.Group.ID, memberID, err)
				a.groupRepairs.schedule(detail.Group.ID)
				break
			}
		}
	}
	if a.groupProjector != nil {
		if err := a.groupProjector.Replace(
			syncCtx, detail.Group.ID, createdIDs, false,
		); err != nil {
			log.Printf("project created group %d: %v", detail.Group.ID, err)
			a.groupRepairs.schedule(detail.Group.ID)
		}
	}
	a.PublishGroupChanged(
		excludeUserID(createdIDs, current.ID),
		detail.Group.ID,
		"added",
	)
	c.JSON(http.StatusCreated, toGroupDetailResponse(detail))
}

func (a *API) getGroup(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	groupID, ok := parseGroupID(c)
	if !ok {
		return
	}

	detail, err := a.store.GetGroup(c.Request.Context(), current.ID, groupID)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}
	c.JSON(http.StatusOK, toGroupDetailResponse(detail))
}

func (a *API) renameGroup(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	groupID, ok := parseGroupID(c)
	if !ok {
		return
	}

	var request renameGroupRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "群名格式不正确")
		return
	}
	name := strings.TrimSpace(request.Name)
	if !validGroupName(name) {
		writeError(c, http.StatusBadRequest, "invalid_group_name", "群名需要 1–30 个字符")
		return
	}

	if err := a.store.RenameGroup(
		c.Request.Context(), current.ID, groupID, name,
	); err != nil {
		a.writeGroupError(c, err)
		return
	}
	detail, err := a.store.GetGroup(c.Request.Context(), current.ID, groupID)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}
	if memberIDs, err := a.store.ListGroupMemberIDs(
		c.Request.Context(), groupID,
	); err != nil {
		log.Printf("list renamed group members: %v", err)
	} else {
		a.PublishGroupChanged(memberIDs, groupID, "renamed")
	}
	c.JSON(http.StatusOK, toGroupDetailResponse(detail))
}

func (a *API) inviteGroupMembers(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	groupID, ok := parseGroupID(c)
	if !ok {
		return
	}

	var request inviteGroupMembersRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "邀请信息格式不正确")
		return
	}
	memberIDs, ok := a.resolveGroupMemberIDs(c, current, request.Usernames)
	if !ok {
		return
	}

	ctx := c.Request.Context()
	member, err := a.store.IsGroupMember(ctx, current.ID, groupID)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}
	if !member {
		// Authorize before BeginMutation so a non-member cannot fail-close the
		// group by reaching the sync flag.
		a.writeGroupError(c, store.ErrGroupNotFound)
		return
	}
	existingIDs, existingErr := a.store.ListGroupMemberIDs(ctx, groupID)
	if existingErr != nil {
		log.Printf("list group members before invite: %v", existingErr)
	}

	mutation, err := a.beginGroupMutation(ctx, groupID)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}
	defer mutation.finish()

	baselineFloor := uint64(0)
	if a.groupWatermarkReader != nil {
		// Read after BeginMutation: a send racing with the invite is already
		// rejected, so it cannot slip below the new members' baseline.
		accepted, err := a.groupWatermarkReader.LastAcceptedID(ctx, groupID)
		if err != nil {
			log.Printf("read group accepted watermark for invite: %v", err)
			a.writeGroupError(c, messaging.ErrMessageServiceUnavailable)
			return
		}
		baselineFloor = accepted
	}

	baseline, err := a.store.InviteGroupMembersWithBaseline(
		ctx,
		groupID,
		current.ID,
		memberIDs,
		time.Now().UTC(),
		baselineFloor,
	)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}

	syncCtx, cancel := groupCommitContext(ctx)
	defer cancel()

	detail, detailErr := a.store.GetGroup(syncCtx, current.ID, groupID)
	allIDs, listErr := a.store.ListGroupMemberIDs(syncCtx, groupID)
	if listErr != nil {
		log.Printf("list invited group members: %v", listErr)
		allIDs = nil
	}

	seedIDs := make([]uint64, 0, len(memberIDs))
	if allIDs != nil && existingErr == nil {
		existing := userIDSet(existingIDs)
		for _, memberID := range allIDs {
			if _, ok := existing[memberID]; !ok {
				seedIDs = append(seedIDs, memberID)
			}
		}
	}
	var seedErr error
	if a.groupReads != nil {
		for _, memberID := range seedIDs {
			if err := a.groupReads.SetMemberBaseline(
				syncCtx, groupID, memberID, baseline,
			); err != nil {
				log.Printf("seed invited group %d member %d read: %v",
					groupID, memberID, err)
				seedErr = messaging.ErrMessageServiceUnavailable
				break
			}
		}
		if seedErr != nil || (allIDs != nil && existingErr != nil) {
			// The repair loop sets positions from MySQL, which also covers a
			// retry whose baseline was already consumed.
			mutation.queueRepair()
		}
	}
	mutation.project(syncCtx, allIDs, false)

	if existingErr == nil {
		if len(seedIDs) > 0 {
			a.PublishGroupChanged(seedIDs, groupID, "added")
		}
		if len(existingIDs) > 0 {
			a.PublishGroupChanged(existingIDs, groupID, "members_changed")
		}
	} else {
		a.PublishGroupChanged(memberIDs, groupID, "added")
	}

	if detailErr != nil {
		a.writeGroupError(c, detailErr)
		return
	}
	if seedErr != nil {
		a.writeGroupError(c, seedErr)
		return
	}
	c.JSON(http.StatusOK, toGroupDetailResponse(detail))
}

func (a *API) kickGroupMember(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	groupID, ok := parseGroupID(c)
	if !ok {
		return
	}
	target, ok := a.findUser(c, c.Param("username"))
	if !ok {
		return
	}

	detail, err := a.store.GetGroup(c.Request.Context(), current.ID, groupID)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}
	if !detail.IsOwner {
		// Authorize before BeginMutation so a non-owner cannot fail-close the
		// group by reaching the sync flag.
		a.writeGroupError(c, store.ErrNotGroupOwner)
		return
	}

	mutation, err := a.beginGroupMutation(c.Request.Context(), groupID)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}
	defer mutation.finish()
	if err := a.store.KickGroupMember(
		c.Request.Context(), current.ID, groupID, target.ID,
	); err != nil {
		a.writeGroupError(c, err)
		return
	}

	syncCtx, cancel := groupCommitContext(c.Request.Context())
	defer cancel()
	detail, detailErr := a.store.GetGroup(syncCtx, current.ID, groupID)
	remainingIDs, remainingErr := a.store.ListGroupMemberIDs(syncCtx, groupID)
	if remainingErr != nil {
		log.Printf("list group members after kick: %v", remainingErr)
		remainingIDs = nil
	}
	if a.groupReads != nil {
		if err := a.groupReads.RemoveMember(
			syncCtx, groupID, target.ID,
		); err != nil {
			log.Printf("remove kicked group %d member %d read: %v",
				groupID, target.ID, err)
		}
	}
	// The member-set rewrite also prunes the kicked member's read position and
	// is retried in the background when it fails.
	mutation.project(syncCtx, remainingIDs, false)
	if remainingIDs != nil {
		a.PublishGroupChanged(remainingIDs, groupID, "members_changed")
	}
	a.PublishGroupChanged([]uint64{target.ID}, groupID, "removed")
	if detailErr != nil {
		a.writeGroupError(c, detailErr)
		return
	}
	c.JSON(http.StatusOK, toGroupDetailResponse(detail))
}

func (a *API) leaveGroup(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	groupID, ok := parseGroupID(c)
	if !ok {
		return
	}

	member, err := a.store.IsGroupMember(c.Request.Context(), current.ID, groupID)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}
	if !member {
		// Authorize before BeginMutation so a non-member cannot fail-close the
		// group by reaching the sync flag.
		a.writeGroupError(c, store.ErrGroupNotFound)
		return
	}

	mutation, err := a.beginGroupMutation(c.Request.Context(), groupID)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}
	defer mutation.finish()
	memberIDs, memberErr := a.store.ListGroupMemberIDs(c.Request.Context(), groupID)
	dissolved, err := a.store.LeaveGroup(
		c.Request.Context(), current.ID, groupID, time.Now().UTC(),
	)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}

	syncCtx, cancel := groupCommitContext(c.Request.Context())
	defer cancel()
	if dissolved {
		if memberErr != nil || len(memberIDs) == 0 {
			memberIDs = []uint64{current.ID}
		}
		mutation.project(syncCtx, memberIDs, true)
		a.PublishGroupChanged(memberIDs, groupID, "dissolved")
	} else {
		remainingIDs, remainingErr := a.store.ListGroupMemberIDs(syncCtx, groupID)
		if remainingErr != nil {
			log.Printf("list group members after leave: %v", remainingErr)
			remainingIDs = nil
		}
		if a.groupReads != nil {
			if err := a.groupReads.RemoveMember(
				syncCtx, groupID, current.ID,
			); err != nil {
				log.Printf("remove left group %d member %d read: %v",
					groupID, current.ID, err)
			}
		}
		// The member-set rewrite also prunes the leaver's read position and is
		// retried in the background when it fails.
		mutation.project(syncCtx, remainingIDs, false)
		if remainingIDs != nil {
			a.PublishGroupChanged(remainingIDs, groupID, "members_changed")
		}
	}
	c.JSON(http.StatusOK, leaveGroupResponse{Dissolved: dissolved})
}

func (a *API) listGroupMessages(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	groupID, ok := parseGroupID(c)
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

	var page store.GroupMessagePage
	servedFromRedis := false
	var hotPage store.GroupMessagePage
	hotAvailable := false
	if a.groupReader != nil {
		member, memberErr := a.store.IsGroupMember(c.Request.Context(), current.ID, groupID)
		if memberErr != nil {
			log.Printf("check group membership: %v", memberErr)
		} else if member {
			var hot messaging.GroupConversationPage
			if afterID > 0 {
				hot, err = a.groupReader.ReadGroupAfter(
					c.Request.Context(), current.ID, groupID, afterID, limit,
				)
			} else {
				hot, err = a.groupReader.ReadGroup(
					c.Request.Context(), current.ID, groupID, beforeID, limit,
				)
			}
			if err != nil {
				log.Printf("list group hot messages: %v", err)
			} else {
				hotPage = hot.Page
				hotAvailable = true
				if hot.Complete {
					page = hot.Page
					servedFromRedis = true
				}
			}
		}
	}

	if !servedFromRedis {
		if afterID > 0 {
			page, err = a.store.ListGroupMessagesAfter(
				c.Request.Context(),
				current.ID,
				groupID,
				afterID,
				limit,
			)
		} else {
			page, err = a.store.ListGroupMessages(
				c.Request.Context(),
				current.ID,
				groupID,
				beforeID,
				limit,
			)
		}
		if err != nil {
			a.writeGroupError(c, err)
			return
		}
		if hotAvailable {
			page = mergeGroupMessagePages(page, hotPage, beforeID > 0 || afterID == 0, limit)
		}
	}

	response := groupMessagePageResponse{
		Messages: make([]groupMessageResponse, 0, len(page.Messages)),
		HasMore:  page.HasMore,
	}
	for _, message := range page.Messages {
		response.Messages = append(response.Messages, toGroupMessageResponse(message))
	}
	c.JSON(http.StatusOK, response)
}

func (a *API) sendGroupMessage(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	groupID, ok := parseGroupID(c)
	if !ok {
		return
	}
	body, clientMessageID, ok := parseMessagePayload(c)
	if !ok {
		return
	}

	var message store.GroupMessage
	var created bool
	var err error
	if a.groupAcceptor != nil {
		result, acceptErr := a.groupAcceptor.Accept(c.Request.Context(), messaging.GroupMessageRequest{
			SenderID:        current.ID,
			GroupID:         groupID,
			ClientMessageID: clientMessageID,
			Body:            body,
			CreatedAt:       time.Now().UTC(),
			SenderUsername:  current.Username,
			SenderNickname:  current.Nickname,
		})
		message, created, err = result.Message, result.Created, acceptErr
	} else {
		message, created, err = a.store.CreateGroupMessage(
			c.Request.Context(),
			current.ID,
			groupID,
			clientMessageID,
			body,
			time.Now().UTC(),
		)
	}
	if err != nil {
		a.writeGroupError(c, err)
		return
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	response := toGroupMessageResponse(message)
	if created {
		if memberIDs, err := a.store.ListGroupMemberIDs(
			c.Request.Context(), groupID,
		); err != nil {
			log.Printf("list group message recipients: %v", err)
		} else {
			a.PublishGroupMessage(memberIDs, response)
			a.scheduleGroupImplicitRead(groupID, current.ID, message.ID, memberIDs)
		}
	}
	c.JSON(status, sendGroupMessageResponse{Message: response})
}

func (a *API) markGroupRead(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	groupID, ok := parseGroupID(c)
	if !ok {
		return
	}

	throughID, ok := parseReadThroughID(c)
	if !ok {
		return
	}
	if a.groupReads != nil {
		if !a.ensureGroupReadModelReady(c) {
			return
		}
		result, err := a.groupReads.AdvanceRead(
			c.Request.Context(),
			groupID,
			current.ID,
			throughID,
			time.Now().UTC(),
		)
		if err != nil {
			a.writeGroupError(c, err)
			return
		}
		if result.Advanced {
			logGroupReadStreamError(result, groupID)
			a.publishGroupReadFromRedis(c.Request.Context(), groupID, map[uint64]struct{}{
				current.ID: {},
			})
		}
		c.JSON(http.StatusOK, readStateResponse{
			LastReadMessageID: result.LastReadMessageID,
		})
		return
	}

	lastReadMessageID, _, err := a.markGroupReadThrough(
		c.Request.Context(), current.ID, groupID, throughID,
	)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}
	c.JSON(http.StatusOK, readStateResponse{LastReadMessageID: lastReadMessageID})
}

// publishGroupReadFromRedis broadcasts a group read signal from the Redis
// member set, keeping the read path off MySQL.
func (a *API) publishGroupReadFromRedis(
	ctx context.Context,
	groupID uint64,
	excluded map[uint64]struct{},
) {
	if a.groupReads == nil {
		return
	}
	memberIDs, err := a.groupReads.MemberIDs(ctx, groupID)
	if err != nil {
		log.Printf("list group read broadcast members group=%d: %v", groupID, err)
		return
	}
	a.publishGroupRead(memberIDs, groupID, excluded)
}

func (a *API) ensureGroupReadModelReady(c *gin.Context) bool {
	if a.groupReads == nil {
		writeError(c, http.StatusServiceUnavailable, "message_service_unavailable", "消息服务暂时不可用")
		return false
	}
	ready, err := a.groupReads.ReadModelReady(c.Request.Context())
	if err != nil {
		log.Printf("check group read model: %v", err)
		writeError(c, http.StatusServiceUnavailable, "message_service_unavailable", "消息服务暂时不可用")
		return false
	}
	if !ready {
		writeError(c, http.StatusServiceUnavailable, "read_model_not_ready", "已读服务尚未就绪")
		return false
	}
	return true
}

type groupReadStatsResponse struct {
	MessageID   uint64 `json:"message_id"`
	ReadCount   int    `json:"read_count"`
	MemberCount int    `json:"member_count"`
	AllRead     bool   `json:"all_read"`
}

func (a *API) groupReadStats(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	groupID, ok := parseGroupID(c)
	if !ok {
		return
	}
	messageID, err := parseReadStatsMessageID(c.Query("message_id"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "已读消息参数不正确")
		return
	}
	if !a.ensureGroupReadModelReady(c) {
		return
	}

	ctx := c.Request.Context()
	// Check membership before the message lookup: a non-member must get the
	// same answer whether or not the message exists.
	stats, err := a.groupReads.ReadStats(ctx, groupID, current.ID, messageID)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}

	senderID, found, err := a.groupReads.MessageSender(ctx, groupID, messageID)
	if err != nil {
		log.Printf("load group message sender group=%d message=%d: %v",
			groupID, messageID, err)
		writeError(c, http.StatusServiceUnavailable, "message_service_unavailable", "消息服务暂时不可用")
		return
	}
	if !found {
		senderID, found, err = a.store.GroupMessageSender(ctx, groupID, messageID)
		if err != nil {
			log.Printf("load persisted group message sender group=%d message=%d: %v",
				groupID, messageID, err)
			writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
			return
		}
		if !found {
			writeError(c, http.StatusNotFound, "message_not_found", "消息不存在")
			return
		}
	}
	if senderID != current.ID {
		writeError(c, http.StatusForbidden, "not_your_message", "只能查看自己消息的已读情况")
		return
	}

	c.JSON(http.StatusOK, groupReadStatsResponse{
		MessageID:   messageID,
		ReadCount:   stats.ReadCount,
		MemberCount: stats.MemberCount,
		AllRead:     stats.AllRead,
	})
}

func parseReadStatsMessageID(raw string) (uint64, error) {
	if raw == "" {
		return 0, errors.New("missing message id")
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		return 0, errors.New("invalid message id")
	}
	return value, nil
}

// logGroupReadStreamError surfaces a lost MySQL persistence event; the Redis
// position is still authoritative, so the advance itself is kept.
func logGroupReadStreamError(advance messaging.GroupReadAdvance, groupID uint64) {
	if !advance.StreamError {
		return
	}
	log.Printf("group read stream append failed group=%d user=%d position=%d; MySQL persistence is delayed",
		groupID, advance.UserID, advance.LastReadMessageID)
}

func (a *API) markGroupReadThrough(
	ctx context.Context,
	userID, groupID, throughID uint64,
) (uint64, bool, error) {
	target := throughID
	if a.groupWatermarkReader != nil {
		accepted, err := a.groupWatermarkReader.LastAcceptedID(ctx, groupID)
		if err != nil {
			log.Printf("read group accepted watermark: %v", err)
		} else if accepted > 0 {
			if target == 0 || target > accepted {
				target = accepted
			}
			if advancer, ok := a.store.(interface {
				AdvanceGroupReadThrough(context.Context, uint64, uint64, uint64) (uint64, bool, error)
			}); ok {
				return advancer.AdvanceGroupReadThrough(ctx, userID, groupID, target)
			}
		}
	}
	return a.store.MarkGroupRead(ctx, userID, groupID, target)
}

func (a *API) resolveGroupMemberIDs(
	c *gin.Context,
	current store.User,
	usernames []string,
) ([]uint64, bool) {
	seen := make(map[uint64]struct{}, len(usernames))
	memberIDs := make([]uint64, 0, len(usernames))
	for _, username := range usernames {
		usernameKey := auth.UsernameKey(strings.TrimSpace(username))
		if usernameKey == "" {
			writeError(c, http.StatusBadRequest, "invalid_members", "请选择要邀请的好友")
			return nil, false
		}
		user, err := a.store.GetUserByUsernameKey(c.Request.Context(), usernameKey)
		if errors.Is(err, store.ErrUserNotFound) {
			writeError(c, http.StatusNotFound, "user_not_found", "未找到该用户")
			return nil, false
		}
		if err != nil {
			log.Printf("resolve group member: %v", err)
			writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
			return nil, false
		}
		if user.ID == current.ID {
			writeError(c, http.StatusBadRequest, "invalid_members", "不能邀请自己")
			return nil, false
		}
		if _, ok := seen[user.ID]; ok {
			continue
		}
		seen[user.ID] = struct{}{}
		memberIDs = append(memberIDs, user.ID)
	}
	if len(memberIDs) == 0 {
		writeError(c, http.StatusBadRequest, "invalid_members", "请选择要邀请的好友")
		return nil, false
	}
	return memberIDs, true
}

func (a *API) writeGroupError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, store.ErrMessageConflict):
		writeError(c, http.StatusConflict, "client_message_id_conflict", "消息标识对应的内容不一致")
	case errors.Is(err, store.ErrMessageIDExhausted):
		writeError(c, http.StatusServiceUnavailable, "message_id_exhausted", "消息服务暂时不可用")
	case errors.Is(err, messaging.ErrMessageServiceUnavailable):
		writeError(c, http.StatusServiceUnavailable, "message_service_unavailable", "消息服务暂时不可用")
	case errors.Is(err, messaging.ErrGroupNotMember), errors.Is(err, messaging.ErrGroupDissolved):
		writeError(c, http.StatusNotFound, "group_not_found", "群聊不存在或你不在群中")
	case errors.Is(err, store.ErrGroupNotFound):
		writeError(c, http.StatusNotFound, "group_not_found", "群聊不存在或你不在群中")
	case errors.Is(err, store.ErrNotGroupOwner):
		writeError(c, http.StatusForbidden, "not_owner", "只有群主可以执行该操作")
	case errors.Is(err, store.ErrGroupFull):
		writeError(c, http.StatusConflict, "group_full", "群成员已达到上限")
	case errors.Is(err, store.ErrGroupMemberNotFound):
		writeError(c, http.StatusNotFound, "member_not_found", "该用户不在群中")
	case errors.Is(err, store.ErrCannotKickSelf):
		writeError(c, http.StatusBadRequest, "cannot_kick_self", "群主不能踢自己")
	case errors.Is(err, store.ErrInvalidGroupMembers):
		writeError(c, http.StatusBadRequest, "invalid_members", "请选择要邀请的好友")
	case errors.Is(err, store.ErrNotFriends):
		writeError(c, http.StatusForbidden, "not_friends", "只能邀请自己的好友")
	default:
		log.Printf("group operation: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
	}
}

func parseGroupID(c *gin.Context) (uint64, bool) {
	groupID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || groupID == 0 {
		writeError(c, http.StatusBadRequest, "invalid_request", "群聊参数不正确")
		return 0, false
	}
	return groupID, true
}

func validGroupName(name string) bool {
	if name == "" || strings.ContainsRune(name, '\x00') {
		return false
	}
	count := utf8.RuneCountInString(name)
	return count >= 1 && count <= maxGroupNameRunes
}

func mergeGroupMessagePages(
	base, hot store.GroupMessagePage,
	keepNewest bool,
	limit int,
) store.GroupMessagePage {
	byID := make(map[uint64]store.GroupMessage)
	for _, message := range append(append([]store.GroupMessage{}, base.Messages...), hot.Messages...) {
		byID[message.ID] = message
	}
	messages := make([]store.GroupMessage, 0, len(byID))
	for _, message := range byID {
		messages = append(messages, message)
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].ID < messages[j].ID })
	hasMore := base.HasMore || hot.HasMore || len(messages) > limit
	if len(messages) > limit {
		if keepNewest {
			messages = messages[len(messages)-limit:]
		} else {
			messages = messages[:limit]
		}
	}
	return store.GroupMessagePage{Messages: messages, HasMore: hasMore}
}

func toGroupSummaryResponse(group store.Group, memberCount int) groupSummaryResponse {
	return groupSummaryResponse{
		ID:   group.ID,
		Name: group.Name,
		Owner: publicUserResponse{
			Username: group.Owner.Username,
			Nickname: group.Owner.Nickname,
		},
		MemberCount: memberCount,
		CreatedAt:   group.CreatedAt,
	}
}

func toGroupDetailResponse(detail store.GroupDetail) groupDetailResponse {
	members := make([]groupMemberResponse, 0, len(detail.Members))
	for _, member := range detail.Members {
		members = append(members, groupMemberResponse{
			User: publicUserResponse{
				Username: member.User.Username,
				Nickname: member.User.Nickname,
			},
			IsOwner:  member.IsOwner,
			JoinedAt: member.JoinedAt,
		})
	}
	return groupDetailResponse{
		Group:   toGroupSummaryResponse(detail.Group, detail.MemberCount),
		Members: members,
		IsOwner: detail.IsOwner,
	}
}

func toGroupMessageResponse(message store.GroupMessage) groupMessageResponse {
	return groupMessageResponse{
		ID:      message.ID,
		GroupID: message.GroupID,
		Sender: publicUserResponse{
			Username: message.Sender.Username,
			Nickname: message.Sender.Nickname,
		},
		Body:            message.Body,
		ClientMessageID: message.ClientMessageID,
		CreatedAt:       message.CreatedAt,
	}
}

func userIDSet(userIDs []uint64) map[uint64]struct{} {
	set := make(map[uint64]struct{}, len(userIDs))
	for _, userID := range userIDs {
		set[userID] = struct{}{}
	}
	return set
}

func excludeUserID(userIDs []uint64, excluded uint64) []uint64 {
	filtered := make([]uint64, 0, len(userIDs))
	for _, userID := range userIDs {
		if userID != excluded {
			filtered = append(filtered, userID)
		}
	}
	return filtered
}
