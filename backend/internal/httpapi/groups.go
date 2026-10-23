package httpapi

import (
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Johnny-Cheung/IM/backend/internal/auth"
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
	if memberIDs, err := a.store.ListGroupMemberIDs(
		c.Request.Context(), detail.Group.ID,
	); err != nil {
		log.Printf("list created group members: %v", err)
	} else {
		a.PublishGroupChanged(
			excludeUserID(memberIDs, current.ID),
			detail.Group.ID,
			"added",
		)
	}
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

	existingIDs, existingErr := a.store.ListGroupMemberIDs(c.Request.Context(), groupID)
	if err := a.store.InviteGroupMembers(
		c.Request.Context(),
		groupID,
		current.ID,
		memberIDs,
		time.Now().UTC(),
	); err != nil {
		a.writeGroupError(c, err)
		return
	}
	detail, err := a.store.GetGroup(c.Request.Context(), current.ID, groupID)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}
	if allIDs, err := a.store.ListGroupMemberIDs(
		c.Request.Context(), groupID,
	); err != nil {
		log.Printf("list invited group members: %v", err)
	} else if existingErr != nil {
		a.PublishGroupChanged(allIDs, groupID, "members_changed")
	} else {
		existing := userIDSet(existingIDs)
		newMemberIDs := make([]uint64, 0, len(allIDs))
		for _, memberID := range allIDs {
			if _, ok := existing[memberID]; !ok {
				newMemberIDs = append(newMemberIDs, memberID)
			}
		}
		if len(newMemberIDs) > 0 {
			a.PublishGroupChanged(newMemberIDs, groupID, "added")
		}
		if len(existingIDs) > 0 {
			a.PublishGroupChanged(existingIDs, groupID, "members_changed")
		}
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

	if err := a.store.KickGroupMember(
		c.Request.Context(), current.ID, groupID, target.ID,
	); err != nil {
		a.writeGroupError(c, err)
		return
	}
	detail, err := a.store.GetGroup(c.Request.Context(), current.ID, groupID)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}
	if remainingIDs, err := a.store.ListGroupMemberIDs(
		c.Request.Context(), groupID,
	); err != nil {
		log.Printf("list group members after kick: %v", err)
	} else {
		a.PublishGroupChanged(remainingIDs, groupID, "members_changed")
	}
	a.PublishGroupChanged([]uint64{target.ID}, groupID, "removed")
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

	memberIDs, memberErr := a.store.ListGroupMemberIDs(c.Request.Context(), groupID)
	dissolved, err := a.store.LeaveGroup(
		c.Request.Context(), current.ID, groupID, time.Now().UTC(),
	)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}
	if dissolved {
		if memberErr != nil || len(memberIDs) == 0 {
			memberIDs = []uint64{current.ID}
		}
		a.PublishGroupChanged(memberIDs, groupID, "dissolved")
	} else if remainingIDs, err := a.store.ListGroupMemberIDs(
		c.Request.Context(), groupID,
	); err != nil {
		log.Printf("list group members after leave: %v", err)
	} else {
		a.PublishGroupChanged(remainingIDs, groupID, "members_changed")
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

	message, created, err := a.store.CreateGroupMessage(
		c.Request.Context(),
		current.ID,
		groupID,
		clientMessageID,
		body,
		time.Now().UTC(),
	)
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

	lastReadMessageID, _, err := a.store.MarkGroupRead(
		c.Request.Context(), current.ID, groupID,
	)
	if err != nil {
		a.writeGroupError(c, err)
		return
	}
	c.JSON(http.StatusOK, readStateResponse{LastReadMessageID: lastReadMessageID})
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
