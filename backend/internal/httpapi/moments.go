package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

const (
	momentMaxBodyRunes        = 1000
	momentCommentMaxRunes     = 500
	momentDefaultPageSize     = 20
	momentMaxPageSize         = 50
	momentDefaultCommentLimit = 20
	momentMaxCommentLimit     = 100
	momentDetailCommentLimit  = 100
	momentDefaultLikerLimit   = 50
	momentMaxLikerLimit       = 100
	momentPreviewCommentCount = 2
)

// MomentStore is the persistence used by the Moments feature. It is kept
// separate from Store so unrelated HTTP tests do not have to implement it.
type MomentStore interface {
	ListFriendIDs(ctx context.Context, userID uint64) ([]uint64, error)
	CreateMoment(
		ctx context.Context,
		authorID uint64,
		body string,
		visibility store.MomentVisibility,
		now time.Time,
	) (store.Moment, error)
	ListMoments(
		ctx context.Context,
		viewerID uint64,
		friendIDs []uint64,
		beforeID uint64,
		limit int,
	) ([]store.Moment, bool, error)
	GetMoment(ctx context.Context, viewerID, momentID uint64) (store.Moment, error)
	MomentCommentPreviews(
		ctx context.Context,
		momentIDs []uint64,
		perMoment int,
	) (map[uint64][]store.MomentComment, error)
	DeleteMoment(ctx context.Context, momentID, authorID uint64, now time.Time) error
	LikeMoment(ctx context.Context, momentID, userID uint64, now time.Time) (uint64, error)
	UnlikeMoment(ctx context.Context, momentID, userID uint64) (uint64, error)
	ListMomentLikes(
		ctx context.Context,
		momentID, beforeID uint64,
		limit int,
	) ([]store.MomentLike, bool, error)
	ListMomentComments(
		ctx context.Context,
		momentID, beforeID uint64,
		limit int,
	) ([]store.MomentComment, bool, error)
	CreateMomentComment(
		ctx context.Context,
		momentID, authorID uint64,
		body string,
		now time.Time,
	) (store.MomentComment, error)
	DeleteMomentComment(
		ctx context.Context,
		momentID, commentID, userID uint64,
		now time.Time,
	) error
	MomentUnread(
		ctx context.Context,
		viewerID uint64,
		friendIDs []uint64,
	) (uint64, bool, error)
	MarkMomentsSeen(
		ctx context.Context,
		viewerID, lastSeenID uint64,
		now time.Time,
	) (uint64, error)
}

// WithMomentStore enables the Moments HTTP endpoints.
func WithMomentStore(momentStore MomentStore) Option {
	return func(api *API) {
		api.moments = momentStore
	}
}

type momentCommentResponse struct {
	ID        uint64             `json:"id"`
	Author    publicUserResponse `json:"author"`
	Body      string             `json:"body"`
	CreatedAt time.Time          `json:"created_at"`
}

type momentResponse struct {
	ID              uint64                  `json:"id"`
	Author          publicUserResponse      `json:"author"`
	Body            string                  `json:"body"`
	Visibility      string                  `json:"visibility"`
	CreatedAt       time.Time               `json:"created_at"`
	LikeCount       uint64                  `json:"like_count"`
	CommentCount    uint64                  `json:"comment_count"`
	LikedByMe       bool                    `json:"liked_by_me"`
	Mine            bool                    `json:"mine"`
	CommentsPreview []momentCommentResponse `json:"comments_preview"`
}

type momentFeedResponse struct {
	Moments      []momentResponse `json:"moments"`
	HasMore      bool             `json:"has_more"`
	NextBeforeID uint64           `json:"next_before_id"`
}

type momentLikeResponse struct {
	ID        uint64             `json:"id"`
	User      publicUserResponse `json:"user"`
	CreatedAt time.Time          `json:"created_at"`
}

type momentDetailResponse struct {
	Moment          momentResponse          `json:"moment"`
	Comments        []momentCommentResponse `json:"comments"`
	CommentsHasMore bool                    `json:"comments_has_more"`
	Likers          []momentLikeResponse    `json:"likers"`
}

type momentCommentPageResponse struct {
	Comments     []momentCommentResponse `json:"comments"`
	HasMore      bool                    `json:"has_more"`
	NextBeforeID uint64                  `json:"next_before_id"`
}

type momentLikePageResponse struct {
	Likers       []momentLikeResponse `json:"likers"`
	HasMore      bool                 `json:"has_more"`
	NextBeforeID uint64               `json:"next_before_id"`
}

type momentLikeResultResponse struct {
	LikeCount uint64 `json:"like_count"`
	LikedByMe bool   `json:"liked_by_me"`
}

type momentUnreadResponse struct {
	HasNew     bool   `json:"has_new"`
	LastSeenID uint64 `json:"last_seen_id"`
}

type momentSeenResponse struct {
	LastSeenID uint64 `json:"last_seen_id"`
}

type createMomentRequest struct {
	Body       string `json:"body"`
	Visibility string `json:"visibility"`
}

type createMomentCommentRequest struct {
	Body string `json:"body"`
}

type momentSeenRequest struct {
	LastSeenID uint64 `json:"last_seen_id"`
}

func (a *API) requireMomentStore(c *gin.Context) bool {
	if a.moments == nil {
		writeError(c, http.StatusServiceUnavailable, "message_service_unavailable", "朋友圈服务暂不可用")
		return false
	}
	return true
}

func (a *API) createMoment(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if !a.requireMomentStore(c) {
		return
	}

	var request createMomentRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "请求格式不正确")
		return
	}
	body, ok := cleanMomentBody(request.Body, momentMaxBodyRunes)
	if !ok {
		writeError(c, http.StatusBadRequest, "invalid_moment_body", "动态内容需为 1–1000 字")
		return
	}
	visibility, ok := parseMomentVisibility(request.Visibility)
	if !ok {
		writeError(c, http.StatusBadRequest, "invalid_visibility", "可见范围不正确")
		return
	}

	ctx := c.Request.Context()
	moment, err := a.moments.CreateMoment(ctx, current.ID, body, visibility, time.Now().UTC())
	if err != nil {
		log.Printf("create moment user=%d: %v", current.ID, err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	moment.Author = store.PublicUser{Username: current.Username, Nickname: current.Nickname}
	moment.Mine = true

	if visibility == store.MomentVisibilityFriends {
		friendIDs, friendErr := a.moments.ListFriendIDs(ctx, current.ID)
		if friendErr != nil {
			log.Printf("list moment broadcast friends user=%d: %v", current.ID, friendErr)
		} else {
			a.publishMomentCreated(friendIDs, moment, current)
		}
	}

	c.JSON(http.StatusCreated, toMomentResponse(moment, nil))
}

func (a *API) listMoments(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if !a.requireMomentStore(c) {
		return
	}
	beforeID, err := parseUintQuery(c.Query("before_id"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_moment_cursor", "分页参数不正确")
		return
	}
	limit, err := parseMomentLimit(c.Query("limit"), momentDefaultPageSize, momentMaxPageSize)
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_moment_cursor", "分页参数不正确")
		return
	}

	ctx := c.Request.Context()
	friendIDs, err := a.moments.ListFriendIDs(ctx, current.ID)
	if err != nil {
		log.Printf("list moment friends user=%d: %v", current.ID, err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	moments, hasMore, err := a.moments.ListMoments(ctx, current.ID, friendIDs, beforeID, limit)
	if err != nil {
		log.Printf("list moments user=%d: %v", current.ID, err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	ids := make([]uint64, 0, len(moments))
	for _, moment := range moments {
		ids = append(ids, moment.ID)
	}
	previews, err := a.moments.MomentCommentPreviews(ctx, ids, momentPreviewCommentCount)
	if err != nil {
		log.Printf("list moment previews user=%d: %v", current.ID, err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	response := momentFeedResponse{
		Moments: make([]momentResponse, 0, len(moments)),
		HasMore: hasMore,
	}
	for _, moment := range moments {
		response.Moments = append(response.Moments, toMomentResponse(moment, previews[moment.ID]))
	}
	if len(moments) > 0 {
		response.NextBeforeID = moments[len(moments)-1].ID
	}
	c.JSON(http.StatusOK, response)
}

func (a *API) getMoment(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if !a.requireMomentStore(c) {
		return
	}
	momentID, ok := parseMomentID(c, "id")
	if !ok {
		return
	}

	ctx := c.Request.Context()
	moment, err := a.moments.GetMoment(ctx, current.ID, momentID)
	if err != nil {
		a.writeMomentError(c, err)
		return
	}
	visible, err := a.canViewMoment(ctx, current.ID, moment)
	if err != nil {
		log.Printf("check moment visibility moment=%d user=%d: %v", momentID, current.ID, err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	if !visible {
		writeError(c, http.StatusForbidden, "not_friend", "无权查看这条动态")
		return
	}

	comments, commentsHasMore, err := a.moments.ListMomentComments(
		ctx, momentID, 0, momentDetailCommentLimit,
	)
	if err != nil {
		log.Printf("list moment detail comments moment=%d: %v", momentID, err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	likes, _, err := a.moments.ListMomentLikes(ctx, momentID, 0, momentDefaultLikerLimit)
	if err != nil {
		log.Printf("list moment detail likes moment=%d: %v", momentID, err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}

	c.JSON(http.StatusOK, momentDetailResponse{
		Moment:          toMomentResponse(moment, nil),
		Comments:        toMomentCommentResponses(comments),
		CommentsHasMore: commentsHasMore,
		Likers:          toMomentLikeResponses(likes),
	})
}

func (a *API) deleteMoment(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if !a.requireMomentStore(c) {
		return
	}
	momentID, ok := parseMomentID(c, "id")
	if !ok {
		return
	}
	if err := a.moments.DeleteMoment(
		c.Request.Context(), momentID, current.ID, time.Now().UTC(),
	); err != nil {
		a.writeMomentError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (a *API) likeMoment(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if !a.requireMomentStore(c) {
		return
	}
	momentID, ok := parseMomentID(c, "id")
	if !ok {
		return
	}
	if _, ok := a.requireVisibleMoment(c, current.ID, momentID); !ok {
		return
	}
	count, err := a.moments.LikeMoment(
		c.Request.Context(), momentID, current.ID, time.Now().UTC(),
	)
	if err != nil {
		a.writeMomentError(c, err)
		return
	}
	c.JSON(http.StatusOK, momentLikeResultResponse{LikeCount: count, LikedByMe: true})
}

func (a *API) unlikeMoment(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if !a.requireMomentStore(c) {
		return
	}
	momentID, ok := parseMomentID(c, "id")
	if !ok {
		return
	}
	if _, ok := a.requireVisibleMoment(c, current.ID, momentID); !ok {
		return
	}
	count, err := a.moments.UnlikeMoment(c.Request.Context(), momentID, current.ID)
	if err != nil {
		a.writeMomentError(c, err)
		return
	}
	c.JSON(http.StatusOK, momentLikeResultResponse{LikeCount: count, LikedByMe: false})
}

func (a *API) listMomentLikes(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if !a.requireMomentStore(c) {
		return
	}
	momentID, ok := parseMomentID(c, "id")
	if !ok {
		return
	}
	if _, ok := a.requireVisibleMoment(c, current.ID, momentID); !ok {
		return
	}
	beforeID, err := parseUintQuery(c.Query("before_id"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_moment_cursor", "分页参数不正确")
		return
	}
	limit, err := parseMomentLimit(c.Query("limit"), momentDefaultLikerLimit, momentMaxLikerLimit)
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_moment_cursor", "分页参数不正确")
		return
	}
	likes, hasMore, err := a.moments.ListMomentLikes(
		c.Request.Context(), momentID, beforeID, limit,
	)
	if err != nil {
		log.Printf("list moment likes moment=%d: %v", momentID, err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	response := momentLikePageResponse{
		Likers:  toMomentLikeResponses(likes),
		HasMore: hasMore,
	}
	if len(likes) > 0 {
		response.NextBeforeID = likes[len(likes)-1].ID
	}
	c.JSON(http.StatusOK, response)
}

func (a *API) listMomentComments(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if !a.requireMomentStore(c) {
		return
	}
	momentID, ok := parseMomentID(c, "id")
	if !ok {
		return
	}
	if _, ok := a.requireVisibleMoment(c, current.ID, momentID); !ok {
		return
	}
	beforeID, err := parseUintQuery(c.Query("before_id"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_moment_cursor", "分页参数不正确")
		return
	}
	limit, err := parseMomentLimit(c.Query("limit"), momentDefaultCommentLimit, momentMaxCommentLimit)
	if err != nil {
		writeError(c, http.StatusBadRequest, "invalid_moment_cursor", "分页参数不正确")
		return
	}
	comments, hasMore, err := a.moments.ListMomentComments(
		c.Request.Context(), momentID, beforeID, limit,
	)
	if err != nil {
		log.Printf("list moment comments moment=%d: %v", momentID, err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	response := momentCommentPageResponse{
		Comments: toMomentCommentResponses(comments),
		HasMore:  hasMore,
	}
	if len(comments) > 0 {
		response.NextBeforeID = comments[0].ID
	}
	c.JSON(http.StatusOK, response)
}

func (a *API) createMomentComment(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if !a.requireMomentStore(c) {
		return
	}
	momentID, ok := parseMomentID(c, "id")
	if !ok {
		return
	}
	if _, ok := a.requireVisibleMoment(c, current.ID, momentID); !ok {
		return
	}

	var request createMomentCommentRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", "请求格式不正确")
		return
	}
	body, ok := cleanMomentBody(request.Body, momentCommentMaxRunes)
	if !ok {
		writeError(c, http.StatusBadRequest, "invalid_moment_body", "评论内容需为 1–500 字")
		return
	}
	comment, err := a.moments.CreateMomentComment(
		c.Request.Context(), momentID, current.ID, body, time.Now().UTC(),
	)
	if err != nil {
		a.writeMomentError(c, err)
		return
	}
	comment.Author = store.PublicUser{Username: current.Username, Nickname: current.Nickname}
	c.JSON(http.StatusCreated, toMomentCommentResponse(comment))
}

func (a *API) deleteMomentComment(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if !a.requireMomentStore(c) {
		return
	}
	momentID, ok := parseMomentID(c, "id")
	if !ok {
		return
	}
	commentID, err := strconv.ParseUint(c.Param("commentId"), 10, 64)
	if err != nil || commentID == 0 {
		writeError(c, http.StatusBadRequest, "invalid_request", "评论参数不正确")
		return
	}
	if err := a.moments.DeleteMomentComment(
		c.Request.Context(), momentID, commentID, current.ID, time.Now().UTC(),
	); err != nil {
		a.writeMomentError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (a *API) momentUnread(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if !a.requireMomentStore(c) {
		return
	}
	ctx := c.Request.Context()
	friendIDs, err := a.moments.ListFriendIDs(ctx, current.ID)
	if err != nil {
		log.Printf("list moment unread friends user=%d: %v", current.ID, err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	lastSeenID, hasNew, err := a.moments.MomentUnread(ctx, current.ID, friendIDs)
	if err != nil {
		log.Printf("read moment unread user=%d: %v", current.ID, err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	c.JSON(http.StatusOK, momentUnreadResponse{HasNew: hasNew, LastSeenID: lastSeenID})
}

func (a *API) momentSeen(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	if !a.requireMomentStore(c) {
		return
	}
	var request momentSeenRequest
	if err := c.ShouldBindJSON(&request); err != nil && !errors.Is(err, io.EOF) {
		writeError(c, http.StatusBadRequest, "invalid_request", "请求格式不正确")
		return
	}
	lastSeenID, err := a.moments.MarkMomentsSeen(
		c.Request.Context(), current.ID, request.LastSeenID, time.Now().UTC(),
	)
	if err != nil {
		log.Printf("mark moments seen user=%d: %v", current.ID, err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return
	}
	c.JSON(http.StatusOK, momentSeenResponse{LastSeenID: lastSeenID})
}

// requireVisibleMoment loads a moment and applies the author/friend rule.
func (a *API) requireVisibleMoment(
	c *gin.Context,
	viewerID, momentID uint64,
) (store.Moment, bool) {
	moment, err := a.moments.GetMoment(c.Request.Context(), viewerID, momentID)
	if err != nil {
		a.writeMomentError(c, err)
		return store.Moment{}, false
	}
	visible, err := a.canViewMoment(c.Request.Context(), viewerID, moment)
	if err != nil {
		log.Printf("check moment visibility moment=%d user=%d: %v", momentID, viewerID, err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
		return store.Moment{}, false
	}
	if !visible {
		writeError(c, http.StatusForbidden, "not_friend", "无权查看这条动态")
		return store.Moment{}, false
	}
	return moment, true
}

func (a *API) canViewMoment(
	ctx context.Context,
	viewerID uint64,
	moment store.Moment,
) (bool, error) {
	if moment.AuthorID == viewerID {
		return true, nil
	}
	if moment.Visibility != store.MomentVisibilityFriends {
		return false, nil
	}
	relationship, err := a.store.GetRelationship(ctx, viewerID, moment.AuthorID)
	if err != nil {
		return false, err
	}
	return relationship == store.RelationshipFriend, nil
}

func (a *API) writeMomentError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, store.ErrMomentNotFound):
		writeError(c, http.StatusNotFound, "moment_not_found", "动态不存在")
	case errors.Is(err, store.ErrMomentCommentNotFound):
		writeError(c, http.StatusNotFound, "comment_not_found", "评论不存在")
	case errors.Is(err, store.ErrMomentForbidden):
		writeError(c, http.StatusForbidden, "forbidden", "无权执行这个操作")
	default:
		log.Printf("moment operation: %v", err)
		writeError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误")
	}
}

func (a *API) publishMomentCreated(
	friendIDs []uint64,
	moment store.Moment,
	author store.User,
) {
	if len(friendIDs) == 0 {
		return
	}
	payload, err := json.Marshal(struct {
		Type     string             `json:"type"`
		MomentID uint64             `json:"moment_id"`
		Author   publicUserResponse `json:"author"`
	}{
		Type:     "moment.created",
		MomentID: moment.ID,
		Author: publicUserResponse{
			Username: author.Username,
			Nickname: author.Nickname,
		},
	})
	if err != nil {
		log.Printf("marshal moment.created: %v", err)
		return
	}
	a.hub.Broadcast(friendIDs, payload)
}

func toMomentResponse(
	moment store.Moment,
	comments []store.MomentComment,
) momentResponse {
	return momentResponse{
		ID: moment.ID,
		Author: publicUserResponse{
			Username: moment.Author.Username,
			Nickname: moment.Author.Nickname,
		},
		Body:            moment.Body,
		Visibility:      string(moment.Visibility),
		CreatedAt:       moment.CreatedAt,
		LikeCount:       moment.LikeCount,
		CommentCount:    moment.CommentCount,
		LikedByMe:       moment.LikedByMe,
		Mine:            moment.Mine,
		CommentsPreview: toMomentCommentResponses(comments),
	}
}

func toMomentCommentResponses(comments []store.MomentComment) []momentCommentResponse {
	response := make([]momentCommentResponse, 0, len(comments))
	for _, comment := range comments {
		response = append(response, toMomentCommentResponse(comment))
	}
	return response
}

func toMomentCommentResponse(comment store.MomentComment) momentCommentResponse {
	return momentCommentResponse{
		ID: comment.ID,
		Author: publicUserResponse{
			Username: comment.Author.Username,
			Nickname: comment.Author.Nickname,
		},
		Body:      comment.Body,
		CreatedAt: comment.CreatedAt,
	}
}

func toMomentLikeResponses(likes []store.MomentLike) []momentLikeResponse {
	response := make([]momentLikeResponse, 0, len(likes))
	for _, like := range likes {
		response = append(response, momentLikeResponse{
			ID: like.ID,
			User: publicUserResponse{
				Username: like.User.Username,
				Nickname: like.User.Nickname,
			},
			CreatedAt: like.CreatedAt,
		})
	}
	return response
}

func cleanMomentBody(raw string, maxRunes int) (string, bool) {
	body := strings.TrimSpace(raw)
	if body == "" || strings.ContainsRune(body, '\x00') {
		return "", false
	}
	if utf8.RuneCountInString(body) > maxRunes {
		return "", false
	}
	return body, true
}

func parseMomentVisibility(raw string) (store.MomentVisibility, bool) {
	if strings.TrimSpace(raw) == "" {
		return store.MomentVisibilityFriends, true
	}
	visibility := store.MomentVisibility(strings.ToLower(strings.TrimSpace(raw)))
	if !visibility.Valid() {
		return "", false
	}
	return visibility, true
}

func parseMomentID(c *gin.Context, param string) (uint64, bool) {
	momentID, err := strconv.ParseUint(c.Param(param), 10, 64)
	if err != nil || momentID == 0 {
		writeError(c, http.StatusBadRequest, "invalid_request", "动态参数不正确")
		return 0, false
	}
	return momentID, true
}

func parseMomentLimit(raw string, fallback, maximum int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > maximum {
		return 0, errors.New("invalid moment limit")
	}
	return value, nil
}
