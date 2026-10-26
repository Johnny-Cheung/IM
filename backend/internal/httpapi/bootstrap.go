package httpapi

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
)

const (
	bootstrapDefaultDirectLimit = 200
	bootstrapMaxDirectLimit     = 200
	bootstrapGroupLimit         = 50
	bootstrapMaxGroupMessages   = 200
	bootstrapMaxGroupCursors    = 200
	bootstrapGroupBudget        = 300 * time.Millisecond
	bootstrapGroupConcurrency   = 3
)

type bootstrapGroupCursor struct {
	GroupID uint64 `json:"group_id"`
	AfterID uint64 `json:"after_id"`
}

type bootstrapRequest struct {
	AfterID uint64                 `json:"after_id"`
	Groups  []bootstrapGroupCursor `json:"groups"`
	Limit   int                    `json:"limit"`
}

type bootstrapDirectResponse struct {
	Messages []messageResponse `json:"messages"`
	HasMore  bool              `json:"has_more"`
	Complete bool              `json:"complete"`
	Cursor   uint64            `json:"cursor"`
}

type bootstrapGroupResponse struct {
	GroupID  uint64                 `json:"group_id"`
	Messages []groupMessageResponse `json:"messages"`
	HasMore  bool                   `json:"has_more"`
	Complete bool                   `json:"complete"`
	Cursor   uint64                 `json:"cursor"`
}

type bootstrapResponse struct {
	User          publicUserResponse       `json:"user"`
	Conversations []conversationResponse   `json:"conversations"`
	Friends       []publicUserResponse     `json:"friends"`
	Direct        bootstrapDirectResponse  `json:"direct"`
	Groups        []bootstrapGroupResponse `json:"groups"`
	Partial       []string                 `json:"partial"`
}

type bootstrapGroupResult struct {
	groupID  uint64
	afterID  uint64
	response bootstrapGroupResponse
	ok       bool
}

// bootstrap returns everything the browser needs for its first render in one
// request: the current user, conversation and friend lists, the direct-message
// delta and deltas for groups whose cached cursor is behind.
func (a *API) bootstrap(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	var request bootstrapRequest
	if err := c.ShouldBindJSON(&request); err != nil && !errors.Is(err, io.EOF) {
		writeError(c, http.StatusBadRequest, "invalid_request", "同步参数不正确")
		return
	}
	if len(request.Groups) > bootstrapMaxGroupCursors {
		writeError(c, http.StatusBadRequest, "invalid_request", "群同步游标过多")
		return
	}
	limit := request.Limit
	if limit <= 0 {
		limit = bootstrapDefaultDirectLimit
	}
	if limit > bootstrapMaxDirectLimit {
		limit = bootstrapMaxDirectLimit
	}

	response := bootstrapResponse{
		User: publicUserResponse{
			Username: current.Username,
			Nickname: current.Nickname,
		},
		Conversations: []conversationResponse{},
		Friends:       []publicUserResponse{},
		Direct: bootstrapDirectResponse{
			Messages: []messageResponse{},
			Cursor:   request.AfterID,
		},
		Groups:  []bootstrapGroupResponse{},
		Partial: []string{},
	}

	ctx := c.Request.Context()
	var mu sync.Mutex
	markPartial := func(section string) {
		mu.Lock()
		response.Partial = append(response.Partial, section)
		mu.Unlock()
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		friends, err := a.buildFriends(ctx, current)
		if err != nil {
			log.Printf("bootstrap friends: %v", err)
			markPartial("friends")
			return
		}
		mu.Lock()
		response.Friends = friends
		mu.Unlock()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		direct, err := a.buildDirectSync(ctx, current, request.AfterID, limit)
		if err != nil {
			log.Printf("bootstrap direct sync: %v", err)
			markPartial("direct")
			return
		}
		mu.Lock()
		response.Direct = bootstrapDirectResponse{
			Messages: direct.Messages,
			HasMore:  direct.HasMore,
			Complete: direct.Complete,
			Cursor:   direct.Cursor,
		}
		mu.Unlock()
	}()

	conversations, conversationsErr := a.buildConversations(ctx, current)
	var planned []bootstrapGroupCursor
	if conversationsErr != nil {
		log.Printf("bootstrap conversations: %v", conversationsErr)
		markPartial("conversations")
	} else {
		mu.Lock()
		response.Conversations = conversations
		mu.Unlock()
		if a.groupReader == nil {
			if len(request.Groups) > 0 {
				markPartial("groups")
			}
		} else {
			planned = planBootstrapGroups(conversations, request.Groups)
		}
	}

	if len(planned) > 0 {
		results, failed := a.readBootstrapGroups(ctx, current, planned)
		if failed {
			markPartial("groups")
		}
		mu.Lock()
		response.Groups = assembleBootstrapGroups(results, bootstrapMaxGroupMessages)
		mu.Unlock()
	}

	wg.Wait()
	sort.Strings(response.Partial)
	c.JSON(http.StatusOK, response)
}

func (a *API) readBootstrapGroups(
	ctx context.Context,
	current store.User,
	planned []bootstrapGroupCursor,
) ([]bootstrapGroupResult, bool) {
	groupCtx, cancel := context.WithTimeout(ctx, bootstrapGroupBudget)
	defer cancel()

	results := make([]bootstrapGroupResult, len(planned))
	var wg sync.WaitGroup
	var failed atomic.Bool
	sem := make(chan struct{}, bootstrapGroupConcurrency)
	for index, cursor := range planned {
		results[index] = bootstrapGroupResult{
			groupID: cursor.GroupID,
			afterID: cursor.AfterID,
		}
		wg.Add(1)
		go func(index int, cursor bootstrapGroupCursor) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-groupCtx.Done():
				failed.Store(true)
				return
			}
			delta, err := a.readGroupDelta(
				groupCtx, current, cursor.GroupID, cursor.AfterID, bootstrapGroupLimit,
			)
			if err != nil {
				log.Printf("bootstrap group %d: %v", cursor.GroupID, err)
				failed.Store(true)
				return
			}
			results[index].response = delta
			results[index].ok = true
		}(index, cursor)
	}
	wg.Wait()
	return results, failed.Load()
}

func (a *API) readGroupDelta(
	ctx context.Context,
	current store.User,
	groupID, afterID uint64,
	limit int,
) (bootstrapGroupResponse, error) {
	if a.groupReader == nil {
		return bootstrapGroupResponse{}, errMessageServiceUnavailable
	}
	hot, err := a.groupReader.ReadGroupAfter(ctx, current.ID, groupID, afterID, limit)
	if err != nil {
		return bootstrapGroupResponse{}, err
	}

	response := bootstrapGroupResponse{
		GroupID:  groupID,
		Messages: make([]groupMessageResponse, 0, len(hot.Page.Messages)),
		HasMore:  hot.Page.HasMore,
		Complete: hot.Complete,
		Cursor:   afterID,
	}
	for _, message := range hot.Page.Messages {
		response.Messages = append(response.Messages, toGroupMessageResponse(message))
		if message.ID > response.Cursor {
			response.Cursor = message.ID
		}
	}
	return response, nil
}

// planBootstrapGroups keeps only groups that the user is currently in (proven
// by the conversation list) and whose last message is newer than the cached
// cursor. Groups without a cursor are opened lazily and skipped here.
func planBootstrapGroups(
	conversations []conversationResponse,
	cursors []bootstrapGroupCursor,
) []bootstrapGroupCursor {
	cursorByGroup := make(map[uint64]uint64, len(cursors))
	for _, cursor := range cursors {
		if cursor.GroupID == 0 || cursor.AfterID == 0 {
			continue
		}
		if existing, ok := cursorByGroup[cursor.GroupID]; !ok || cursor.AfterID > existing {
			cursorByGroup[cursor.GroupID] = cursor.AfterID
		}
	}

	plan := make([]bootstrapGroupCursor, 0, len(cursorByGroup))
	seen := make(map[uint64]struct{}, len(cursorByGroup))
	for _, conversation := range conversations {
		if conversation.Type != "group" || conversation.Group == nil ||
			conversation.LastMessage == nil {
			continue
		}
		groupID := conversation.Group.ID
		cursor, ok := cursorByGroup[groupID]
		if !ok {
			continue
		}
		if _, duplicate := seen[groupID]; duplicate {
			continue
		}
		if conversation.LastMessage.ID <= cursor {
			continue
		}
		seen[groupID] = struct{}{}
		plan = append(plan, bootstrapGroupCursor{GroupID: groupID, AfterID: cursor})
	}
	return plan
}

func assembleBootstrapGroups(
	results []bootstrapGroupResult,
	maxMessages int,
) []bootstrapGroupResponse {
	response := make([]bootstrapGroupResponse, 0, len(results))
	remaining := maxMessages
	for _, result := range results {
		if result.groupID == 0 || !result.ok {
			continue
		}
		item := result.response
		if remaining <= 0 {
			response = append(response, bootstrapGroupResponse{
				GroupID:  item.GroupID,
				Messages: []groupMessageResponse{},
				HasMore:  true,
				Complete: item.Complete,
				Cursor:   result.afterID,
			})
			continue
		}
		if len(item.Messages) > remaining {
			item.Messages = item.Messages[:remaining]
			item.HasMore = true
			if len(item.Messages) == 0 {
				item.Cursor = result.afterID
			} else {
				item.Cursor = item.Messages[len(item.Messages)-1].ID
			}
		}
		remaining -= len(item.Messages)
		response = append(response, item)
	}
	return response
}
