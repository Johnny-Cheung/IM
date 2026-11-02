package httpapi

import (
	"log"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

type bootstrapResponse struct {
	User          publicUserResponse     `json:"user"`
	Conversations []conversationResponse `json:"conversations"`
	Friends       []publicUserResponse   `json:"friends"`
	Partial       []string               `json:"partial"`
}

// bootstrap returns the first-screen metadata: the current user, the complete
// conversation list and the friend list. Message bodies are loaded lazily when
// a conversation is opened.
func (a *API) bootstrap(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}

	response := bootstrapResponse{
		User: publicUserResponse{
			Username: current.Username,
			Nickname: current.Nickname,
		},
		Conversations: []conversationResponse{},
		Friends:       []publicUserResponse{},
		Partial:       []string{},
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
		conversations, err := a.buildConversations(ctx, current)
		if err != nil {
			log.Printf("bootstrap conversations: %v", err)
			markPartial("conversations")
			return
		}
		mu.Lock()
		response.Conversations = conversations
		mu.Unlock()
	}()

	wg.Wait()
	c.JSON(http.StatusOK, response)
}
