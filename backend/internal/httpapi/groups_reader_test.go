package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

type groupReaderStub struct {
	page  messaging.GroupConversationPage
	err   error
	calls int
}

func (s *groupReaderStub) ReadGroup(
	_ context.Context, _, _, _ uint64, _ int,
) (messaging.GroupConversationPage, error) {
	s.calls++
	return s.page, s.err
}

func (s *groupReaderStub) ReadGroupAfter(
	_ context.Context, _, _, _ uint64, _ int,
) (messaging.GroupConversationPage, error) {
	s.calls++
	return s.page, s.err
}

func TestListGroupMessagesServesCompleteRedisPageWithoutStore(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedFriendStore()
	dataStore.groupMember = true
	reader := &groupReaderStub{page: messaging.GroupConversationPage{
		Page: store.GroupMessagePage{Messages: []store.GroupMessage{{
			ID:              7,
			Seq:             7,
			GroupID:         9,
			SenderID:        2,
			Sender:          store.PublicUser{Username: "bob", Nickname: "Bob"},
			ClientMessageID: "client-00000007",
			Body:            "hot group message",
			CreatedAt:       now,
		}}},
		Complete: true,
	}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithGroupMessageReader(reader)).Register(router)

	recorder := doJSON(t, router, http.MethodGet, "/api/groups/9/messages", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if dataStore.groupMessageCall.groupID != 0 {
		t.Fatal("store was queried even though Redis covered the page")
	}

	var response groupMessagePageResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Messages) != 1 || response.Messages[0].ID != 7 ||
		response.Messages[0].Seq != 7 {
		t.Fatalf("messages = %#v", response.Messages)
	}
}

func TestListGroupMessagesChecksMembershipBeforeRedis(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedFriendStore()
	dataStore.groupMember = false
	dataStore.groupMessagePageErr = store.ErrGroupNotFound
	reader := &groupReaderStub{page: messaging.GroupConversationPage{
		Page: store.GroupMessagePage{Messages: []store.GroupMessage{{
			ID:              7,
			GroupID:         9,
			SenderID:        2,
			ClientMessageID: "client-00000007",
			Body:            "should not leak",
			CreatedAt:       now,
		}}},
		Complete: true,
	}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithGroupMessageReader(reader)).Register(router)

	recorder := doJSON(t, router, http.MethodGet, "/api/groups/9/messages", nil, authTestCookie())
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
	if got := errorCode(t, recorder); got != "group_not_found" {
		t.Fatalf("error code = %q", got)
	}
	if reader.calls != 0 {
		t.Fatal("Redis was read before membership was confirmed")
	}
}

func TestListGroupMessagesFallsBackWhenCounterIsMissing(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedFriendStore()
	dataStore.groupMember = true
	dataStore.groupMessagePage = store.GroupMessagePage{Messages: []store.GroupMessage{{
		ID:              6,
		Seq:             6,
		GroupID:         9,
		SenderID:        1,
		ClientMessageID: "client-00000006",
		Body:            "from mysql",
		CreatedAt:       now,
	}}}
	// The reader reports the empty, incomplete page produced by a lost counter.
	reader := &groupReaderStub{page: messaging.GroupConversationPage{Complete: false}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithGroupMessageReader(reader)).Register(router)

	recorder := doJSON(t, router, http.MethodGet, "/api/groups/9/messages", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if dataStore.groupMessageCall.groupID != 9 {
		t.Fatal("store was not queried when the group seq counter was missing")
	}

	var response groupMessagePageResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Messages) != 1 || response.Messages[0].Body != "from mysql" {
		t.Fatalf("messages = %#v, want the MySQL history", response.Messages)
	}
}

func TestListGroupMessagesMergesHotPageWhenRedisIsIncomplete(t *testing.T) {
	now := time.Now().UTC()
	dataStore := authenticatedFriendStore()
	dataStore.groupMember = true
	dataStore.groupMessagePage = store.GroupMessagePage{Messages: []store.GroupMessage{{
		ID:              6,
		Seq:             6,
		GroupID:         9,
		SenderID:        1,
		ClientMessageID: "client-00000006",
		Body:            "from mysql",
		CreatedAt:       now,
	}}}
	reader := &groupReaderStub{page: messaging.GroupConversationPage{
		Page: store.GroupMessagePage{Messages: []store.GroupMessage{{
			ID:              7,
			Seq:             7,
			GroupID:         9,
			SenderID:        2,
			ClientMessageID: "client-00000007",
			Body:            "from redis",
			CreatedAt:       now,
		}}},
		Complete: false,
	}}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(dataStore, WithGroupMessageReader(reader)).Register(router)

	recorder := doJSON(t, router, http.MethodGet, "/api/groups/9/messages", nil, authTestCookie())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if dataStore.groupMessageCall.groupID != 9 {
		t.Fatal("store was not queried for an incomplete Redis page")
	}

	var response groupMessagePageResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Messages) != 2 ||
		response.Messages[0].Body != "from mysql" ||
		response.Messages[1].Body != "from redis" {
		t.Fatalf("messages = %#v", response.Messages)
	}
}
