package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

type fakeMomentStore struct {
	friendIDs []uint64
	friendErr error

	created        store.Moment
	createErr      error
	createCalls    int
	createdBody    string
	createdVisible store.MomentVisibility

	feed        []store.Moment
	feedHasMore bool
	feedErr     error
	feedBefore  uint64
	feedLimit   int
	previews    map[uint64][]store.MomentComment
	previewsErr error

	moment    store.Moment
	momentErr error

	deleteErr error

	likeCount uint64
	likeErr   error
	unlikeErr error
	likes     []store.MomentLike
	likesErr  error

	comments         []store.MomentComment
	commentsMore     bool
	commentsErr      error
	createdComment   store.MomentComment
	createCommentErr error
	deleteCommentErr error

	unreadLastSeen uint64
	unreadHasNew   bool
	unreadErr      error

	seenValue uint64
	seenErr   error
	seenInput uint64
}

func (f *fakeMomentStore) ListFriendIDs(context.Context, uint64) ([]uint64, error) {
	return f.friendIDs, f.friendErr
}

func (f *fakeMomentStore) CreateMoment(
	_ context.Context,
	authorID uint64,
	body string,
	visibility store.MomentVisibility,
	now time.Time,
) (store.Moment, error) {
	f.createCalls++
	f.createdBody = body
	f.createdVisible = visibility
	if f.createErr != nil {
		return store.Moment{}, f.createErr
	}
	if f.created.ID == 0 {
		f.created = store.Moment{
			ID:         1,
			AuthorID:   authorID,
			Body:       body,
			Visibility: visibility,
			CreatedAt:  now,
			Mine:       true,
		}
	}
	return f.created, nil
}

func (f *fakeMomentStore) ListMoments(
	_ context.Context,
	_ uint64,
	_ []uint64,
	beforeID uint64,
	limit int,
) ([]store.Moment, bool, error) {
	f.feedBefore = beforeID
	f.feedLimit = limit
	return f.feed, f.feedHasMore, f.feedErr
}

func (f *fakeMomentStore) GetMoment(context.Context, uint64, uint64) (store.Moment, error) {
	return f.moment, f.momentErr
}

func (f *fakeMomentStore) MomentCommentPreviews(
	context.Context, []uint64, int,
) (map[uint64][]store.MomentComment, error) {
	return f.previews, f.previewsErr
}

func (f *fakeMomentStore) DeleteMoment(context.Context, uint64, uint64, time.Time) error {
	return f.deleteErr
}

func (f *fakeMomentStore) LikeMoment(
	context.Context, uint64, uint64, time.Time,
) (uint64, error) {
	return f.likeCount, f.likeErr
}

func (f *fakeMomentStore) UnlikeMoment(context.Context, uint64, uint64) (uint64, error) {
	return f.likeCount, f.unlikeErr
}

func (f *fakeMomentStore) ListMomentLikes(
	context.Context, uint64, uint64, int,
) ([]store.MomentLike, bool, error) {
	return f.likes, false, f.likesErr
}

func (f *fakeMomentStore) ListMomentComments(
	context.Context, uint64, uint64, int,
) ([]store.MomentComment, bool, error) {
	return f.comments, f.commentsMore, f.commentsErr
}

func (f *fakeMomentStore) CreateMomentComment(
	context.Context, uint64, uint64, string, time.Time,
) (store.MomentComment, error) {
	return f.createdComment, f.createCommentErr
}

func (f *fakeMomentStore) DeleteMomentComment(
	context.Context, uint64, uint64, uint64, time.Time,
) error {
	return f.deleteCommentErr
}

func (f *fakeMomentStore) MomentUnread(
	context.Context, uint64, []uint64,
) (uint64, bool, error) {
	return f.unreadLastSeen, f.unreadHasNew, f.unreadErr
}

func (f *fakeMomentStore) MarkMomentsSeen(
	_ context.Context, _ uint64, lastSeenID uint64, _ time.Time,
) (uint64, error) {
	f.seenInput = lastSeenID
	return f.seenValue, f.seenErr
}

func momentTestRouter(
	t *testing.T,
	momentStore MomentStore,
	relationship store.Relationship,
) (*gin.Engine, *http.Cookie) {
	t.Helper()
	dataStore := &fakeStore{
		session: store.SessionUser{
			User: store.User{
				ID:       1,
				Username: "alice",
				Nickname: "Alice",
			},
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		},
		relationship: relationship,
	}
	return testRouter(dataStore, WithMomentStore(momentStore)),
		&http.Cookie{Name: sessionCookieName, Value: "session-token"}
}

func TestCreateMomentTrimsBodyAndDefaultsVisibility(t *testing.T) {
	momentStore := &fakeMomentStore{}
	router, cookie := momentTestRouter(t, momentStore, store.RelationshipNone)

	recorder := doJSON(t, router, http.MethodPost, "/api/moments", map[string]string{
		"body": "  今天天气不错  ",
	}, cookie)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d (%s), want 201", recorder.Code, recorder.Body.String())
	}
	if momentStore.createdBody != "今天天气不错" {
		t.Fatalf("created body = %q", momentStore.createdBody)
	}
	if momentStore.createdVisible != store.MomentVisibilityFriends {
		t.Fatalf("created visibility = %q", momentStore.createdVisible)
	}
	var response momentResponse
	decodeJSON(t, recorder, &response)
	if response.ID != 1 || !response.Mine || response.Author.Username != "alice" {
		t.Fatalf("response = %#v", response)
	}
}

func TestMomentBodyAndVisibilityValidation(t *testing.T) {
	momentStore := &fakeMomentStore{}
	router, cookie := momentTestRouter(t, momentStore, store.RelationshipNone)

	recorder := doJSON(t, router, http.MethodPost, "/api/moments", map[string]string{
		"body": strings.Repeat("字", momentMaxBodyRunes+1),
	}, cookie)
	if recorder.Code != http.StatusBadRequest || errorCode(t, recorder) != "invalid_moment_body" {
		t.Fatalf("long body status=%d code=%s", recorder.Code, errorCode(t, recorder))
	}

	recorder = doJSON(t, router, http.MethodPost, "/api/moments", map[string]string{
		"body": "hello", "visibility": "public",
	}, cookie)
	if recorder.Code != http.StatusBadRequest || errorCode(t, recorder) != "invalid_visibility" {
		t.Fatalf("bad visibility status=%d code=%s", recorder.Code, errorCode(t, recorder))
	}
	if momentStore.createCalls != 0 {
		t.Fatalf("create calls = %d, want 0", momentStore.createCalls)
	}
}

func TestMomentVisibilityRules(t *testing.T) {
	momentStore := &fakeMomentStore{
		moment: store.Moment{
			ID: 5, AuthorID: 2, Visibility: store.MomentVisibilityFriends,
			Author: store.PublicUser{Username: "bob", Nickname: "Bob"},
		},
	}
	router, cookie := momentTestRouter(t, momentStore, store.RelationshipNone)

	recorder := doJSON(t, router, http.MethodGet, "/api/moments/5", nil, cookie)
	if recorder.Code != http.StatusForbidden || errorCode(t, recorder) != "not_friend" {
		t.Fatalf("non-friend status=%d code=%s", recorder.Code, errorCode(t, recorder))
	}

	router, cookie = momentTestRouter(t, momentStore, store.RelationshipFriend)
	recorder = doJSON(t, router, http.MethodGet, "/api/moments/5", nil, cookie)
	if recorder.Code != http.StatusOK {
		t.Fatalf("friend status = %d (%s)", recorder.Code, recorder.Body.String())
	}

	momentStore.moment.Visibility = store.MomentVisibilityPrivate
	router, cookie = momentTestRouter(t, momentStore, store.RelationshipFriend)
	recorder = doJSON(t, router, http.MethodGet, "/api/moments/5", nil, cookie)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("private status = %d, want 403", recorder.Code)
	}
}

func TestLikeAndUnlikeMoment(t *testing.T) {
	momentStore := &fakeMomentStore{
		moment: store.Moment{
			ID: 5, AuthorID: 2, Visibility: store.MomentVisibilityFriends,
		},
		likeCount: 3,
	}
	router, cookie := momentTestRouter(t, momentStore, store.RelationshipFriend)

	recorder := doJSON(t, router, http.MethodPut, "/api/moments/5/like", nil, cookie)
	if recorder.Code != http.StatusOK {
		t.Fatalf("like status = %d (%s)", recorder.Code, recorder.Body.String())
	}
	var liked momentLikeResultResponse
	decodeJSON(t, recorder, &liked)
	if liked.LikeCount != 3 || !liked.LikedByMe {
		t.Fatalf("like response = %#v", liked)
	}

	momentStore.likeCount = 2
	recorder = doJSON(t, router, http.MethodDelete, "/api/moments/5/like", nil, cookie)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unlike status = %d", recorder.Code)
	}
	var unliked momentLikeResultResponse
	decodeJSON(t, recorder, &unliked)
	if unliked.LikeCount != 2 || unliked.LikedByMe {
		t.Fatalf("unlike response = %#v", unliked)
	}
}

func TestMomentFeedBuildsPreviewsAndCursor(t *testing.T) {
	now := time.Now().UTC()
	momentStore := &fakeMomentStore{
		friendIDs: []uint64{2},
		feed: []store.Moment{
			{
				ID: 9, AuthorID: 1, Author: store.PublicUser{Username: "alice", Nickname: "Alice"},
				Body: "own", Visibility: store.MomentVisibilityFriends, CreatedAt: now, Mine: true,
			},
			{
				ID: 8, AuthorID: 2, Author: store.PublicUser{Username: "bob", Nickname: "Bob"},
				Body: "friend", Visibility: store.MomentVisibilityFriends, CreatedAt: now,
				LikeCount: 2, CommentCount: 1,
			},
		},
		feedHasMore: true,
		previews: map[uint64][]store.MomentComment{
			8: {{
				ID: 3, MomentID: 8, AuthorID: 1,
				Author: store.PublicUser{Username: "alice", Nickname: "Alice"},
				Body:   "nice", CreatedAt: now,
			}},
		},
	}
	router, cookie := momentTestRouter(t, momentStore, store.RelationshipFriend)

	recorder := doJSON(t, router, http.MethodGet, "/api/moments?limit=2", nil, cookie)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d (%s)", recorder.Code, recorder.Body.String())
	}
	var response momentFeedResponse
	decodeJSON(t, recorder, &response)
	if len(response.Moments) != 2 || response.NextBeforeID != 8 || !response.HasMore {
		t.Fatalf("feed response = %#v", response)
	}
	if len(response.Moments[1].CommentsPreview) != 1 ||
		response.Moments[1].CommentsPreview[0].Body != "nice" {
		t.Fatalf("preview = %#v", response.Moments[1].CommentsPreview)
	}
}

func TestMomentUnreadAndSeen(t *testing.T) {
	momentStore := &fakeMomentStore{
		friendIDs:      []uint64{2},
		unreadLastSeen: 4,
		unreadHasNew:   true,
		seenValue:      9,
	}
	router, cookie := momentTestRouter(t, momentStore, store.RelationshipFriend)

	recorder := doJSON(t, router, http.MethodGet, "/api/moments/unread", nil, cookie)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unread status = %d", recorder.Code)
	}
	var unread momentUnreadResponse
	decodeJSON(t, recorder, &unread)
	if !unread.HasNew || unread.LastSeenID != 4 {
		t.Fatalf("unread = %#v", unread)
	}

	recorder = doJSON(t, router, http.MethodPost, "/api/moments/seen", map[string]uint64{
		"last_seen_id": 9,
	}, cookie)
	if recorder.Code != http.StatusOK {
		t.Fatalf("seen status = %d", recorder.Code)
	}
	if momentStore.seenInput != 9 {
		t.Fatalf("seen input = %d", momentStore.seenInput)
	}
}

func TestMomentDeleteErrors(t *testing.T) {
	momentStore := &fakeMomentStore{deleteErr: store.ErrMomentForbidden}
	router, cookie := momentTestRouter(t, momentStore, store.RelationshipFriend)

	recorder := doJSON(t, router, http.MethodDelete, "/api/moments/5", nil, cookie)
	if recorder.Code != http.StatusForbidden || errorCode(t, recorder) != "forbidden" {
		t.Fatalf("status=%d code=%s", recorder.Code, errorCode(t, recorder))
	}

	momentStore.deleteErr = store.ErrMomentNotFound
	recorder = doJSON(t, router, http.MethodDelete, "/api/moments/5", nil, cookie)
	if recorder.Code != http.StatusNotFound || errorCode(t, recorder) != "moment_not_found" {
		t.Fatalf("status=%d code=%s", recorder.Code, errorCode(t, recorder))
	}
}

func TestMomentStoreUnavailable(t *testing.T) {
	dataStore := &fakeStore{
		session: store.SessionUser{
			User:      store.User{ID: 1, Username: "alice"},
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		},
	}
	router := testRouter(dataStore)
	cookie := &http.Cookie{Name: sessionCookieName, Value: "session-token"}
	recorder := doJSON(t, router, http.MethodGet, "/api/moments", nil, cookie)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
}

func decodeJSON(t *testing.T, recorder *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(recorder.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response: %v (%s)", err, recorder.Body.String())
	}
}
