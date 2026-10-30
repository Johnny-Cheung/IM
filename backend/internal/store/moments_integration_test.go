package store

// Real-MySQL coverage for the v2.0 Moments queries. Skipped unless
// IM_TEST_MYSQL_DSN is set, same as the reconcile integration test. It reuses
// the throwaway "<database>_reconcile_test" schema.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestMySQLMomentsIntegration(t *testing.T) {
	st := openIntegrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	alice, err := st.CreateUser(ctx, "moment_alice", "moment_alice", "Alice", "hash")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := st.CreateUser(ctx, "moment_bob", "moment_bob", "Bob", "hash")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	carol, err := st.CreateUser(ctx, "moment_carol", "moment_carol", "Carol", "hash")
	if err != nil {
		t.Fatalf("create carol: %v", err)
	}
	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO friendships (user_id, friend_user_id, created_at)
		 VALUES (?, ?, ?), (?, ?, ?)`,
		alice.ID, bob.ID, now, bob.ID, alice.ID, now,
	); err != nil {
		t.Fatalf("create alice-bob friendship: %v", err)
	}

	friendMoment, err := st.CreateMoment(
		ctx, alice.ID, "alice friends", MomentVisibilityFriends, now,
	)
	if err != nil {
		t.Fatalf("create friend moment: %v", err)
	}
	privateMoment, err := st.CreateMoment(
		ctx, alice.ID, "alice private", MomentVisibilityPrivate, now.Add(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("create private moment: %v", err)
	}
	bobMoment, err := st.CreateMoment(
		ctx, bob.ID, "bob friends", MomentVisibilityFriends, now.Add(2*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("create bob moment: %v", err)
	}

	t.Run("feed respects friendship and private visibility", func(t *testing.T) {
		feed, hasMore, err := st.ListMoments(ctx, bob.ID, []uint64{alice.ID}, 0, 20)
		if err != nil {
			t.Fatalf("bob feed: %v", err)
		}
		if hasMore {
			t.Fatal("bob feed has_more = true, want false")
		}
		ids := make(map[uint64]bool, len(feed))
		for _, moment := range feed {
			ids[moment.ID] = true
		}
		if !ids[friendMoment.ID] || !ids[bobMoment.ID] {
			t.Fatalf("bob feed ids = %v, want friend and own moments", ids)
		}
		if ids[privateMoment.ID] {
			t.Fatal("bob feed contains alice's private moment")
		}

		carolFeed, _, err := st.ListMoments(ctx, carol.ID, nil, 0, 20)
		if err != nil {
			t.Fatalf("carol feed: %v", err)
		}
		if len(carolFeed) != 0 {
			t.Fatalf("carol feed = %#v, want empty", carolFeed)
		}
	})

	t.Run("likes are idempotent and counted", func(t *testing.T) {
		count, err := st.LikeMoment(ctx, friendMoment.ID, bob.ID, now)
		if err != nil || count != 1 {
			t.Fatalf("like count=%d err=%v, want 1", count, err)
		}
		count, err = st.LikeMoment(ctx, friendMoment.ID, bob.ID, now)
		if err != nil || count != 1 {
			t.Fatalf("repeat like count=%d err=%v, want 1", count, err)
		}
		moment, err := st.GetMoment(ctx, bob.ID, friendMoment.ID)
		if err != nil {
			t.Fatalf("get liked moment: %v", err)
		}
		if moment.LikeCount != 1 || !moment.LikedByMe {
			t.Fatalf("moment like_count=%d liked_by_me=%v", moment.LikeCount, moment.LikedByMe)
		}
		likes, hasMore, err := st.ListMomentLikes(ctx, friendMoment.ID, 0, 20)
		if err != nil || hasMore || len(likes) != 1 || likes[0].User.Username != "moment_bob" {
			t.Fatalf("likes=%#v hasMore=%v err=%v", likes, hasMore, err)
		}
		count, err = st.UnlikeMoment(ctx, friendMoment.ID, bob.ID)
		if err != nil || count != 0 {
			t.Fatalf("unlike count=%d err=%v, want 0", count, err)
		}
		count, err = st.UnlikeMoment(ctx, friendMoment.ID, bob.ID)
		if err != nil || count != 0 {
			t.Fatalf("repeat unlike count=%d err=%v, want 0", count, err)
		}
	})

	t.Run("comments are single-level and delete respects authorship", func(t *testing.T) {
		comment, err := st.CreateMomentComment(ctx, friendMoment.ID, bob.ID, "hello", now)
		if err != nil {
			t.Fatalf("create comment: %v", err)
		}
		previews, err := st.MomentCommentPreviews(ctx, []uint64{friendMoment.ID}, 2)
		if err != nil {
			t.Fatalf("comment previews: %v", err)
		}
		if len(previews[friendMoment.ID]) != 1 || previews[friendMoment.ID][0].Body != "hello" {
			t.Fatalf("previews = %#v", previews)
		}
		moment, err := st.GetMoment(ctx, bob.ID, friendMoment.ID)
		if err != nil || moment.CommentCount != 1 {
			t.Fatalf("comment count=%d err=%v", moment.CommentCount, err)
		}

		if err := st.DeleteMomentComment(ctx, friendMoment.ID, comment.ID, carol.ID, now); !errors.Is(err, ErrMomentForbidden) {
			t.Fatalf("carol delete error = %v, want forbidden", err)
		}
		// The moment author may delete another user's comment.
		if err := st.DeleteMomentComment(ctx, friendMoment.ID, comment.ID, alice.ID, now); err != nil {
			t.Fatalf("alice delete comment: %v", err)
		}
		comments, hasMore, err := st.ListMomentComments(ctx, friendMoment.ID, 0, 20)
		if err != nil || hasMore || len(comments) != 0 {
			t.Fatalf("comments=%#v hasMore=%v err=%v", comments, hasMore, err)
		}
		moment, err = st.GetMoment(ctx, bob.ID, friendMoment.ID)
		if err != nil || moment.CommentCount != 0 {
			t.Fatalf("comment count after delete=%d err=%v", moment.CommentCount, err)
		}
	})

	t.Run("comment pagination and previews use the newest comments", func(t *testing.T) {
		pageMoment, err := st.CreateMoment(
			ctx, bob.ID, "comment pagination", MomentVisibilityFriends, now.Add(3*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("create pagination moment: %v", err)
		}
		for index := 1; index <= 25; index++ {
			if _, err := st.CreateMomentComment(
				ctx, pageMoment.ID, alice.ID, fmt.Sprintf("c%d", index), now,
			); err != nil {
				t.Fatalf("create comment %d: %v", index, err)
			}
		}
		first, hasMore, err := st.ListMomentComments(ctx, pageMoment.ID, 0, 20)
		if err != nil || !hasMore || len(first) != 20 {
			t.Fatalf("first comments len=%d hasMore=%v err=%v", len(first), hasMore, err)
		}
		if first[0].Body != "c6" || first[len(first)-1].Body != "c25" {
			t.Fatalf("first comment page = %q..%q, want c6..c25",
				first[0].Body, first[len(first)-1].Body)
		}
		second, hasMore, err := st.ListMomentComments(ctx, pageMoment.ID, first[0].ID, 20)
		if err != nil || hasMore || len(second) != 5 {
			t.Fatalf("second comments len=%d hasMore=%v err=%v", len(second), hasMore, err)
		}
		if second[0].Body != "c1" || second[len(second)-1].Body != "c5" {
			t.Fatalf("second comment page = %q..%q, want c1..c5",
				second[0].Body, second[len(second)-1].Body)
		}

		previews, err := st.MomentCommentPreviews(ctx, []uint64{pageMoment.ID}, 2)
		if err != nil {
			t.Fatalf("comment previews: %v", err)
		}
		got := previews[pageMoment.ID]
		if len(got) != 2 || got[0].Body != "c24" || got[1].Body != "c25" {
			t.Fatalf("previews = %#v, want c24,c25", got)
		}
		if err := st.DeleteMomentComment(ctx, pageMoment.ID, got[1].ID, alice.ID, now); err != nil {
			t.Fatalf("delete preview comment: %v", err)
		}
		previews, err = st.MomentCommentPreviews(ctx, []uint64{pageMoment.ID}, 2)
		if err != nil {
			t.Fatalf("comment previews after delete: %v", err)
		}
		got = previews[pageMoment.ID]
		if len(got) != 2 || got[0].Body != "c23" || got[1].Body != "c24" {
			t.Fatalf("previews after delete = %#v, want c23,c24", got)
		}
	})

	t.Run("unread watermark moves only forward", func(t *testing.T) {
		lastSeen, hasNew, err := st.MomentUnread(ctx, bob.ID, []uint64{alice.ID})
		if err != nil {
			t.Fatalf("unread: %v", err)
		}
		if lastSeen != 0 || !hasNew {
			t.Fatalf("lastSeen=%d hasNew=%v, want 0 true", lastSeen, hasNew)
		}
		effective, err := st.MarkMomentsSeen(ctx, bob.ID, friendMoment.ID, now)
		if err != nil || effective != friendMoment.ID {
			t.Fatalf("mark seen=%d err=%v", effective, err)
		}
		effective, err = st.MarkMomentsSeen(ctx, bob.ID, 0, now)
		if err != nil || effective != friendMoment.ID {
			t.Fatalf("seen moved backwards to %d (err=%v)", effective, err)
		}
		_, hasNew, err = st.MomentUnread(ctx, bob.ID, []uint64{alice.ID})
		if err != nil || hasNew {
			t.Fatalf("hasNew=%v err=%v, want false", hasNew, err)
		}
	})

	t.Run("delete moment is author-only and hides it", func(t *testing.T) {
		comment, err := st.CreateMomentComment(ctx, friendMoment.ID, bob.ID, "still here", now)
		if err != nil {
			t.Fatalf("create comment before moment delete: %v", err)
		}
		if err := st.DeleteMoment(ctx, friendMoment.ID, bob.ID, now); !errors.Is(err, ErrMomentForbidden) {
			t.Fatalf("bob delete error = %v, want forbidden", err)
		}
		if err := st.DeleteMoment(ctx, friendMoment.ID, alice.ID, now); err != nil {
			t.Fatalf("alice delete moment: %v", err)
		}
		if _, err := st.GetMoment(ctx, bob.ID, friendMoment.ID); !errors.Is(err, ErrMomentNotFound) {
			t.Fatalf("get deleted moment error = %v, want not found", err)
		}
		if err := st.DeleteMomentComment(
			ctx, friendMoment.ID, comment.ID, bob.ID, now,
		); !errors.Is(err, ErrMomentNotFound) {
			t.Fatalf("delete comment on deleted moment error = %v, want moment not found", err)
		}
	})

	t.Run("feed paginates by id without duplicates", func(t *testing.T) {
		for index := 0; index < 25; index++ {
			if _, err := st.CreateMoment(
				ctx, bob.ID, "page", MomentVisibilityFriends,
				now.Add(time.Duration(index+3)*time.Millisecond),
			); err != nil {
				t.Fatalf("seed page moment: %v", err)
			}
		}
		first, hasMore, err := st.ListMoments(ctx, alice.ID, []uint64{bob.ID}, 0, 20)
		if err != nil || !hasMore || len(first) != 20 {
			t.Fatalf("first page len=%d hasMore=%v err=%v", len(first), hasMore, err)
		}
		seen := make(map[uint64]bool, len(first))
		for _, moment := range first {
			seen[moment.ID] = true
		}
		next := first[len(first)-1].ID
		second, hasMore, err := st.ListMoments(ctx, alice.ID, []uint64{bob.ID}, next, 20)
		if err != nil || hasMore {
			t.Fatalf("second page hasMore=%v err=%v", hasMore, err)
		}
		if len(second) != 8 {
			t.Fatalf("second page len = %d, want 8", len(second))
		}
		total := len(first) + len(second)
		if total != 28 {
			t.Fatalf("feed total = %d, want 28 (alice's private moment plus bob's 27)", total)
		}
		for _, moment := range second {
			if seen[moment.ID] {
				t.Fatalf("moment %d appeared on both pages", moment.ID)
			}
			if moment.ID >= next {
				t.Fatalf("moment %d >= cursor %d", moment.ID, next)
			}
		}
	})
}
