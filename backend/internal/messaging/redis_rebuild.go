package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// QueueDrainResult reports what the persist queues delivered before a rebuild.
type QueueDrainResult struct {
	Direct       int
	Group        int
	DeadLettered int
}

// ErrRebuildMissingSeq reports that MySQL still has messages without a seq, so
// the seq backfill must run first.
var ErrRebuildMissingSeq = errors.New(
	"messages without seq found; run backfill-seq before rebuilding Redis",
)

// RebuildLockTTL bounds the rebuild lock that the rebuild command holds.
const RebuildLockTTL = 30 * time.Minute

const (
	defaultRebuildBatch  = 1000
	rebuildScanCountHint = 500
	rebuildEntitySamples = 8
	// rebuildDedupSamples bounds the dedup records read back per message kind.
	rebuildDedupSamples = 8
	// rebuildConvLastUsers bounds how many users' conversation previews are
	// verified; the sample is the first users met in message ID order.
	rebuildConvLastUsers = 64
	// rebuildDedupExpiryMargin keeps records that may expire during the
	// rebuild out of the dedup-count lower bound.
	rebuildDedupExpiryMargin = time.Hour
	rebuildCleanupTimeout    = 5 * time.Second
)

// RebuildStore is the durable source of a full Redis rebuild. *store.Store
// implements it.
type RebuildStore interface {
	ListMessagesForRebuild(context.Context, uint64, int) ([]store.Message, error)
	ListGroupMessagesForRebuild(context.Context, uint64, int) ([]store.GroupMessage, error)
	ListMessagesBySeqRange(context.Context, uint64, uint64, uint64, int) ([]store.Message, error)
	ListGroupMessagesBySeqRange(context.Context, uint64, uint64, int) ([]store.GroupMessage, error)
	MaxServerMessageID(context.Context) (uint64, error)
	ListConversationReadStates(context.Context, uint64, uint64, int) ([]store.ConversationReadState, error)
	ListConversationSeqStats(context.Context, uint64, uint64, int) ([]store.ConversationSeqStat, error)
	ListGroupSeqStats(context.Context, uint64, int) ([]store.GroupSeqStat, error)
	ListGroupMembershipProjections(context.Context) ([]store.GroupMembershipProjection, error)
	ListFriendshipPairs(context.Context) ([]store.FriendshipPair, error)
	CountUnnumberedMessages(context.Context) (uint64, uint64, error)
}

// RedisRebuildOptions configures a full rebuild. Zero values select defaults.
type RedisRebuildOptions struct {
	Keyspace  Keyspace
	Namespace Namespace
	Batch     int
	DedupTTL  time.Duration
	Now       func() time.Time
	Logf      func(string, ...interface{})
	// DrainQueues must consume both persist queues to empty before the
	// namespace is cleared. It is required.
	DrainQueues func(ctx context.Context) (QueueDrainResult, error)
}

// RedisRebuildResult summarises one rebuild.
type RedisRebuildResult struct {
	Drained                       QueueDrainResult
	DirectMessages, GroupMessages uint64
	DedupRecords, Watermarks      uint64
	// Holes counts seq numbers that were allocated but never persisted. They
	// are reported, not repaired: only MySQL can explain them.
	Holes                 uint64
	Conversations, Groups int
	Cleared               int
	Duration              time.Duration
}

// rebuildDedupSample is one dedup record the rebuild should have written.
type rebuildDedupSample struct {
	key         string
	id          string
	createdAtMS string
	bodyHash    string
}

// rebuildStats accumulates what the passes wrote so verification can compare it
// with MySQL without rescanning.
type rebuildStats struct {
	maxSeqByPair   map[[2]uint64]uint64
	maxSeqByGroup  map[uint64]uint64
	feedCounts     map[uint64]uint64
	groupPositions map[uint64]map[uint64]uint64
	dedupExpected  uint64
	// dedupDurable excludes records whose TTL may run out before verification.
	dedupDurable uint64
	directDedups []rebuildDedupSample
	groupDedups  []rebuildDedupSample
	// convLast holds, per sampled user, the newest message ID per peer.
	convLast map[uint64]map[uint64]uint64
	holes    uint64
}

func newRebuildStats() *rebuildStats {
	return &rebuildStats{
		maxSeqByPair:   make(map[[2]uint64]uint64),
		maxSeqByGroup:  make(map[uint64]uint64),
		feedCounts:     make(map[uint64]uint64),
		groupPositions: make(map[uint64]map[uint64]uint64),
		convLast:       make(map[uint64]map[uint64]uint64),
	}
}

// noteConversationLast records the expected dm:conv-last entry for one user,
// admitting at most rebuildConvLastUsers users to bound memory.
func (s *rebuildStats) noteConversationLast(userID, peerID, messageID uint64) {
	if userID == 0 || peerID == 0 || userID == peerID {
		return
	}
	peers, ok := s.convLast[userID]
	if !ok {
		if len(s.convLast) >= rebuildConvLastUsers {
			return
		}
		peers = make(map[uint64]uint64)
		s.convLast[userID] = peers
	}
	if messageID > peers[peerID] {
		peers[peerID] = messageID
	}
}

func (s *rebuildStats) noteDirectDedup(sample rebuildDedupSample, ttl time.Duration) {
	s.noteDedup(ttl)
	s.directDedups = appendRollingDedup(s.directDedups, sample)
}

func (s *rebuildStats) noteGroupDedup(sample rebuildDedupSample, ttl time.Duration) {
	s.noteDedup(ttl)
	s.groupDedups = appendRollingDedup(s.groupDedups, sample)
}

func (s *rebuildStats) noteDedup(ttl time.Duration) {
	s.dedupExpected++
	if ttl > rebuildDedupExpiryMargin {
		s.dedupDurable++
	}
}

// appendRollingDedup keeps the newest samples, which have the longest TTL and
// therefore survive the verification read.
func appendRollingDedup(
	samples []rebuildDedupSample,
	sample rebuildDedupSample,
) []rebuildDedupSample {
	samples = append(samples, sample)
	if len(samples) > rebuildDedupSamples {
		samples = samples[len(samples)-rebuildDedupSamples:]
	}
	return samples
}

// RedisRebuilder regenerates the whole messaging namespace from MySQL. It must
// run with the backend stopped and the persist queues drained.
type RedisRebuilder struct {
	client  redis.UniversalClient
	store   RebuildStore
	options RedisRebuildOptions
	writer  *ProjectionWriter
}

func NewRedisRebuilder(
	client redis.UniversalClient,
	dataStore RebuildStore,
	options RedisRebuildOptions,
) *RedisRebuilder {
	if options.Keyspace.prefix == "" {
		options.Keyspace = NewKeyspace(options.Namespace)
	}
	if options.Batch <= 0 {
		options.Batch = defaultRebuildBatch
	}
	if options.DedupTTL <= 0 {
		options.DedupTTL = defaultDedupTTL
	}
	if options.Now == nil {
		options.Now = func() time.Time { return time.Now().UTC() }
	}
	if options.Logf == nil {
		options.Logf = log.Printf
	}
	return &RedisRebuilder{
		client:  client,
		store:   dataStore,
		options: options,
		writer: NewProjectionWriter(client, options.Keyspace, ProjectionWriteOptions{
			DedupTTL: options.DedupTTL,
			Now:      options.Now,
		}),
	}
}

// Run drains the queues, clears the namespace and rebuilds every structure,
// verifying them before it writes seq:ready.
func (r *RedisRebuilder) Run(ctx context.Context) (result RedisRebuildResult, err error) {
	start := time.Now()
	defer func() { result.Duration = time.Since(start) }()

	if r == nil || r.client == nil || r.store == nil {
		return result, ErrMessageServiceUnavailable
	}
	if r.options.DrainQueues == nil {
		return result, fmt.Errorf("redis rebuild requires a queue drain")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	keyspace := r.options.Keyspace

	backfilling, err := r.client.Exists(ctx, keyspace.backfillLockKey()).Result()
	if err != nil {
		return result, fmt.Errorf("check backfill lock: %w", err)
	}
	if backfilling > 0 {
		return result, fmt.Errorf("seq backfill is running; wait for it to finish")
	}
	if err := r.checkMissingSeq(ctx); err != nil {
		return result, err
	}

	drained, err := r.options.DrainQueues(ctx)
	if err != nil {
		return result, fmt.Errorf("drain persist queues: %w", err)
	}
	result.Drained = drained
	r.logf(
		"rebuild: queues drained direct=%d group=%d dead_lettered=%d",
		drained.Direct, drained.Group, drained.DeadLettered,
	)

	// Mark the rebuild as unfinished before anything is destroyed. The marker
	// stays behind on every failure so an operator knows to re-run.
	if err := r.client.Set(
		ctx, RebuildPendingKey(keyspace), "1", 0,
	).Err(); err != nil {
		return result, fmt.Errorf("mark rebuild pending: %w", err)
	}

	cleared, err := ClearNamespace(ctx, r.client, keyspace)
	if err != nil {
		return result, err
	}
	result.Cleared = cleared
	r.logf("rebuild: cleared %d keys", cleared)

	counter, err := r.store.MaxServerMessageID(ctx)
	if err != nil {
		return result, fmt.Errorf("read global message counter: %w", err)
	}
	if err := r.client.Set(
		ctx, keyspace.counterKey(), strconv.FormatUint(counter, 10), 0,
	).Err(); err != nil {
		return result, fmt.Errorf("set global message counter: %w", err)
	}
	r.logf("rebuild: global counter=%d", counter)

	stats := newRebuildStats()
	if err := r.rebuildDirectPass(ctx, &result, stats); err != nil {
		return result, err
	}
	if err := r.rebuildGroupPass(ctx, &result, stats); err != nil {
		return result, err
	}
	if err := r.rebuildRelations(ctx, &result, stats); err != nil {
		return result, err
	}
	result.Conversations = len(stats.maxSeqByPair)
	result.Groups = len(stats.maxSeqByGroup)

	verifyErr := r.verify(ctx, stats)
	result.Holes = stats.holes
	r.logf("rebuild: verification holes=%d", stats.holes)
	if verifyErr != nil {
		return result, verifyErr
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	// index:epoch claims the feed is complete, so it may only exist for a
	// verified namespace; withdraw it again if anything later fails.
	epochSet := false
	readySet := false
	defer func() {
		if !epochSet || readySet || err == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), rebuildCleanupTimeout)
		defer cancel()
		if delErr := r.client.Del(cleanupCtx, keyspace.indexEpochKey()).Err(); delErr != nil {
			r.logf("rebuild: withdraw index epoch: %v", delErr)
		}
	}()
	if err := r.client.Set(ctx, keyspace.indexEpochKey(), "0", 0).Err(); err != nil {
		return result, fmt.Errorf("reset index epoch: %w", err)
	}
	epochSet = true
	if err := r.client.Set(ctx, keyspace.sequenceReadyKey(), "1", 0).Err(); err != nil {
		return result, fmt.Errorf("mark sequence ready: %w", err)
	}
	readySet = true
	if err := r.client.Del(ctx, RebuildPendingKey(keyspace)).Err(); err != nil {
		return result, fmt.Errorf("clear rebuild pending marker: %w", err)
	}

	r.logf(
		"rebuild: done direct=%d group=%d conversations=%d groups=%d dedup=%d "+
			"watermarks=%d holes=%d",
		result.DirectMessages, result.GroupMessages, result.Conversations,
		result.Groups, result.DedupRecords, result.Watermarks, result.Holes,
	)
	return result, nil
}

func (r *RedisRebuilder) logf(format string, args ...interface{}) {
	if r.options.Logf != nil {
		r.options.Logf(format, args...)
	}
}

// checkMissingSeq refuses the rebuild before anything is cleared when MySQL
// still has rows without a seq.
func (r *RedisRebuilder) checkMissingSeq(ctx context.Context) error {
	direct, group, err := r.store.CountUnnumberedMessages(ctx)
	if err != nil {
		return fmt.Errorf("count unnumbered messages: %w", err)
	}
	if direct > 0 || group > 0 {
		return fmt.Errorf("%w: direct=%d group=%d", ErrRebuildMissingSeq, direct, group)
	}
	return nil
}

// rebuildDirectPass writes every direct message, paging by server message ID.
func (r *RedisRebuilder) rebuildDirectPass(
	ctx context.Context,
	result *RedisRebuildResult,
	stats *rebuildStats,
) error {
	cursor := uint64(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		messages, err := r.store.ListMessagesForRebuild(ctx, cursor, r.options.Batch)
		if err != nil {
			return fmt.Errorf("list messages for rebuild: %w", err)
		}
		if len(messages) == 0 {
			return nil
		}
		if err := rejectMissingSeq(messages); err != nil {
			return err
		}
		written, err := r.writer.WriteDirectBatch(ctx, messages)
		if err != nil {
			return err
		}
		now := r.options.Now().UTC()
		nextCursor := cursor
		for _, message := range messages {
			nextCursor = message.ID
			if ttl, ok := messageDedupTTL(
				message.ClientMessageID, message.CreatedAt, now, r.options.DedupTTL,
			); ok {
				result.DedupRecords++
				createdAt := message.CreatedAt.UTC().Truncate(time.Millisecond)
				stats.noteDirectDedup(rebuildDedupSample{
					key: r.options.Keyspace.dedupKey(
						message.SenderID, message.RecipientID, message.ClientMessageID,
					),
					id:          strconv.FormatUint(message.ID, 10),
					createdAtMS: strconv.FormatInt(createdAt.UnixMilli(), 10),
					bodyHash:    hashMessageBody(message.Body),
				}, ttl)
			}
			pair := [2]uint64{}
			pair[0], pair[1] = sortedPair(message.SenderID, message.RecipientID)
			if message.Seq > stats.maxSeqByPair[pair] {
				stats.maxSeqByPair[pair] = message.Seq
			}
			stats.feedCounts[message.SenderID]++
			if message.RecipientID != message.SenderID {
				stats.feedCounts[message.RecipientID]++
			}
			stats.noteConversationLast(message.SenderID, message.RecipientID, message.ID)
			stats.noteConversationLast(message.RecipientID, message.SenderID, message.ID)
		}
		if nextCursor <= cursor {
			return fmt.Errorf("direct rebuild cursor did not advance at %d", cursor)
		}
		cursor = nextCursor
		result.DirectMessages += uint64(written)
		r.logf(
			"rebuild: direct messages=%d conversations=%d",
			result.DirectMessages, len(stats.maxSeqByPair),
		)
	}
}

// rebuildGroupPass writes every group message, paging by server message ID.
func (r *RedisRebuilder) rebuildGroupPass(
	ctx context.Context,
	result *RedisRebuildResult,
	stats *rebuildStats,
) error {
	cursor := uint64(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		messages, err := r.store.ListGroupMessagesForRebuild(ctx, cursor, r.options.Batch)
		if err != nil {
			return fmt.Errorf("list group messages for rebuild: %w", err)
		}
		if len(messages) == 0 {
			return nil
		}
		for _, message := range messages {
			if message.Seq == 0 {
				return fmt.Errorf("%w: group message %d", ErrRebuildMissingSeq, message.ID)
			}
		}
		written, err := r.writer.WriteGroupBatch(ctx, messages)
		if err != nil {
			return err
		}
		now := r.options.Now().UTC()
		nextCursor := cursor
		for _, message := range messages {
			nextCursor = message.ID
			if ttl, ok := messageDedupTTL(
				message.ClientMessageID, message.CreatedAt, now, r.options.DedupTTL,
			); ok {
				result.DedupRecords++
				createdAt := message.CreatedAt.UTC().Truncate(time.Millisecond)
				stats.noteGroupDedup(rebuildDedupSample{
					key: r.options.Keyspace.groupDedupKey(
						message.GroupID, message.SenderID, message.ClientMessageID,
					),
					id:          strconv.FormatUint(message.ID, 10),
					createdAtMS: strconv.FormatInt(createdAt.UnixMilli(), 10),
					bodyHash:    hashMessageBody(message.Body),
				}, ttl)
			}
			if message.Seq > stats.maxSeqByGroup[message.GroupID] {
				stats.maxSeqByGroup[message.GroupID] = message.Seq
			}
		}
		if nextCursor <= cursor {
			return fmt.Errorf("group rebuild cursor did not advance at %d", cursor)
		}
		cursor = nextCursor
		result.GroupMessages += uint64(written)
		r.logf(
			"rebuild: group messages=%d groups=%d",
			result.GroupMessages, len(stats.maxSeqByGroup),
		)
	}
}

func rejectMissingSeq(messages []store.Message) error {
	for _, message := range messages {
		if message.Seq == 0 {
			return fmt.Errorf("%w: direct message %d", ErrRebuildMissingSeq, message.ID)
		}
	}
	return nil
}

// rebuildRelations rebuilds friendships, the group projection and every read
// watermark.
func (r *RedisRebuilder) rebuildRelations(
	ctx context.Context,
	result *RedisRebuildResult,
	stats *rebuildStats,
) error {
	keyspace := r.options.Keyspace

	pairs, err := r.store.ListFriendshipPairs(ctx)
	if err != nil {
		return fmt.Errorf("list friendship pairs: %w", err)
	}
	friendship := NewFriendshipProjectionWithKeyspace(r.client, keyspace)
	if err := friendship.Rebuild(ctx, pairs); err != nil {
		return fmt.Errorf("rebuild friendship projection: %w", err)
	}
	r.logf("rebuild: friends=%d", len(pairs))

	afterUserID, afterPeerID := uint64(0), uint64(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		states, err := r.store.ListConversationReadStates(
			ctx, afterUserID, afterPeerID, r.options.Batch,
		)
		if err != nil {
			return fmt.Errorf("list conversation read states: %w", err)
		}
		if len(states) == 0 {
			break
		}
		afterUserID = states[len(states)-1].UserID
		afterPeerID = states[len(states)-1].PeerID
		// A durable watermark can sit past MAX(seq) when the tail was lost;
		// clamping means "everything there is read".
		clamped := make([]store.ConversationReadState, 0, len(states))
		for _, state := range states {
			if state.UserID == 0 || state.PeerID == 0 ||
				state.UserID == state.PeerID {
				clamped = append(clamped, state)
				continue
			}
			pair := [2]uint64{}
			pair[0], pair[1] = sortedPair(state.UserID, state.PeerID)
			if counter := stats.maxSeqByPair[pair]; state.LastReadSeq > counter {
				state.LastReadSeq = counter
			}
			clamped = append(clamped, state)
		}
		raised, err := r.writer.RaiseDirectReadWatermarks(ctx, clamped)
		if err != nil {
			return err
		}
		result.Watermarks += uint64(raised)
		r.logf("rebuild: direct watermarks=%d", result.Watermarks)
	}

	storedGroups, err := r.store.ListGroupMembershipProjections(ctx)
	if err != nil {
		return fmt.Errorf("list group membership projections: %w", err)
	}
	projections := make([]GroupMembershipProjection, 0, len(storedGroups))
	for _, group := range storedGroups {
		projections = append(projections, GroupMembershipProjection{
			GroupID:   group.GroupID,
			MemberIDs: group.MemberIDs,
			Dissolved: group.Dissolved,
		})
		stats.groupPositions[group.GroupID] = group.Positions
	}
	groupProjection := NewGroupProjection(r.client, keyspace)
	if err := groupProjection.Rebuild(ctx, projections); err != nil {
		return fmt.Errorf("rebuild group projection: %w", err)
	}
	for _, group := range storedGroups {
		if err := ctx.Err(); err != nil {
			return err
		}
		counter := stats.maxSeqByGroup[group.GroupID]
		positions := make(map[uint64]uint64, len(group.Positions))
		for userID, position := range group.Positions {
			if position > counter {
				position = counter
			}
			positions[userID] = position
		}
		raised, err := r.writer.RaiseGroupReadPositions(
			ctx, group.GroupID, positions,
		)
		if err != nil {
			return err
		}
		result.Watermarks += uint64(raised)
	}
	r.logf(
		"rebuild: groups=%d watermarks=%d", len(storedGroups), result.Watermarks,
	)
	return nil
}

// verify compares every rebuilt structure with MySQL before seq:ready is
// written. Any mismatch aborts with the offending conversation or message.
func (r *RedisRebuilder) verify(ctx context.Context, stats *rebuildStats) error {
	if err := r.verifyDirectConversations(ctx, stats); err != nil {
		return err
	}
	if err := r.verifyGroups(ctx, stats); err != nil {
		return err
	}
	if err := r.verifyFeeds(ctx, stats); err != nil {
		return err
	}
	if err := r.verifyDedup(ctx, stats); err != nil {
		return err
	}
	if err := r.verifyConversationLast(ctx, stats); err != nil {
		return err
	}
	return r.verifyWatermarks(ctx, stats)
}

func (r *RedisRebuilder) verifyDirectConversations(
	ctx context.Context,
	stats *rebuildStats,
) error {
	keyspace := r.options.Keyspace
	afterLowID, afterHighID := uint64(0), uint64(0)
	seen := make(map[[2]uint64]struct{}, len(stats.maxSeqByPair))
	samples := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := r.store.ListConversationSeqStats(
			ctx, afterLowID, afterHighID, r.options.Batch,
		)
		if err != nil {
			return fmt.Errorf("list conversation seq stats: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			afterLowID, afterHighID = row.LowID, row.HighID
			if row.LowID == 0 || row.HighID == 0 || row.LowID == row.HighID {
				return fmt.Errorf("conversation %d:%d is invalid", row.LowID, row.HighID)
			}
			// Seq numbers allocated but never persisted leave a permanent gap;
			// they are a MySQL fact that a rebuild must report, not refuse.
			if row.MaxSeq > row.Count {
				holes := row.MaxSeq - row.Count
				stats.holes += holes
				r.logf(
					"rebuild: conversation %d:%d has %d seq holes",
					row.LowID, row.HighID, holes,
				)
			}
			indexed, err := r.client.ZCard(
				ctx, keyspace.directPairKey(row.LowID, row.HighID),
			).Result()
			if err != nil {
				return fmt.Errorf("count conversation pair %d:%d: %w", row.LowID, row.HighID, err)
			}
			if uint64(indexed) != row.Count {
				return fmt.Errorf(
					"conversation %d:%d index has %d entries, mysql has %d",
					row.LowID, row.HighID, indexed, row.Count,
				)
			}
			counter, err := r.readCounter(
				ctx, keyspace.directSequenceKey(row.LowID, row.HighID),
			)
			if err != nil {
				return err
			}
			if counter != row.MaxSeq {
				return fmt.Errorf(
					"conversation %d:%d counter %d does not equal max seq %d",
					row.LowID, row.HighID, counter, row.MaxSeq,
				)
			}
			pair := [2]uint64{row.LowID, row.HighID}
			if stats.maxSeqByPair[pair] != row.MaxSeq {
				return fmt.Errorf(
					"conversation %d:%d rebuild wrote max seq %d, mysql has %d",
					row.LowID, row.HighID, stats.maxSeqByPair[pair], row.MaxSeq,
				)
			}
			seen[pair] = struct{}{}
			if samples < rebuildEntitySamples && row.Count > 0 {
				if err := r.verifyDirectEntities(
					ctx, row.LowID, row.HighID, row.MaxSeq,
				); err != nil {
					return err
				}
				samples++
			}
		}
	}
	for pair := range stats.maxSeqByPair {
		if _, ok := seen[pair]; !ok {
			return fmt.Errorf(
				"conversation %d:%d was rebuilt but not found in mysql stats",
				pair[0], pair[1],
			)
		}
	}
	return nil
}

func (r *RedisRebuilder) verifyGroups(
	ctx context.Context,
	stats *rebuildStats,
) error {
	keyspace := r.options.Keyspace
	afterGroupID := uint64(0)
	seen := make(map[uint64]struct{}, len(stats.maxSeqByGroup))
	samples := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := r.store.ListGroupSeqStats(ctx, afterGroupID, r.options.Batch)
		if err != nil {
			return fmt.Errorf("list group seq stats: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			afterGroupID = row.GroupID
			if row.GroupID == 0 {
				return fmt.Errorf("group 0 is invalid")
			}
			if row.MaxSeq > row.Count {
				holes := row.MaxSeq - row.Count
				stats.holes += holes
				r.logf("rebuild: group %d has %d seq holes", row.GroupID, holes)
			}
			indexed, err := r.client.ZCard(ctx, keyspace.groupInboxKey(row.GroupID)).Result()
			if err != nil {
				return fmt.Errorf("count group %d inbox: %w", row.GroupID, err)
			}
			if uint64(indexed) != row.Count {
				return fmt.Errorf(
					"group %d inbox has %d entries, mysql has %d",
					row.GroupID, indexed, row.Count,
				)
			}
			counter, err := r.readCounter(ctx, keyspace.groupSequenceKey(row.GroupID))
			if err != nil {
				return err
			}
			if counter != row.MaxSeq {
				return fmt.Errorf(
					"group %d counter %d does not equal max seq %d",
					row.GroupID, counter, row.MaxSeq,
				)
			}
			if stats.maxSeqByGroup[row.GroupID] != row.MaxSeq {
				return fmt.Errorf(
					"group %d rebuild wrote max seq %d, mysql has %d",
					row.GroupID, stats.maxSeqByGroup[row.GroupID], row.MaxSeq,
				)
			}
			seen[row.GroupID] = struct{}{}
			if samples < rebuildEntitySamples && row.Count > 0 {
				if err := r.verifyGroupEntities(ctx, row.GroupID, row.MaxSeq); err != nil {
					return err
				}
				samples++
			}
		}
	}
	for groupID := range stats.maxSeqByGroup {
		if _, ok := seen[groupID]; !ok {
			return fmt.Errorf(
				"group %d was rebuilt but not found in mysql stats", groupID,
			)
		}
	}
	return nil
}

// verifyDirectEntities compares the oldest and newest messages of one
// conversation with MySQL, including their pair index entries.
func (r *RedisRebuilder) verifyDirectEntities(
	ctx context.Context,
	lowID, highID, maxSeq uint64,
) error {
	keyspace := r.options.Keyspace
	afterSeqs := []uint64{0}
	if maxSeq > 2 {
		afterSeqs = append(afterSeqs, maxSeq-2)
	}
	for _, afterSeq := range afterSeqs {
		rows, err := r.store.ListMessagesBySeqRange(ctx, lowID, highID, afterSeq, 2)
		if err != nil {
			return fmt.Errorf(
				"list conversation %d:%d entities: %w", lowID, highID, err,
			)
		}
		for _, row := range rows {
			values, err := r.client.HMGet(ctx, keyspace.messageKey(row.ID),
				"id", "seq", "sender_id", "recipient_id", "body",
			).Result()
			if err != nil {
				return fmt.Errorf("read conversation %d:%d message %d: %w", lowID, highID, row.ID, err)
			}
			if err := matchDirectEntity(values, row); err != nil {
				return fmt.Errorf("conversation %d:%d %w", lowID, highID, err)
			}
			score, err := r.client.ZScore(
				ctx, keyspace.directPairKey(lowID, highID), strconv.FormatUint(row.ID, 10),
			).Result()
			if err != nil || uint64(score) != row.Seq {
				return fmt.Errorf(
					"conversation %d:%d message %d pair score %v does not equal seq %d",
					lowID, highID, row.ID, score, row.Seq,
				)
			}
		}
	}
	return nil
}

func matchDirectEntity(values []interface{}, row store.Message) error {
	if len(values) != 5 {
		return fmt.Errorf("message %d entity is missing fields", row.ID)
	}
	for index, value := range values {
		if value == nil {
			return fmt.Errorf("message %d entity is missing field %d", row.ID, index)
		}
	}
	id, err := redisValueUint(values[0])
	if err != nil || id != row.ID {
		return fmt.Errorf("message %d entity id is %v", row.ID, values[0])
	}
	seq, err := redisValueUint(values[1])
	if err != nil || seq != row.Seq {
		return fmt.Errorf("message %d entity seq is %v, want %d", row.ID, values[1], row.Seq)
	}
	senderID, err := redisValueUint(values[2])
	if err != nil || senderID != row.SenderID {
		return fmt.Errorf("message %d entity sender is %v, want %d", row.ID, values[2], row.SenderID)
	}
	recipientID, err := redisValueUint(values[3])
	if err != nil || recipientID != row.RecipientID {
		return fmt.Errorf(
			"message %d entity recipient is %v, want %d", row.ID, values[3], row.RecipientID,
		)
	}
	body, err := redisValueString(values[4])
	if err != nil || body != row.Body {
		return fmt.Errorf("message %d entity body does not match mysql", row.ID)
	}
	return nil
}

// verifyGroupEntities compares the oldest and newest messages of one group with
// MySQL, including their inbox entries.
func (r *RedisRebuilder) verifyGroupEntities(
	ctx context.Context,
	groupID, maxSeq uint64,
) error {
	keyspace := r.options.Keyspace
	afterSeqs := []uint64{0}
	if maxSeq > 2 {
		afterSeqs = append(afterSeqs, maxSeq-2)
	}
	for _, afterSeq := range afterSeqs {
		rows, err := r.store.ListGroupMessagesBySeqRange(ctx, groupID, afterSeq, 2)
		if err != nil {
			return fmt.Errorf("list group %d entities: %w", groupID, err)
		}
		for _, row := range rows {
			values, err := r.client.HMGet(ctx, keyspace.groupMessageKey(row.ID),
				"id", "seq", "group_id", "sender_id", "body",
				"sender_username", "sender_nickname",
			).Result()
			if err != nil {
				return fmt.Errorf("read group %d message %d: %w", groupID, row.ID, err)
			}
			if err := matchGroupEntity(values, row); err != nil {
				return fmt.Errorf("group %d %w", groupID, err)
			}
			score, err := r.client.ZScore(
				ctx, keyspace.groupInboxKey(groupID), strconv.FormatUint(row.ID, 10),
			).Result()
			if err != nil || uint64(score) != row.Seq {
				return fmt.Errorf(
					"group %d message %d inbox score %v does not equal seq %d",
					groupID, row.ID, score, row.Seq,
				)
			}
		}
	}
	return nil
}

func matchGroupEntity(values []interface{}, row store.GroupMessage) error {
	if len(values) != 7 {
		return fmt.Errorf("message %d entity is missing fields", row.ID)
	}
	for index, value := range values {
		if value == nil {
			return fmt.Errorf("message %d entity is missing field %d", row.ID, index)
		}
	}
	id, err := redisValueUint(values[0])
	if err != nil || id != row.ID {
		return fmt.Errorf("message %d entity id is %v", row.ID, values[0])
	}
	seq, err := redisValueUint(values[1])
	if err != nil || seq != row.Seq {
		return fmt.Errorf("message %d entity seq is %v, want %d", row.ID, values[1], row.Seq)
	}
	groupID, err := redisValueUint(values[2])
	if err != nil || groupID != row.GroupID {
		return fmt.Errorf("message %d entity group is %v, want %d", row.ID, values[2], row.GroupID)
	}
	senderID, err := redisValueUint(values[3])
	if err != nil || senderID != row.SenderID {
		return fmt.Errorf("message %d entity sender is %v, want %d", row.ID, values[3], row.SenderID)
	}
	body, err := redisValueString(values[4])
	if err != nil || body != row.Body {
		return fmt.Errorf("message %d entity body does not match mysql", row.ID)
	}
	// The sender snapshot is the current profile, so only presence is checked.
	if _, err := redisValueString(values[5]); err != nil {
		return fmt.Errorf("message %d entity sender username is invalid", row.ID)
	}
	if _, err := redisValueString(values[6]); err != nil {
		return fmt.Errorf("message %d entity sender nickname is invalid", row.ID)
	}
	return nil
}

func (r *RedisRebuilder) verifyFeeds(ctx context.Context, stats *rebuildStats) error {
	keyspace := r.options.Keyspace
	userIDs := make([]uint64, 0, len(stats.feedCounts))
	for userID := range stats.feedCounts {
		if userID != 0 {
			userIDs = append(userIDs, userID)
		}
	}
	sort.Slice(userIDs, func(i, j int) bool { return userIDs[i] < userIDs[j] })
	for _, userID := range userIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		card, err := r.client.ZCard(ctx, keyspace.directFeedKey(userID)).Result()
		if err != nil {
			return fmt.Errorf("count user %d feed: %w", userID, err)
		}
		if uint64(card) != stats.feedCounts[userID] {
			return fmt.Errorf(
				"user %d feed has %d entries, want %d",
				userID, card, stats.feedCounts[userID],
			)
		}
	}
	return nil
}

// verifyDedup compares the rebuilt dedup layer with the messages the passes
// found inside the dedup window, then reads back a bounded sample.
func (r *RedisRebuilder) verifyDedup(
	ctx context.Context,
	stats *rebuildStats,
) error {
	keyspace := r.options.Keyspace
	prefix := keyspace.prefixOrDefault()
	actual := uint64(0)
	for _, pattern := range []string{prefix + "dedup:*", prefix + "group-dedup:*"} {
		cursor := uint64(0)
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			keys, next, err := r.client.Scan(
				ctx, cursor, pattern, rebuildScanCountHint,
			).Result()
			if err != nil {
				return fmt.Errorf("scan rebuilt dedup keys: %w", err)
			}
			actual += uint64(len(keys))
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
	// Records close to the end of their TTL may expire between the pass and
	// this read, so the count is bounded rather than exact.
	if actual > stats.dedupExpected {
		return fmt.Errorf(
			"rebuilt %d dedup records, want at most %d messages inside the dedup window",
			actual, stats.dedupExpected,
		)
	}
	if actual < stats.dedupDurable {
		return fmt.Errorf(
			"rebuilt %d dedup records, want at least %d messages inside the dedup window",
			actual, stats.dedupDurable,
		)
	}
	for _, sample := range stats.directDedups {
		if err := r.verifyDedupSample(ctx, sample); err != nil {
			return err
		}
	}
	for _, sample := range stats.groupDedups {
		if err := r.verifyDedupSample(ctx, sample); err != nil {
			return err
		}
	}
	return nil
}

func (r *RedisRebuilder) verifyDedupSample(
	ctx context.Context,
	sample rebuildDedupSample,
) error {
	values, err := r.client.HMGet(ctx, sample.key,
		"id", "created_at_ms", "body_hash",
	).Result()
	if err != nil {
		return fmt.Errorf("read rebuilt dedup record: %w", err)
	}
	want := []string{sample.id, sample.createdAtMS, sample.bodyHash}
	if len(values) != len(want) {
		return fmt.Errorf("rebuilt dedup record has %d fields, want %d", len(values), len(want))
	}
	for index, value := range values {
		got, ok := value.(string)
		if !ok || got != want[index] {
			return fmt.Errorf("rebuilt dedup record field %d does not match the message", index)
		}
	}
	ttl, err := r.client.TTL(ctx, sample.key).Result()
	if err != nil {
		return fmt.Errorf("read rebuilt dedup ttl: %w", err)
	}
	if ttl <= 0 || ttl > r.options.DedupTTL {
		return fmt.Errorf("rebuilt dedup record ttl %s is outside the dedup window", ttl)
	}
	return nil
}

// verifyConversationLast compares the sampled users' conversation previews
// with the newest message seen for each peer during the direct pass.
func (r *RedisRebuilder) verifyConversationLast(
	ctx context.Context,
	stats *rebuildStats,
) error {
	keyspace := r.options.Keyspace
	userIDs := make([]uint64, 0, len(stats.convLast))
	for userID := range stats.convLast {
		userIDs = append(userIDs, userID)
	}
	sort.Slice(userIDs, func(i, j int) bool { return userIDs[i] < userIDs[j] })
	for _, userID := range userIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		actual, err := r.client.HGetAll(
			ctx, keyspace.directConversationLastKey(userID),
		).Result()
		if err != nil {
			return fmt.Errorf("read user %d conversation previews: %w", userID, err)
		}
		expected := stats.convLast[userID]
		if len(actual) != len(expected) {
			return fmt.Errorf(
				"user %d has %d conversation previews, want %d",
				userID, len(actual), len(expected),
			)
		}
		for peerID, messageID := range expected {
			raw, ok := actual[strconv.FormatUint(peerID, 10)]
			if !ok {
				return fmt.Errorf(
					"user %d conversation preview for peer %d is missing", userID, peerID,
				)
			}
			gotID, err := strconv.ParseUint(raw, 10, 64)
			if err != nil || gotID != messageID {
				return fmt.Errorf(
					"user %d conversation preview for peer %d is %s, want %d",
					userID, peerID, raw, messageID,
				)
			}
		}
	}
	return nil
}

func (r *RedisRebuilder) verifyWatermarks(
	ctx context.Context,
	stats *rebuildStats,
) error {
	keyspace := r.options.Keyspace
	afterUserID, afterPeerID := uint64(0), uint64(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		states, err := r.store.ListConversationReadStates(
			ctx, afterUserID, afterPeerID, r.options.Batch,
		)
		if err != nil {
			return fmt.Errorf("list conversation read states: %w", err)
		}
		if len(states) == 0 {
			break
		}
		for _, state := range states {
			afterUserID, afterPeerID = state.UserID, state.PeerID
			if state.UserID == 0 || state.PeerID == 0 ||
				state.UserID == state.PeerID {
				continue
			}
			position, err := r.readHashWatermark(
				ctx, keyspace.directReadKey(state.UserID), state.PeerID,
			)
			if err != nil {
				return err
			}
			pair := [2]uint64{}
			pair[0], pair[1] = sortedPair(state.UserID, state.PeerID)
			counter := stats.maxSeqByPair[pair]
			want := state.LastReadSeq
			if want > counter {
				want = counter
			}
			if want > position {
				return fmt.Errorf(
					"conversation %d:%d watermark %d is below durable seq %d",
					state.UserID, state.PeerID, position, want,
				)
			}
			if position > counter {
				return fmt.Errorf(
					"conversation %d:%d watermark %d exceeds counter %d",
					state.UserID, state.PeerID, position, counter,
				)
			}
		}
	}

	groupIDs := make([]uint64, 0, len(stats.groupPositions))
	for groupID := range stats.groupPositions {
		if groupID != 0 {
			groupIDs = append(groupIDs, groupID)
		}
	}
	sort.Slice(groupIDs, func(i, j int) bool { return groupIDs[i] < groupIDs[j] })
	for _, groupID := range groupIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		counter := stats.maxSeqByGroup[groupID]
		positions := stats.groupPositions[groupID]
		memberIDs := make([]uint64, 0, len(positions))
		for userID := range positions {
			if userID != 0 {
				memberIDs = append(memberIDs, userID)
			}
		}
		sort.Slice(memberIDs, func(i, j int) bool { return memberIDs[i] < memberIDs[j] })
		for _, userID := range memberIDs {
			position, err := r.readGroupWatermark(ctx, groupID, userID)
			if err != nil {
				return err
			}
			want := positions[userID]
			if want > counter {
				want = counter
			}
			if want > position {
				return fmt.Errorf(
					"group %d member %d watermark %d is below durable seq %d",
					groupID, userID, position, want,
				)
			}
			if position > counter {
				return fmt.Errorf(
					"group %d member %d watermark %d exceeds counter %d",
					groupID, userID, position, counter,
				)
			}
		}
	}
	return nil
}

func (r *RedisRebuilder) readCounter(ctx context.Context, key string) (uint64, error) {
	raw, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read rebuild counter: %w", err)
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse rebuild counter: %w", err)
	}
	return value, nil
}

func (r *RedisRebuilder) readHashWatermark(
	ctx context.Context,
	key string,
	field uint64,
) (uint64, error) {
	raw, err := r.client.HGet(ctx, key, strconv.FormatUint(field, 10)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read rebuild watermark: %w", err)
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse rebuild watermark: %w", err)
	}
	return value, nil
}

func (r *RedisRebuilder) readGroupWatermark(
	ctx context.Context,
	groupID, userID uint64,
) (uint64, error) {
	score, err := r.client.ZScore(
		ctx,
		r.options.Keyspace.groupReadKey(groupID),
		strconv.FormatUint(userID, 10),
	).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read rebuild group watermark: %w", err)
	}
	if score <= 0 {
		return 0, nil
	}
	return uint64(score), nil
}

// rebuildClearPatterns are the only namespaces a rebuild clears.
var rebuildClearPatterns = []string{
	"messages:*",
	"group-messages:*",
	"dm:*",
	"group:*",
	"friends:*",
	"dedup:*",
	"group-dedup:*",
}

// ClearNamespace deletes the known messaging keys of one keyspace in batches.
// Locks and unknown keys are never touched, and it never flushes a database.
func ClearNamespace(
	ctx context.Context,
	client redis.UniversalClient,
	keyspace Keyspace,
) (int, error) {
	if client == nil {
		return 0, ErrMessageServiceUnavailable
	}
	prefix := keyspace.prefixOrDefault()
	protected := map[string]struct{}{
		RebuildLockKey(keyspace):    {},
		keyspace.backfillLockKey():  {},
		RebuildPendingKey(keyspace): {},
	}
	// Readiness is invalidated before anything is destroyed: an interrupted
	// clear must never leave a namespace that still claims to be complete.
	removed, err := client.Del(
		ctx, keyspace.sequenceReadyKey(), keyspace.indexEpochKey(),
	).Result()
	if err != nil {
		return 0, fmt.Errorf("delete rebuild readiness markers: %w", err)
	}
	deleted := int(removed)
	for _, suffix := range rebuildClearPatterns {
		pattern := prefix + suffix
		cursor := uint64(0)
		for {
			if err := ctx.Err(); err != nil {
				return deleted, err
			}
			keys, next, err := client.Scan(ctx, cursor, pattern, rebuildScanCountHint).Result()
			if err != nil {
				return deleted, fmt.Errorf("scan rebuild keys: %w", err)
			}
			batch := make([]string, 0, len(keys))
			for _, key := range keys {
				if _, ok := protected[key]; ok {
					continue
				}
				batch = append(batch, key)
			}
			if len(batch) > 0 {
				removed, err := client.Del(ctx, batch...).Result()
				if err != nil {
					return deleted, fmt.Errorf("delete rebuild keys: %w", err)
				}
				deleted += int(removed)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}

	// Exact keys that no pattern above is guaranteed to cover.
	exact := []string{
		keyspace.counterKey(),
		keyspace.messageStreamKey(),
		keyspace.groupMessageStreamKey(),
		keyspace.directReadStreamKey(),
		keyspace.groupReadStreamKey(),
	}
	removed, err = client.Del(ctx, exact...).Result()
	if err != nil {
		return deleted, fmt.Errorf("delete rebuild keys: %w", err)
	}
	return deleted + int(removed), nil
}
