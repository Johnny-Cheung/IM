package messaging

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// SeqBackfillStore is the durable surface the seq backfill needs. It only uses
// store methods that exist for the v1.8.1 switch.
type SeqBackfillStore interface {
	ListMessageSeqIndex(
		ctx context.Context,
		afterServerMessageID uint64,
		limit int,
	) ([]store.MessageSeqIndex, error)
	ListGroupMessageSeqIndex(
		ctx context.Context,
		afterServerMessageID uint64,
		limit int,
	) ([]store.GroupMessageSeqIndex, error)
	ListConversationSeqMax(
		ctx context.Context,
		afterLowID, afterHighID uint64,
		limit int,
	) ([]store.ConversationSeqMax, error)
	ListGroupSeqMax(
		ctx context.Context,
		afterGroupID uint64,
		limit int,
	) ([]store.GroupSeqMax, error)
	ListConversationReadStates(
		ctx context.Context,
		afterUserID, afterPeerID uint64,
		limit int,
	) ([]store.ConversationReadState, error)
	ListGroupMemberReadStates(
		ctx context.Context,
		afterGroupID, afterUserID uint64,
		limit int,
	) ([]store.GroupMemberReadState, error)
	CountDirectMessagesByPair(ctx context.Context) ([]store.DirectPairCount, error)
	CountGroupMessagesByGroup(ctx context.Context) (map[uint64]uint64, error)
	CountUnnumberedMessages(ctx context.Context) (uint64, uint64, error)
	FillMessageConversationPairs(ctx context.Context, limit int) (int, error)
	AssignMessageSeqs(ctx context.Context, limit int) (int, error)
	AssignGroupMessageSeqs(ctx context.Context, limit int) (int, error)
	DirectSeqAtOrBefore(
		ctx context.Context,
		lowID, highID, serverMessageID uint64,
	) (uint64, error)
	GroupSeqAtOrBefore(
		ctx context.Context,
		groupID, serverMessageID uint64,
	) (uint64, error)
	UpsertConversationRead(
		ctx context.Context,
		userID, peerID, lastReadSeq uint64,
		updatedAt time.Time,
	) error
	UpsertGroupRead(ctx context.Context, groupID, userID, lastReadSeq uint64) (bool, error)
}

// SeqBackfillResult reports what each step of the one-off switch processed.
type SeqBackfillResult struct {
	PrecheckedPairs          int
	PrecheckedGroups         int
	PairsFilled              int
	MessagesNumbered         int
	GroupMessagesNumbered    int
	PairEntriesIndexed       int
	GroupInboxEntriesIndexed int
	EntitiesPatched          int
	CountersSet              int
	ReadPositionsConverted   int
	ReadPositionsVerified    int
	ConversationsVerified    int
	GroupsVerified           int
	MemoryBefore             string
	MemoryAfter              string
}

// writeMaxHSetScript raises a hash field only when the new value is greater,
// or rewrites it when the stored value exceeds the counter (an impossible seq
// that must be clamped). It is used once seq:ready exists, so all values are in
// the same unit.
var writeMaxHSetScript = redis.NewScript(`
local current = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
local next = tonumber(ARGV[2])
local counter = tonumber(ARGV[3])
if next > current or current > counter then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
  return 1
end
return 0
`)

// writeMaxZAddScript mirrors writeMaxHSetScript for group read ZSETs.
var writeMaxZAddScript = redis.NewScript(`
local current = tonumber(redis.call('ZSCORE', KEYS[1], ARGV[1]) or '0')
local next = tonumber(ARGV[2])
local counter = tonumber(ARGV[3])
if next > current or current > counter then
  redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
  return 1
end
return 0
`)

// SeqBackfillLockTTL bounds a backfill lock. It matches the backfill command's
// own 30-minute deadline.
const SeqBackfillLockTTL = 30 * time.Minute

// BackfillLockKey exposes the Redis key a running seq backfill holds so the
// server and the command can agree on it.
func BackfillLockKey(keyspace Keyspace) string {
	return keyspace.backfillLockKey()
}

// SeqBackfill performs the one-off switch to per-conversation seq. The caller
// must run the MySQL migrations and stop the backend first. It is idempotent
// and never lowers a counter.
type SeqBackfill struct {
	client    redis.UniversalClient
	keyspace  Keyspace
	store     SeqBackfillStore
	batchSize int
}

func NewSeqBackfill(
	client redis.UniversalClient,
	keyspace Keyspace,
	dataStore SeqBackfillStore,
	batchSize int,
) *SeqBackfill {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &SeqBackfill{
		client:    client,
		keyspace:  keyspace,
		store:     dataStore,
		batchSize: batchSize,
	}
}

// patchEntitySeqScriptSource only writes seq into an entity that already
// exists, so old messages stay absent from Redis and are served from MySQL.
const patchEntitySeqScriptSource = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('HSET', KEYS[1], 'seq', ARGV[1])
  return 1
end
return 0
`

// SeqBackfill runs steps 1-6 of the switch.
func (b *SeqBackfill) Run(ctx context.Context) (SeqBackfillResult, error) {
	if b == nil || b.client == nil || b.store == nil {
		return SeqBackfillResult{}, ErrMessageServiceUnavailable
	}
	result := SeqBackfillResult{MemoryBefore: redisUsedMemory(ctx, b.client)}

	if err := b.precheck(ctx, &result); err != nil {
		return result, err
	}
	if err := b.numberMessages(ctx, &result); err != nil {
		return result, err
	}
	if err := b.rebuildRedis(ctx, &result); err != nil {
		return result, err
	}
	ready, err := b.sequenceReady(ctx)
	if err != nil {
		return result, err
	}
	if err := b.convertReadPositions(ctx, &result, ready); err != nil {
		return result, err
	}
	if err := b.verify(ctx, &result); err != nil {
		return result, err
	}
	if err := b.client.Set(ctx, b.keyspace.sequenceReadyKey(), "1", 0).Err(); err != nil {
		return result, fmt.Errorf("mark sequence ready: %w", err)
	}
	result.MemoryAfter = redisUsedMemory(ctx, b.client)
	return result, nil
}

// sequenceReady reports whether a completed backfill already swore that every
// Redis watermark is in seq units. It is the only global proof: a partial run
// leaves no marker behind, so its remaining watermarks are still legacy global
// message IDs and must be converted again.
func (b *SeqBackfill) sequenceReady(ctx context.Context) (bool, error) {
	count, err := b.client.Exists(ctx, b.keyspace.sequenceReadyKey()).Result()
	if err != nil {
		return false, fmt.Errorf("read conversion state: %w", err)
	}
	return count > 0, nil
}

// precheck aborts when Redis holds accepted messages that MySQL does not.
func (b *SeqBackfill) precheck(
	ctx context.Context,
	result *SeqBackfillResult,
) error {
	prefix := b.keyspace.prefixOrDefault()

	pairs, err := b.store.CountDirectMessagesByPair(ctx)
	if err != nil {
		return fmt.Errorf("count persisted direct messages: %w", err)
	}
	directCounts := make(map[[2]uint64]uint64, len(pairs))
	for _, pair := range pairs {
		directCounts[[2]uint64{pair.RecipientID, pair.SenderID}] = pair.Count
	}
	legacyKeys, err := scanKeys(ctx, b.client, prefix+"dm:conv:*")
	if err != nil {
		return fmt.Errorf("scan legacy conversation indexes: %w", err)
	}
	for _, key := range legacyKeys {
		recipientID, senderID, ok := parseLegacyDirectKey(prefix, key)
		if !ok {
			continue
		}
		if err := b.checkLegacyCount(
			ctx, key, directCounts[[2]uint64{recipientID, senderID}],
			"conversation", senderID, recipientID,
		); err != nil {
			return err
		}
		delete(directCounts, [2]uint64{recipientID, senderID})
		result.PrecheckedPairs++
	}
	result.PrecheckedPairs += len(directCounts)

	groups, err := b.store.CountGroupMessagesByGroup(ctx)
	if err != nil {
		return fmt.Errorf("count persisted group messages: %w", err)
	}
	groupCounts := make(map[uint64]uint64, len(groups))
	for groupID, count := range groups {
		groupCounts[groupID] = count
	}
	legacyKeys, err = scanKeys(ctx, b.client, prefix+"group:*:inbox")
	if err != nil {
		return fmt.Errorf("scan group inboxes: %w", err)
	}
	for _, key := range legacyKeys {
		groupID, ok := parseLegacyGroupInboxKey(prefix, key)
		if !ok {
			continue
		}
		if err := b.checkLegacyCount(
			ctx, key, groupCounts[groupID], "group", groupID,
		); err != nil {
			return err
		}
		delete(groupCounts, groupID)
		result.PrecheckedGroups++
	}
	result.PrecheckedGroups += len(groupCounts)
	return nil
}

func (b *SeqBackfill) checkLegacyCount(
	ctx context.Context,
	key string,
	persisted uint64,
	kind string,
	ids ...uint64,
) error {
	indexed, err := b.client.ZCard(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("count legacy %s index: %w", kind, err)
	}
	if uint64(indexed) > persisted {
		return fmt.Errorf(
			"%s %v has %d accepted messages but only %d persisted; drain the persist queue first",
			kind, ids, indexed, persisted,
		)
	}
	return nil
}

// numberMessages fills pair columns and numbers unnumbered rows in MySQL.
func (b *SeqBackfill) numberMessages(
	ctx context.Context,
	result *SeqBackfillResult,
) error {
	for {
		filled, err := b.store.FillMessageConversationPairs(ctx, b.batchSize)
		if err != nil {
			return fmt.Errorf("fill conversation pairs: %w", err)
		}
		result.PairsFilled += filled
		if filled == 0 {
			break
		}
	}
	for {
		numbered, err := b.store.AssignMessageSeqs(ctx, b.batchSize)
		if err != nil {
			return fmt.Errorf("assign message seqs: %w", err)
		}
		result.MessagesNumbered += numbered
		if numbered == 0 {
			break
		}
	}
	for {
		numbered, err := b.store.AssignGroupMessageSeqs(ctx, b.batchSize)
		if err != nil {
			return fmt.Errorf("assign group message seqs: %w", err)
		}
		result.GroupMessagesNumbered += numbered
		if numbered == 0 {
			break
		}
	}
	return nil
}

// rebuildRedis builds dm:pair, rewrites the group inbox by seq, raises the
// counters, patches existing entities, and deletes the retired keys.
func (b *SeqBackfill) rebuildRedis(
	ctx context.Context,
	result *SeqBackfillResult,
) error {
	afterMessageID := uint64(0)
	for {
		rows, err := b.store.ListMessageSeqIndex(ctx, afterMessageID, b.batchSize)
		if err != nil {
			return fmt.Errorf("list message seq index: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		pipe := b.client.Pipeline()
		patches := make([]*redis.Cmd, 0, len(rows))
		for _, row := range rows {
			afterMessageID = row.ServerMessageID
			if row.Seq == 0 || row.LowID == 0 || row.HighID == 0 || row.LowID == row.HighID {
				continue
			}
			member := strconv.FormatUint(row.ServerMessageID, 10)
			pipe.ZAdd(
				ctx,
				b.keyspace.directPairKey(row.LowID, row.HighID),
				redis.Z{Score: float64(row.Seq), Member: member},
			)
			patches = append(patches, pipe.Eval(
				ctx,
				patchEntitySeqScriptSource,
				[]string{b.keyspace.messageKey(row.ServerMessageID)},
				row.Seq,
			))
			result.PairEntriesIndexed++
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("rebuild conversation pair index: %w", err)
		}
		for _, patch := range patches {
			if patched, err := patch.Int64(); err == nil && patched == 1 {
				result.EntitiesPatched++
			}
		}
	}

	afterGroupMessageID := uint64(0)
	for {
		rows, err := b.store.ListGroupMessageSeqIndex(
			ctx, afterGroupMessageID, b.batchSize,
		)
		if err != nil {
			return fmt.Errorf("list group message seq index: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		pipe := b.client.Pipeline()
		patches := make([]*redis.Cmd, 0, len(rows))
		for _, row := range rows {
			afterGroupMessageID = row.ServerMessageID
			if row.Seq == 0 || row.GroupID == 0 {
				continue
			}
			member := strconv.FormatUint(row.ServerMessageID, 10)
			pipe.ZAdd(
				ctx,
				b.keyspace.groupInboxKey(row.GroupID),
				redis.Z{Score: float64(row.Seq), Member: member},
			)
			patches = append(patches, pipe.Eval(
				ctx,
				patchEntitySeqScriptSource,
				[]string{b.keyspace.groupMessageKey(row.ServerMessageID)},
				row.Seq,
			))
			result.GroupInboxEntriesIndexed++
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("rebuild group inbox: %w", err)
		}
		for _, patch := range patches {
			if patched, err := patch.Int64(); err == nil && patched == 1 {
				result.EntitiesPatched++
			}
		}
	}

	if err := b.raiseCounters(ctx, result); err != nil {
		return err
	}
	return b.deleteLegacyKeys(ctx)
}

func (b *SeqBackfill) raiseCounters(
	ctx context.Context,
	result *SeqBackfillResult,
) error {
	afterLowID, afterHighID := uint64(0), uint64(0)
	for {
		rows, err := b.store.ListConversationSeqMax(
			ctx, afterLowID, afterHighID, b.batchSize,
		)
		if err != nil {
			return fmt.Errorf("list conversation seq max: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		pipe := b.client.Pipeline()
		for _, row := range rows {
			afterLowID, afterHighID = row.LowID, row.HighID
			if row.MaxSeq == 0 || row.LowID == 0 || row.HighID == 0 || row.LowID == row.HighID {
				continue
			}
			pipe.Eval(
				ctx,
				raiseSequenceScriptSource,
				[]string{b.keyspace.directSequenceKey(row.LowID, row.HighID)},
				row.MaxSeq,
			)
			result.CountersSet++
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("raise conversation counters: %w", err)
		}
	}

	afterGroupID := uint64(0)
	for {
		rows, err := b.store.ListGroupSeqMax(ctx, afterGroupID, b.batchSize)
		if err != nil {
			return fmt.Errorf("list group seq max: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		pipe := b.client.Pipeline()
		for _, row := range rows {
			afterGroupID = row.GroupID
			if row.MaxSeq == 0 || row.GroupID == 0 {
				continue
			}
			pipe.Eval(
				ctx,
				raiseSequenceScriptSource,
				[]string{b.keyspace.groupSequenceKey(row.GroupID)},
				row.MaxSeq,
			)
			result.CountersSet++
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("raise group counters: %w", err)
		}
	}
	return nil
}

// deleteLegacyKeys removes the directional index, the last-accepted markers
// and the old read streams.
func (b *SeqBackfill) deleteLegacyKeys(ctx context.Context) error {
	pattern := b.keyspace.prefixOrDefault() + "dm:conv:*"
	if err := deleteKeysByPattern(ctx, b.client, pattern); err != nil {
		return fmt.Errorf("delete legacy conversation indexes: %w", err)
	}

	prefix := b.keyspace.prefixOrDefault()
	afterGroupID := uint64(0)
	for {
		rows, err := b.store.ListGroupSeqMax(ctx, afterGroupID, b.batchSize)
		if err != nil {
			return fmt.Errorf("list groups for legacy cleanup: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		groupIDs := make([]string, 0, len(rows))
		for _, row := range rows {
			afterGroupID = row.GroupID
			if row.GroupID == 0 {
				continue
			}
			groupIDs = append(groupIDs, legacyGroupLastAcceptedKey(prefix, row.GroupID))
		}
		if len(groupIDs) > 0 {
			if err := b.client.Del(ctx, groupIDs...).Err(); err != nil {
				return fmt.Errorf("delete legacy group markers: %w", err)
			}
		}
	}

	if err := b.client.Del(
		ctx,
		b.keyspace.directReadStreamKey(),
		b.keyspace.groupReadStreamKey(),
	).Err(); err != nil {
		return fmt.Errorf("delete legacy read streams: %w", err)
	}
	return nil
}

func deleteKeysByPattern(
	ctx context.Context,
	client redis.UniversalClient,
	pattern string,
) error {
	keys, err := scanKeys(ctx, client, pattern)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	return client.Del(ctx, keys...).Err()
}

func scanKeys(
	ctx context.Context,
	client redis.UniversalClient,
	pattern string,
) ([]string, error) {
	var cursor uint64
	keys := make([]string, 0)
	for {
		batch, next, err := client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			return keys, nil
		}
	}
}

// convertReadPositions turns the old global-ID watermarks into seq. When
// seq:ready is absent every Redis watermark is legacy, so it is converted;
// when it is present Redis already holds seq and is only clamped to the
// counter. The durable last_read_seq is always a floor.
func (b *SeqBackfill) convertReadPositions(
	ctx context.Context,
	result *SeqBackfillResult,
	ready bool,
) error {
	now := time.Now().UTC()
	handledDirect := make(map[[2]uint64]struct{})
	handledGroup := make(map[[2]uint64]struct{})
	afterUserID, afterPeerID := uint64(0), uint64(0)
	for {
		rows, err := b.store.ListConversationReadStates(
			ctx, afterUserID, afterPeerID, b.batchSize,
		)
		if err != nil {
			return fmt.Errorf("list conversation read states: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			afterUserID, afterPeerID = row.UserID, row.PeerID
			if row.UserID == 0 || row.PeerID == 0 || row.UserID == row.PeerID {
				continue
			}
			low, high := sortedPair(row.UserID, row.PeerID)
			counter, err := b.readCounter(
				ctx, b.keyspace.directSequenceKey(low, high),
			)
			if err != nil {
				return err
			}
			redisPosition, err := b.redisReadPosition(ctx, row.UserID, row.PeerID)
			if err != nil {
				return err
			}
			seq, err := b.readSeq(
				ctx, row.LastReadSeq, row.LastReadMessageID, redisPosition, counter,
				ready,
				func(ctx context.Context, position uint64) (uint64, error) {
					return b.store.DirectSeqAtOrBefore(ctx, low, high, position)
				},
			)
			if err != nil {
				return fmt.Errorf("convert conversation read position: %w", err)
			}
			handledDirect[[2]uint64{row.UserID, row.PeerID}] = struct{}{}
			if seq == 0 && row.LastReadSeq == 0 &&
				row.LastReadMessageID == 0 && redisPosition == 0 {
				continue
			}
			if err := b.writeDirectReadSeq(
				ctx, ready, row.UserID, row.PeerID, seq, counter,
			); err != nil {
				return fmt.Errorf("write conversation read position: %w", err)
			}
			if seq > 0 {
				if err := b.store.UpsertConversationRead(
					ctx, row.UserID, row.PeerID, seq, now,
				); err != nil {
					return fmt.Errorf("persist conversation read seq: %w", err)
				}
			}
			result.ReadPositionsConverted++
		}
	}

	afterGroupID, afterMemberID := uint64(0), uint64(0)
	for {
		rows, err := b.store.ListGroupMemberReadStates(
			ctx, afterGroupID, afterMemberID, b.batchSize,
		)
		if err != nil {
			return fmt.Errorf("list group member read states: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			afterGroupID, afterMemberID = row.GroupID, row.UserID
			if row.GroupID == 0 || row.UserID == 0 {
				continue
			}
			counter, err := b.readCounter(
				ctx, b.keyspace.groupSequenceKey(row.GroupID),
			)
			if err != nil {
				return err
			}
			redisPosition, err := b.redisGroupReadPosition(ctx, row.GroupID, row.UserID)
			if err != nil {
				return err
			}
			seq, err := b.readSeq(
				ctx, row.LastReadSeq, row.LastReadMessageID, redisPosition, counter,
				ready,
				func(ctx context.Context, position uint64) (uint64, error) {
					return b.store.GroupSeqAtOrBefore(ctx, row.GroupID, position)
				},
			)
			if err != nil {
				return fmt.Errorf("convert group read position: %w", err)
			}
			handledGroup[[2]uint64{row.GroupID, row.UserID}] = struct{}{}
			if seq == 0 && row.LastReadSeq == 0 &&
				row.LastReadMessageID == 0 && redisPosition == 0 {
				continue
			}
			if err := b.writeGroupReadSeq(
				ctx, ready, row.GroupID, row.UserID, seq, counter,
			); err != nil {
				return fmt.Errorf("write group read position: %w", err)
			}
			if seq > 0 {
				if _, err := b.store.UpsertGroupRead(
					ctx, row.GroupID, row.UserID, seq,
				); err != nil {
					return fmt.Errorf("persist group read seq: %w", err)
				}
			}
			result.ReadPositionsConverted++
		}
	}

	if err := b.convertRedisOnlyReadPositions(ctx, handledDirect, handledGroup, now, result, ready); err != nil {
		return err
	}
	return nil
}

// readSeq resolves a durable read row plus its Redis watermark into seq units.
// The durable last_read_seq is a floor. A legacy last_read_message_id is a
// global ID and is converted. Redis is seq only when seq:ready proves a
// completed conversion; otherwise it is a legacy global ID and is converted.
func (b *SeqBackfill) readSeq(
	ctx context.Context,
	lastReadSeq, lastReadMessageID, redisPosition, counter uint64,
	ready bool,
	convert func(context.Context, uint64) (uint64, error),
) (uint64, error) {
	seq := lastReadSeq
	if lastReadMessageID > 0 {
		converted, err := convert(ctx, lastReadMessageID)
		if err != nil {
			return 0, err
		}
		if converted > seq {
			seq = converted
		}
	}
	if redisPosition > 0 {
		redisSeq, err := b.redisPositionSeq(ctx, redisPosition, counter, ready, convert)
		if err != nil {
			return 0, err
		}
		if redisSeq > seq {
			seq = redisSeq
		}
	}
	return seq, nil
}

// redisPositionSeq interprets a Redis watermark. Only seq:ready proves the
// whole model is in seq units; without it the value is a legacy global message
// ID and must be converted. A crash-window conversion can only lower a seq
// value, which keeps unread conservative, so conversion is always safe.
func (b *SeqBackfill) redisPositionSeq(
	ctx context.Context,
	position, counter uint64,
	ready bool,
	convert func(context.Context, uint64) (uint64, error),
) (uint64, error) {
	if position == 0 {
		return 0, nil
	}
	if !ready {
		return convert(ctx, position)
	}
	if position > counter {
		return counter, nil
	}
	return position, nil
}

// writeDirectReadSeq writes a seq watermark. Once seq:ready exists the stored
// value is comparable, so the write never lowers it; before that the stored
// value is a global ID and must be replaced by the converted seq.
func (b *SeqBackfill) writeDirectReadSeq(
	ctx context.Context,
	ready bool,
	userID, peerID, seq, counter uint64,
) error {
	key := b.keyspace.directReadKey(userID)
	field := strconv.FormatUint(peerID, 10)
	value := strconv.FormatUint(seq, 10)
	if !ready {
		return b.client.HSet(ctx, key, field, value).Err()
	}
	counterValue := strconv.FormatUint(counter, 10)
	return writeMaxHSetScript.Run(
		ctx, b.client, []string{key}, field, value, counterValue,
	).Err()
}

// writeGroupReadSeq mirrors writeDirectReadSeq for the group ZSET.
func (b *SeqBackfill) writeGroupReadSeq(
	ctx context.Context,
	ready bool,
	groupID, userID, seq, counter uint64,
) error {
	key := b.keyspace.groupReadKey(groupID)
	member := strconv.FormatUint(userID, 10)
	value := strconv.FormatUint(seq, 10)
	if !ready {
		return b.client.ZAdd(ctx, key, redis.Z{Score: float64(seq), Member: member}).Err()
	}
	counterValue := strconv.FormatUint(counter, 10)
	return writeMaxZAddScript.Run(
		ctx, b.client, []string{key}, member, value, counterValue,
	).Err()
}

// convertRedisOnlyReadPositions converts watermarks whose pair has no durable
// read row, which the row loops above cannot see.
func (b *SeqBackfill) convertRedisOnlyReadPositions(
	ctx context.Context,
	handledDirect, handledGroup map[[2]uint64]struct{},
	now time.Time,
	result *SeqBackfillResult,
	ready bool,
) error {
	prefix := b.keyspace.prefixOrDefault()
	keys, err := scanKeys(ctx, b.client, prefix+"dm:read:*")
	if err != nil {
		return fmt.Errorf("scan conversation read keys: %w", err)
	}
	for _, key := range keys {
		userID, ok := parseDirectReadKey(prefix, key)
		if !ok {
			continue
		}
		values, err := b.client.HGetAll(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("read conversation watermarks: %w", err)
		}
		for field, value := range values {
			peerID, err := strconv.ParseUint(field, 10, 64)
			if err != nil || peerID == 0 || peerID == userID {
				continue
			}
			if _, ok := handledDirect[[2]uint64{userID, peerID}]; ok {
				continue
			}
			position, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return fmt.Errorf("parse conversation watermark: %w", err)
			}
			low, high := sortedPair(userID, peerID)
			counter, err := b.readCounter(
				ctx, b.keyspace.directSequenceKey(low, high),
			)
			if err != nil {
				return err
			}
			seq, err := b.redisPositionSeq(
				ctx, position, counter, ready,
				func(ctx context.Context, position uint64) (uint64, error) {
					return b.store.DirectSeqAtOrBefore(ctx, low, high, position)
				},
			)
			if err != nil {
				return fmt.Errorf("convert conversation read position: %w", err)
			}
			if err := b.writeDirectReadSeq(ctx, ready, userID, peerID, seq, counter); err != nil {
				return fmt.Errorf("write conversation read position: %w", err)
			}
			if seq > 0 {
				if err := b.store.UpsertConversationRead(
					ctx, userID, peerID, seq, now,
				); err != nil {
					return fmt.Errorf("persist conversation read seq: %w", err)
				}
			}
			result.ReadPositionsConverted++
		}
	}

	keys, err = scanKeys(ctx, b.client, prefix+"group:*:read")
	if err != nil {
		return fmt.Errorf("scan group read keys: %w", err)
	}
	for _, key := range keys {
		groupID, ok := parseGroupReadKey(prefix, key)
		if !ok {
			continue
		}
		values, err := b.client.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return fmt.Errorf("read group watermarks: %w", err)
		}
		counter := uint64(0)
		for _, value := range values {
			member, ok := value.Member.(string)
			if !ok {
				continue
			}
			userID, err := strconv.ParseUint(member, 10, 64)
			if err != nil || userID == 0 {
				continue
			}
			if _, ok := handledGroup[[2]uint64{groupID, userID}]; ok {
				continue
			}
			position := uint64(0)
			if value.Score > 0 {
				position = uint64(value.Score)
			}
			if counter == 0 {
				counter, err = b.readCounter(
					ctx, b.keyspace.groupSequenceKey(groupID),
				)
				if err != nil {
					return err
				}
			}
			seq, err := b.redisPositionSeq(
				ctx, position, counter, ready,
				func(ctx context.Context, position uint64) (uint64, error) {
					return b.store.GroupSeqAtOrBefore(ctx, groupID, position)
				},
			)
			if err != nil {
				return fmt.Errorf("convert group read position: %w", err)
			}
			if err := b.writeGroupReadSeq(ctx, ready, groupID, userID, seq, counter); err != nil {
				return fmt.Errorf("write group read position: %w", err)
			}
			if seq > 0 {
				if _, err := b.store.UpsertGroupRead(
					ctx, groupID, userID, seq,
				); err != nil {
					return fmt.Errorf("persist group read seq: %w", err)
				}
			}
			result.ReadPositionsConverted++
		}
	}
	return nil
}

func parseDirectReadKey(prefix, key string) (uint64, bool) {
	rest, ok := strings.CutPrefix(key, prefix+"dm:read:")
	if !ok || strings.Contains(rest, ":") {
		return 0, false
	}
	userID, err := strconv.ParseUint(rest, 10, 64)
	if err != nil || userID == 0 {
		return 0, false
	}
	return userID, true
}

func parseGroupReadKey(prefix, key string) (uint64, bool) {
	rest, ok := strings.CutPrefix(key, prefix+"group:")
	if !ok {
		return 0, false
	}
	rest, ok = strings.CutSuffix(rest, ":read")
	if !ok || strings.Contains(rest, ":") {
		return 0, false
	}
	groupID, err := strconv.ParseUint(rest, 10, 64)
	if err != nil || groupID == 0 {
		return 0, false
	}
	return groupID, true
}

func (b *SeqBackfill) redisReadPosition(
	ctx context.Context,
	userID, peerID uint64,
) (uint64, error) {
	raw, err := b.client.HGet(
		ctx,
		b.keyspace.directReadKey(userID),
		strconv.FormatUint(peerID, 10),
	).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read conversation watermark: %w", err)
	}
	return strconv.ParseUint(raw, 10, 64)
}

func (b *SeqBackfill) redisGroupReadPosition(
	ctx context.Context,
	groupID, userID uint64,
) (uint64, error) {
	score, err := b.client.ZScore(
		ctx,
		b.keyspace.groupReadKey(groupID),
		strconv.FormatUint(userID, 10),
	).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read group watermark: %w", err)
	}
	if score <= 0 {
		return 0, nil
	}
	return uint64(score), nil
}

// verify checks count == max seq, index sizes and watermark bounds before the
// readiness marker is written.
func (b *SeqBackfill) verify(
	ctx context.Context,
	result *SeqBackfillResult,
) error {
	directUnnumbered, groupUnnumbered, err := b.store.CountUnnumberedMessages(ctx)
	if err != nil {
		return fmt.Errorf("count unnumbered messages: %w", err)
	}
	if directUnnumbered > 0 || groupUnnumbered > 0 {
		return fmt.Errorf(
			"unnumbered messages remain: direct=%d group=%d",
			directUnnumbered, groupUnnumbered,
		)
	}

	afterLowID, afterHighID := uint64(0), uint64(0)
	for {
		rows, err := b.store.ListConversationSeqMax(
			ctx, afterLowID, afterHighID, b.batchSize,
		)
		if err != nil {
			return fmt.Errorf("list conversation seq max: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			afterLowID, afterHighID = row.LowID, row.HighID
			if row.Count != row.MaxSeq {
				return fmt.Errorf(
					"conversation %d:%d count %d does not equal max seq %d",
					row.LowID, row.HighID, row.Count, row.MaxSeq,
				)
			}
			indexed, err := b.client.ZCard(
				ctx, b.keyspace.directPairKey(row.LowID, row.HighID),
			).Result()
			if err != nil {
				return fmt.Errorf("count conversation pair index: %w", err)
			}
			if uint64(indexed) != row.Count {
				return fmt.Errorf(
					"conversation %d:%d index has %d entries, mysql has %d",
					row.LowID, row.HighID, indexed, row.Count,
				)
			}
			result.ConversationsVerified++
		}
	}

	afterGroupID := uint64(0)
	for {
		rows, err := b.store.ListGroupSeqMax(ctx, afterGroupID, b.batchSize)
		if err != nil {
			return fmt.Errorf("list group seq max: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			afterGroupID = row.GroupID
			if row.Count != row.MaxSeq {
				return fmt.Errorf(
					"group %d count %d does not equal max seq %d",
					row.GroupID, row.Count, row.MaxSeq,
				)
			}
			indexed, err := b.client.ZCard(
				ctx, b.keyspace.groupInboxKey(row.GroupID),
			).Result()
			if err != nil {
				return fmt.Errorf("count group inbox: %w", err)
			}
			if uint64(indexed) != row.Count {
				return fmt.Errorf(
					"group %d inbox has %d entries, mysql has %d",
					row.GroupID, indexed, row.Count,
				)
			}
			result.GroupsVerified++
		}
	}

	return b.verifyWatermarks(ctx, result)
}

func (b *SeqBackfill) verifyWatermarks(
	ctx context.Context,
	result *SeqBackfillResult,
) error {
	afterUserID, afterPeerID := uint64(0), uint64(0)
	for {
		rows, err := b.store.ListConversationReadStates(
			ctx, afterUserID, afterPeerID, b.batchSize,
		)
		if err != nil {
			return fmt.Errorf("list conversation read states: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			afterUserID, afterPeerID = row.UserID, row.PeerID
			if row.UserID == 0 || row.PeerID == 0 || row.UserID == row.PeerID {
				continue
			}
			position, err := b.redisReadPosition(ctx, row.UserID, row.PeerID)
			if err != nil {
				return err
			}
			if row.LastReadSeq > position {
				return fmt.Errorf(
					"conversation %d:%d watermark %d below durable seq %d",
					row.UserID, row.PeerID, position, row.LastReadSeq,
				)
			}
			counter, err := b.readCounter(
				ctx, b.keyspace.directSequenceKey(row.UserID, row.PeerID),
			)
			if err != nil {
				return err
			}
			if position > counter {
				return fmt.Errorf(
					"conversation %d:%d watermark %d exceeds counter %d",
					row.UserID, row.PeerID, position, counter,
				)
			}
			result.ReadPositionsVerified++
		}
	}

	afterGroupID, afterMemberID := uint64(0), uint64(0)
	for {
		rows, err := b.store.ListGroupMemberReadStates(
			ctx, afterGroupID, afterMemberID, b.batchSize,
		)
		if err != nil {
			return fmt.Errorf("list group member read states: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			afterGroupID, afterMemberID = row.GroupID, row.UserID
			if row.GroupID == 0 || row.UserID == 0 {
				continue
			}
			position, err := b.redisGroupReadPosition(ctx, row.GroupID, row.UserID)
			if err != nil {
				return err
			}
			if row.LastReadSeq > position {
				return fmt.Errorf(
					"group %d member %d watermark %d below durable seq %d",
					row.GroupID, row.UserID, position, row.LastReadSeq,
				)
			}
			counter, err := b.readCounter(ctx, b.keyspace.groupSequenceKey(row.GroupID))
			if err != nil {
				return err
			}
			if position > counter {
				return fmt.Errorf(
					"group %d member %d watermark %d exceeds counter %d",
					row.GroupID, row.UserID, position, counter,
				)
			}
			result.ReadPositionsVerified++
		}
	}
	return nil
}

func (b *SeqBackfill) readCounter(ctx context.Context, key string) (uint64, error) {
	raw, err := b.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read conversation counter: %w", err)
	}
	return strconv.ParseUint(raw, 10, 64)
}

// The retired keys are only reconstructed here for the pre-switch check and
// cleanup; nothing else in the package may use them.
func legacyDirectConversationKey(prefix string, recipientID, senderID uint64) string {
	return fmt.Sprintf("%sdm:conv:%d:%d", prefix, recipientID, senderID)
}

func legacyGroupLastAcceptedKey(prefix string, groupID uint64) string {
	return fmt.Sprintf("%sgroup:%d:last_accepted_id", prefix, groupID)
}

func parseLegacyDirectKey(prefix, key string) (uint64, uint64, bool) {
	rest, ok := strings.CutPrefix(key, prefix+"dm:conv:")
	if !ok {
		return 0, 0, false
	}
	parts := strings.Split(rest, ":")
	if len(parts) != 2 {
		return 0, 0, false
	}
	recipientID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || recipientID == 0 {
		return 0, 0, false
	}
	senderID, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || senderID == 0 {
		return 0, 0, false
	}
	return recipientID, senderID, true
}

func parseLegacyGroupInboxKey(prefix, key string) (uint64, bool) {
	rest, ok := strings.CutPrefix(key, prefix+"group:")
	if !ok {
		return 0, false
	}
	rest, ok = strings.CutSuffix(rest, ":inbox")
	if !ok || strings.Contains(rest, ":") {
		return 0, false
	}
	groupID, err := strconv.ParseUint(rest, 10, 64)
	if err != nil || groupID == 0 {
		return 0, false
	}
	return groupID, true
}

func redisUsedMemory(ctx context.Context, client redis.UniversalClient) string {
	info, err := client.Info(ctx, "memory").Result()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "used_memory_human:") {
			return strings.TrimPrefix(line, "used_memory_human:")
		}
	}
	return ""
}
