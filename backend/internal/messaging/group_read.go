package messaging

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/group_read_advance.lua
var groupReadAdvanceScriptSource string

//go:embed lua/group_read_advance_many.lua
var groupReadAdvanceManyScriptSource string

//go:embed lua/group_read_stats.lua
var groupReadStatsScriptSource string

//go:embed lua/group_read_reconcile.lua
var groupReadReconcileScriptSource string

var (
	groupReadAdvanceScript = redis.NewScript(groupReadAdvanceScriptSource)
	groupReadAdvanceMany   = redis.NewScript(groupReadAdvanceManyScriptSource)
	groupReadStats         = redis.NewScript(groupReadStatsScriptSource)
	groupReadReconcile     = redis.NewScript(groupReadReconcileScriptSource)
	groupReadSeedMember    = redis.NewScript(`
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return 1
`)
	groupReadRaiseMember = redis.NewScript(`
local current = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not current or tonumber(current) < tonumber(ARGV[2]) then
  redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
end
return 1
`)
	groupReadRemoveMember = redis.NewScript(`
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`)
)

// ErrGroupDissolved reports that a group was dissolved and its read state is
// no longer served.
var ErrGroupDissolved = errors.New("group dissolved")

// ErrGroupNotMember reports that the user is not in the group's member set.
var ErrGroupNotMember = errors.New("group member not found")

// GroupReadAdvance reports the outcome of one member's read-position advance.
type GroupReadAdvance struct {
	UserID      uint64
	LastReadSeq uint64
	Advanced    bool
	StreamError bool
}

// GroupConversationState is one group's seq snapshot for a member.
type GroupConversationState struct {
	LastSeq uint64
	ReadSeq uint64
	Unread  uint64
}

// MessageRef identifies a group message resolved from its conversation seq.
type MessageRef struct {
	ID       uint64
	SenderID uint64
}

// GroupReadStats is the read progress of one group message, excluding the
// requesting member.
type GroupReadStats struct {
	ReadCount   int
	MemberCount int
	AllRead     bool
}

// GroupReadTrackerOptions configures the tracker.
type GroupReadTrackerOptions struct {
	Keyspace     Keyspace
	StreamMaxLen int64
	Gate         *SeqGate
}

// GroupReadTracker owns group read positions in Redis.
type GroupReadTracker struct {
	client       redis.UniversalClient
	keyspace     Keyspace
	streamMaxLen int64
	gate         *SeqGate
}

func NewGroupReadTracker(
	client redis.UniversalClient,
	options GroupReadTrackerOptions,
) *GroupReadTracker {
	if options.StreamMaxLen <= 0 {
		options.StreamMaxLen = defaultReadStreamMaxLen
	}
	return &GroupReadTracker{
		client:       client,
		keyspace:     options.Keyspace,
		streamMaxLen: options.StreamMaxLen,
		gate:         options.Gate,
	}
}

// AdvanceRead moves one member's read position forward to
// min(throughSeq, group counter). A zero throughSeq means the counter.
func (t *GroupReadTracker) AdvanceRead(
	ctx context.Context,
	groupID, userID, throughSeq uint64,
	now time.Time,
) (GroupReadAdvance, error) {
	if t == nil || t.client == nil {
		return GroupReadAdvance{}, ErrMessageServiceUnavailable
	}
	if err := t.gate.Ready(); err != nil {
		return GroupReadAdvance{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	args := []interface{}{
		strconv.FormatUint(userID, 10),
		strconv.FormatUint(throughSeq, 10),
		now.UTC().UnixMilli(),
		t.streamMaxLen,
		strconv.FormatUint(groupID, 10),
	}
	values, err := groupReadAdvanceScript.Run(
		ctx, t.client, t.readKeys(groupID), args...,
	).Slice()
	if err != nil {
		return GroupReadAdvance{}, err
	}
	if err := groupReadStatusError(values); err != nil {
		return GroupReadAdvance{}, err
	}
	if len(values) < 3 {
		return GroupReadAdvance{}, ErrMessageServiceUnavailable
	}
	position, err := parseRedisUint(values[1])
	if err != nil {
		return GroupReadAdvance{}, err
	}
	advanced, err := parseReadAdvanced(values[2])
	if err != nil {
		return GroupReadAdvance{}, err
	}
	streamError := false
	if len(values) >= 4 {
		streamError = fmt.Sprint(values[3]) == "stream_error"
	}
	return GroupReadAdvance{
		UserID:      userID,
		LastReadSeq: position,
		Advanced:    advanced,
		StreamError: streamError,
	}, nil
}

// AdvanceReadMany advances several members in one script call. Non-members and
// duplicate IDs are skipped; only processed members appear in the result.
func (t *GroupReadTracker) AdvanceReadMany(
	ctx context.Context,
	groupID uint64,
	userIDs []uint64,
	throughSeq uint64,
	now time.Time,
) ([]GroupReadAdvance, error) {
	if t == nil || t.client == nil {
		return nil, ErrMessageServiceUnavailable
	}
	if err := t.gate.Ready(); err != nil {
		return nil, err
	}
	advances := make([]GroupReadAdvance, 0, len(userIDs))
	if len(userIDs) == 0 {
		return advances, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	args := []interface{}{
		strconv.FormatUint(throughSeq, 10),
		now.UTC().UnixMilli(),
		t.streamMaxLen,
		strconv.FormatUint(groupID, 10),
	}
	for _, userID := range userIDs {
		if userID != 0 {
			args = append(args, strconv.FormatUint(userID, 10))
		}
	}
	values, err := groupReadAdvanceMany.Run(
		ctx, t.client, t.readKeys(groupID), args...,
	).Slice()
	if err != nil {
		return nil, err
	}
	if err := groupReadStatusError(values); err != nil {
		return nil, err
	}
	if (len(values)-1)%4 != 0 {
		return nil, ErrMessageServiceUnavailable
	}
	for index := 1; index < len(values); index += 4 {
		userID, err := parseRedisUint(values[index])
		if err != nil {
			return nil, err
		}
		position, err := parseRedisUint(values[index+1])
		if err != nil {
			return nil, err
		}
		advanced, err := parseReadAdvanced(values[index+2])
		if err != nil {
			return nil, err
		}
		advances = append(advances, GroupReadAdvance{
			UserID:      userID,
			LastReadSeq: position,
			Advanced:    advanced,
			StreamError: fmt.Sprint(values[index+3]) == "1",
		})
	}
	return advances, nil
}

func (t *GroupReadTracker) readKeys(groupID uint64) []string {
	return []string{
		t.keyspace.groupMembersKey(groupID),
		t.keyspace.groupDissolvedKey(groupID),
		t.keyspace.groupSyncRequiredKey(groupID),
		t.keyspace.groupSequenceKey(groupID),
		t.keyspace.groupReadKey(groupID),
		t.keyspace.groupReadStreamKey(),
	}
}

func groupReadStatusError(values []interface{}) error {
	if len(values) == 0 {
		return ErrMessageServiceUnavailable
	}
	status, ok := values[0].(string)
	if !ok {
		return ErrMessageServiceUnavailable
	}
	switch status {
	case "ok":
		return nil
	case "projection_missing":
		return ErrMessageServiceUnavailable
	case "dissolved":
		return ErrGroupDissolved
	case "not_member":
		return ErrGroupNotMember
	default:
		return fmt.Errorf("unexpected group read status %q", status)
	}
}

// States reads the group counter and the member watermark of several groups in
// one pipeline, so they come from a single snapshot.
func (t *GroupReadTracker) States(
	ctx context.Context,
	userID uint64,
	groupIDs []uint64,
) (map[uint64]GroupConversationState, error) {
	if t == nil || t.client == nil {
		return nil, ErrMessageServiceUnavailable
	}
	if err := t.gate.Ready(); err != nil {
		return nil, err
	}
	states := make(map[uint64]GroupConversationState, len(groupIDs))
	if len(groupIDs) == 0 {
		return states, nil
	}

	pipe := t.client.Pipeline()
	syncCommands := make([]*redis.IntCmd, len(groupIDs))
	setCommands := make([]*redis.IntCmd, len(groupIDs))
	memberCommands := make([]*redis.BoolCmd, len(groupIDs))
	lastCommands := make([]*redis.StringCmd, len(groupIDs))
	readCommands := make([]*redis.FloatCmd, len(groupIDs))
	field := strconv.FormatUint(userID, 10)
	for index, groupID := range groupIDs {
		syncCommands[index] = pipe.Exists(ctx, t.keyspace.groupSyncRequiredKey(groupID))
		setCommands[index] = pipe.Exists(ctx, t.keyspace.groupMembersKey(groupID))
		memberCommands[index] = pipe.SIsMember(ctx, t.keyspace.groupMembersKey(groupID), userID)
		lastCommands[index] = pipe.Get(ctx, t.keyspace.groupSequenceKey(groupID))
		readCommands[index] = pipe.ZScore(ctx, t.keyspace.groupReadKey(groupID), field)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	for index, groupID := range groupIDs {
		member, err := readGroupMembership(
			syncCommands[index], setCommands[index], memberCommands[index],
		)
		if err != nil {
			return nil, err
		}
		if !member {
			return nil, ErrMessageServiceUnavailable
		}
		lastSeq, err := readCounterResult(lastCommands[index])
		if err != nil {
			return nil, err
		}
		readSeq, err := optionalZScore(readCommands[index])
		if err != nil {
			return nil, err
		}
		states[groupID] = GroupConversationState{
			LastSeq: lastSeq,
			ReadSeq: readSeq,
			Unread:  unreadBetween(lastSeq, readSeq),
		}
	}
	return states, nil
}

// UnreadCounts returns max(0, group counter - member position) for every
// requested group. Stale membership state fails closed instead of returning a
// number that may be wrong.
func (t *GroupReadTracker) UnreadCounts(
	ctx context.Context,
	userID uint64,
	groupIDs []uint64,
) (map[uint64]int, error) {
	if t == nil || t.client == nil {
		return nil, ErrMessageServiceUnavailable
	}
	counts := make(map[uint64]int, len(groupIDs))
	if len(groupIDs) == 0 {
		return counts, nil
	}

	pipe := t.client.Pipeline()
	syncCommands := make([]*redis.IntCmd, len(groupIDs))
	setCommands := make([]*redis.IntCmd, len(groupIDs))
	memberCommands := make([]*redis.BoolCmd, len(groupIDs))
	positionCommands := make([]*redis.FloatCmd, len(groupIDs))
	lastCommands := make([]*redis.StringCmd, len(groupIDs))
	field := strconv.FormatUint(userID, 10)
	for index, groupID := range groupIDs {
		syncCommands[index] = pipe.Exists(ctx, t.keyspace.groupSyncRequiredKey(groupID))
		setCommands[index] = pipe.Exists(ctx, t.keyspace.groupMembersKey(groupID))
		memberCommands[index] = pipe.SIsMember(ctx, t.keyspace.groupMembersKey(groupID), userID)
		positionCommands[index] = pipe.ZScore(ctx, t.keyspace.groupReadKey(groupID), field)
		lastCommands[index] = pipe.Get(ctx, t.keyspace.groupSequenceKey(groupID))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	for index, groupID := range groupIDs {
		member, err := readGroupMembership(
			syncCommands[index], setCommands[index], memberCommands[index],
		)
		if err != nil {
			return nil, err
		}
		if !member {
			return nil, ErrMessageServiceUnavailable
		}
		position, err := optionalZScore(positionCommands[index])
		if err != nil {
			return nil, err
		}
		lastSeq, err := readCounterResult(lastCommands[index])
		if err != nil {
			return nil, err
		}
		counts[groupID] = int(unreadBetween(lastSeq, position))
	}
	return counts, nil
}

// readGroupMembership applies the fail-closed guard shared by States and
// UnreadCounts: a missing member set, a stale projection or a non-member must
// not produce a number.
func readGroupMembership(
	syncCommand *redis.IntCmd,
	setCommand *redis.IntCmd,
	memberCommand *redis.BoolCmd,
) (bool, error) {
	syncRequired, err := syncCommand.Result()
	if err != nil {
		return false, err
	}
	membersExist, err := setCommand.Result()
	if err != nil {
		return false, err
	}
	isMember, err := memberCommand.Result()
	if err != nil {
		return false, err
	}
	return syncRequired == 0 && membersExist > 0 && isMember, nil
}

func optionalZScore(command *redis.FloatCmd) (uint64, error) {
	score, err := command.Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if score <= 0 {
		return 0, nil
	}
	return uint64(score), nil
}

// ReadStats returns how many members have read the message with seq
// messageSeq, excluding userID.
func (t *GroupReadTracker) ReadStats(
	ctx context.Context,
	groupID, userID, messageSeq uint64,
) (GroupReadStats, error) {
	if t == nil || t.client == nil {
		return GroupReadStats{}, ErrMessageServiceUnavailable
	}
	keys := []string{
		t.keyspace.groupMembersKey(groupID),
		t.keyspace.groupDissolvedKey(groupID),
		t.keyspace.groupSyncRequiredKey(groupID),
		t.keyspace.groupReadKey(groupID),
	}
	args := []interface{}{
		strconv.FormatUint(userID, 10),
		strconv.FormatUint(messageSeq, 10),
	}
	values, err := groupReadStats.Run(ctx, t.client, keys, args...).Slice()
	if err != nil {
		return GroupReadStats{}, err
	}
	if err := groupReadStatusError(values); err != nil {
		return GroupReadStats{}, err
	}
	if len(values) < 4 {
		return GroupReadStats{}, ErrMessageServiceUnavailable
	}
	readCount, err := parseRedisUint(values[1])
	if err != nil {
		return GroupReadStats{}, err
	}
	memberCount, err := parseRedisUint(values[2])
	if err != nil {
		return GroupReadStats{}, err
	}
	return GroupReadStats{
		ReadCount:   int(readCount),
		MemberCount: int(memberCount),
		AllRead:     fmt.Sprint(values[3]) == "1",
	}, nil
}

// MessageBySeq resolves (group, seq) to the message entity. A missing index
// entry or entity returns found=false so the caller falls back to MySQL.
func (t *GroupReadTracker) MessageBySeq(
	ctx context.Context,
	groupID, seq uint64,
) (MessageRef, bool, error) {
	if t == nil || t.client == nil {
		return MessageRef{}, false, ErrMessageServiceUnavailable
	}
	if err := t.gate.Ready(); err != nil {
		return MessageRef{}, false, err
	}
	if seq == 0 {
		return MessageRef{}, false, nil
	}
	members, err := t.client.ZRangeByScore(
		ctx,
		t.keyspace.groupInboxKey(groupID),
		&redis.ZRangeBy{
			Min:    strconv.FormatUint(seq, 10),
			Max:    strconv.FormatUint(seq, 10),
			Offset: 0,
			Count:  1,
		},
	).Result()
	if err != nil {
		return MessageRef{}, false, err
	}
	if len(members) == 0 {
		return MessageRef{}, false, nil
	}
	messageID, err := strconv.ParseUint(members[0], 10, 64)
	if err != nil || messageID == 0 {
		return MessageRef{}, false, nil
	}
	values, err := t.client.HMGet(
		ctx,
		t.keyspace.groupMessageKey(messageID),
		"group_id",
		"sender_id",
	).Result()
	if err != nil {
		return MessageRef{}, false, err
	}
	if len(values) != 2 || values[0] == nil {
		return MessageRef{}, false, nil
	}
	entityGroupID, err := parseRedisUint(values[0])
	if err != nil {
		return MessageRef{}, false, err
	}
	if entityGroupID != groupID {
		return MessageRef{}, false, nil
	}
	ref := MessageRef{ID: messageID}
	if values[1] != nil {
		senderID, err := parseRedisUint(values[1])
		if err != nil {
			return MessageRef{}, false, err
		}
		ref.SenderID = senderID
	}
	return ref, true, nil
}

// SeedMember adds a member's read position (seq) only when it has none.
func (t *GroupReadTracker) SeedMember(
	ctx context.Context,
	groupID, userID, position uint64,
) error {
	if t == nil || t.client == nil {
		return ErrMessageServiceUnavailable
	}
	return groupReadSeedMember.Run(
		ctx,
		t.client,
		[]string{t.keyspace.groupReadKey(groupID)},
		strconv.FormatUint(userID, 10),
		strconv.FormatUint(position, 10),
	).Err()
}

// SetMemberBaseline raises a member's read position (seq) to the join
// baseline, never lowering an existing position.
func (t *GroupReadTracker) SetMemberBaseline(
	ctx context.Context,
	groupID, userID, position uint64,
) error {
	if t == nil || t.client == nil {
		return ErrMessageServiceUnavailable
	}
	return groupReadRaiseMember.Run(
		ctx,
		t.client,
		[]string{t.keyspace.groupReadKey(groupID)},
		strconv.FormatUint(userID, 10),
		strconv.FormatUint(position, 10),
	).Err()
}

// MemberIDs returns the group member set maintained in Redis.
func (t *GroupReadTracker) MemberIDs(
	ctx context.Context,
	groupID uint64,
) ([]uint64, error) {
	if t == nil || t.client == nil {
		return nil, ErrMessageServiceUnavailable
	}
	values, err := t.client.SMembers(
		ctx, t.keyspace.groupMembersKey(groupID),
	).Result()
	if err != nil {
		return nil, err
	}
	memberIDs := make([]uint64, 0, len(values))
	for _, value := range values {
		memberID, err := strconv.ParseUint(value, 10, 64)
		if err != nil || memberID == 0 {
			continue
		}
		memberIDs = append(memberIDs, memberID)
	}
	return memberIDs, nil
}

// RemoveMember drops a member's read position.
func (t *GroupReadTracker) RemoveMember(
	ctx context.Context,
	groupID, userID uint64,
) error {
	if t == nil || t.client == nil {
		return ErrMessageServiceUnavailable
	}
	return groupReadRemoveMember.Run(
		ctx,
		t.client,
		[]string{t.keyspace.groupReadKey(groupID)},
		strconv.FormatUint(userID, 10),
	).Err()
}

// ReconcileGroup makes the read set match the current members without lowering
// any existing position.
func (t *GroupReadTracker) ReconcileGroup(
	ctx context.Context,
	groupID uint64,
	positions map[uint64]uint64,
) error {
	if t == nil || t.client == nil {
		return ErrMessageServiceUnavailable
	}
	args := make([]interface{}, 0, len(positions)*2)
	for userID, position := range positions {
		if userID == 0 {
			continue
		}
		args = append(
			args,
			strconv.FormatUint(userID, 10),
			strconv.FormatUint(position, 10),
		)
	}
	return groupReadReconcile.Run(
		ctx,
		t.client,
		[]string{t.keyspace.groupReadKey(groupID)},
		args...,
	).Err()
}

// ReadModelReady reports whether the gate is open and seq:ready exists.
func (t *GroupReadTracker) ReadModelReady(ctx context.Context) (bool, error) {
	if t == nil || t.client == nil {
		return false, ErrMessageServiceUnavailable
	}
	if t.gate.Ready() != nil {
		return false, nil
	}
	exists, err := t.client.Exists(ctx, t.keyspace.sequenceReadyKey()).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

// MarkReadModelReady records that history and counters are complete.
func (t *GroupReadTracker) MarkReadModelReady(ctx context.Context) error {
	if t == nil || t.client == nil {
		return ErrMessageServiceUnavailable
	}
	if err := t.client.Set(ctx, t.keyspace.sequenceReadyKey(), "1", 0).Err(); err != nil {
		return fmt.Errorf("mark sequence ready: %w", err)
	}
	return nil
}
