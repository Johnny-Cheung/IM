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
	UserID            uint64
	LastReadMessageID uint64
	Advanced          bool
	StreamError       bool
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
}

// GroupReadTracker owns group read positions in Redis.
type GroupReadTracker struct {
	client       redis.UniversalClient
	keyspace     Keyspace
	streamMaxLen int64
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
	}
}

// AdvanceRead moves one member's read position forward to the newest accepted
// group message at or below throughID. A zero throughID means the newest.
func (t *GroupReadTracker) AdvanceRead(
	ctx context.Context,
	groupID, userID, throughID uint64,
	now time.Time,
) (GroupReadAdvance, error) {
	if t == nil || t.client == nil {
		return GroupReadAdvance{}, ErrMessageServiceUnavailable
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	args := []interface{}{
		strconv.FormatUint(userID, 10),
		strconv.FormatUint(throughID, 10),
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
		UserID:            userID,
		LastReadMessageID: position,
		Advanced:          advanced,
		StreamError:       streamError,
	}, nil
}

// AdvanceReadMany advances several members in one script call. Non-members and
// duplicate IDs are skipped; only processed members appear in the result.
func (t *GroupReadTracker) AdvanceReadMany(
	ctx context.Context,
	groupID uint64,
	userIDs []uint64,
	throughID uint64,
	now time.Time,
) ([]GroupReadAdvance, error) {
	if t == nil || t.client == nil {
		return nil, ErrMessageServiceUnavailable
	}
	advances := make([]GroupReadAdvance, 0, len(userIDs))
	if len(userIDs) == 0 {
		return advances, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	args := []interface{}{
		strconv.FormatUint(throughID, 10),
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
			UserID:            userID,
			LastReadMessageID: position,
			Advanced:          advanced,
			StreamError:       fmt.Sprint(values[index+3]) == "1",
		})
	}
	return advances, nil
}

func (t *GroupReadTracker) readKeys(groupID uint64) []string {
	return []string{
		t.keyspace.groupMembersKey(groupID),
		t.keyspace.groupDissolvedKey(groupID),
		t.keyspace.groupSyncRequiredKey(groupID),
		t.keyspace.groupInboxKey(groupID),
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

// UnreadCounts counts accepted group messages above each member's read
// position. Every requested group is present in the result. Stale membership
// state fails closed instead of returning a number that may be wrong.
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
	field := strconv.FormatUint(userID, 10)
	for index, groupID := range groupIDs {
		syncCommands[index] = pipe.Exists(ctx, t.keyspace.groupSyncRequiredKey(groupID))
		setCommands[index] = pipe.Exists(ctx, t.keyspace.groupMembersKey(groupID))
		memberCommands[index] = pipe.SIsMember(ctx, t.keyspace.groupMembersKey(groupID), userID)
		positionCommands[index] = pipe.ZScore(ctx, t.keyspace.groupReadKey(groupID), field)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	positions := make([]uint64, len(groupIDs))
	for index := range groupIDs {
		syncRequired, err := syncCommands[index].Result()
		if err != nil {
			return nil, err
		}
		membersExist, err := setCommands[index].Result()
		if err != nil {
			return nil, err
		}
		isMember, err := memberCommands[index].Result()
		if err != nil {
			return nil, err
		}
		if syncRequired > 0 || membersExist == 0 || !isMember {
			return nil, ErrMessageServiceUnavailable
		}
		score, err := positionCommands[index].Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if score > 0 {
			positions[index] = uint64(score)
		}
	}

	unreadPipe := t.client.Pipeline()
	unreadCommands := make([]*redis.IntCmd, len(groupIDs))
	for index, groupID := range groupIDs {
		unreadCommands[index] = unreadPipe.ZCount(
			ctx,
			t.keyspace.groupInboxKey(groupID),
			"("+strconv.FormatUint(positions[index], 10),
			"+inf",
		)
	}
	if _, err := unreadPipe.Exec(ctx); err != nil {
		return nil, err
	}
	for index, groupID := range groupIDs {
		count, err := unreadCommands[index].Result()
		if err != nil {
			return nil, err
		}
		counts[groupID] = int(count)
	}
	return counts, nil
}

// ReadStats returns how many members have read messageID, excluding userID.
func (t *GroupReadTracker) ReadStats(
	ctx context.Context,
	groupID, userID, messageID uint64,
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
		strconv.FormatUint(messageID, 10),
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

// MessageSender reports the sender of a group message entity. A missing entity
// returns found=false; an entity of another group returns found=true with
// senderID zero.
func (t *GroupReadTracker) MessageSender(
	ctx context.Context,
	groupID, messageID uint64,
) (uint64, bool, error) {
	if t == nil || t.client == nil {
		return 0, false, ErrMessageServiceUnavailable
	}
	if messageID == 0 {
		return 0, false, nil
	}
	values, err := t.client.HMGet(
		ctx,
		t.keyspace.groupMessageKey(messageID),
		"group_id",
		"sender_id",
	).Result()
	if err != nil {
		return 0, false, err
	}
	if len(values) != 2 || (values[0] == nil && values[1] == nil) {
		return 0, false, nil
	}
	if values[0] == nil {
		return 0, true, nil
	}
	entityGroupID, err := parseRedisUint(values[0])
	if err != nil {
		return 0, false, err
	}
	if entityGroupID != groupID {
		return 0, true, nil
	}
	if values[1] == nil {
		return 0, true, nil
	}
	senderID, err := parseRedisUint(values[1])
	if err != nil {
		return 0, false, err
	}
	return senderID, true, nil
}

// SeedMember adds a member's read position only when the member has none.
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

// ReadModelReady reports whether historical group read backfill has completed.
func (t *GroupReadTracker) ReadModelReady(ctx context.Context) (bool, error) {
	if t == nil || t.client == nil {
		return false, ErrMessageServiceUnavailable
	}
	value, err := t.client.Get(ctx, t.keyspace.groupReadReadyKey()).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return value == "1", nil
}

// MarkReadModelReady records that historical group read backfill has completed.
func (t *GroupReadTracker) MarkReadModelReady(ctx context.Context) error {
	if t == nil || t.client == nil {
		return ErrMessageServiceUnavailable
	}
	if err := t.client.Set(ctx, t.keyspace.groupReadReadyKey(), "1", 0).Err(); err != nil {
		return fmt.Errorf("mark group read model ready: %w", err)
	}
	return nil
}
