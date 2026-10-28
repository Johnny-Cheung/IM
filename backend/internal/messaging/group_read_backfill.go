package messaging

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/redis/go-redis/v9"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

// GroupReadBackfillStore reads the durable data needed to rebuild the Redis
// group read model.
type GroupReadBackfillStore interface {
	ListGroupMessageIndex(
		ctx context.Context,
		afterMessageID uint64,
		limit int,
	) ([]store.GroupMessageIndex, error)
	ListGroupMemberReads(
		ctx context.Context,
		afterGroupID, afterUserID uint64,
		limit int,
	) ([]store.GroupMemberRead, error)
	ListGroupLastOwnMessages(ctx context.Context) ([]store.GroupLastOwnMessage, error)
	CountGroupMessagesByGroup(ctx context.Context) (map[uint64]uint64, error)
	ListGroupMembershipProjections(
		ctx context.Context,
	) ([]store.GroupMembershipProjection, error)
	UpsertGroupRead(
		ctx context.Context,
		groupID, userID, lastReadMessageID uint64,
	) (bool, error)
}

// GroupReadBackfillResult reports what one backfill run indexed and verified.
type GroupReadBackfillResult struct {
	MessagesIndexed          int
	GroupsRebuilt            int
	ReadPositionsIndexed     int
	ReadPositionsWrittenBack int
	GroupsVerified           int
	ReadPositionsVerified    int
	MemoryBefore             string
	MemoryAfter              string
}

type groupMemberKey struct {
	groupID uint64
	userID  uint64
}

// GroupReadBackfill rebuilds group inboxes, membership and read positions from
// MySQL. It is idempotent and must run while the server is stopped.
type GroupReadBackfill struct {
	client    redis.UniversalClient
	keyspace  Keyspace
	store     GroupReadBackfillStore
	batchSize int
}

func NewGroupReadBackfill(
	client redis.UniversalClient,
	keyspace Keyspace,
	dataStore GroupReadBackfillStore,
	batchSize int,
) *GroupReadBackfill {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &GroupReadBackfill{
		client:    client,
		keyspace:  keyspace,
		store:     dataStore,
		batchSize: batchSize,
	}
}

// Run indexes historical group messages and read positions, rebuilds group
// membership, verifies the result and finally marks the read model ready.
func (b *GroupReadBackfill) Run(ctx context.Context) (GroupReadBackfillResult, error) {
	if b == nil || b.client == nil || b.store == nil {
		return GroupReadBackfillResult{}, ErrMessageServiceUnavailable
	}
	result := GroupReadBackfillResult{
		MemoryBefore: redisUsedMemory(ctx, b.client),
	}

	if err := b.indexMessages(ctx, &result); err != nil {
		return result, err
	}

	storedGroups, err := b.store.ListGroupMembershipProjections(ctx)
	if err != nil {
		return result, fmt.Errorf("load group membership projections: %w", err)
	}
	projections := make([]GroupMembershipProjection, 0, len(storedGroups))
	for _, group := range storedGroups {
		projections = append(projections, GroupMembershipProjection{
			GroupID:   group.GroupID,
			MemberIDs: group.MemberIDs,
			Dissolved: group.Dissolved,
		})
	}
	projector := NewGroupProjection(b.client, b.keyspace)
	if err := projector.Rebuild(ctx, projections); err != nil {
		return result, fmt.Errorf("rebuild group projection: %w", err)
	}
	result.GroupsRebuilt = len(projections)

	targets, persisted, err := b.readTargets(ctx)
	if err != nil {
		return result, err
	}
	if err := b.reconcileReadPositions(ctx, storedGroups, targets, persisted, &result); err != nil {
		return result, err
	}

	if err := b.verifyInboxes(ctx, &result); err != nil {
		return result, err
	}
	if err := b.verifyReadSizes(ctx, storedGroups); err != nil {
		return result, err
	}
	if err := b.verifyReadPositions(ctx, targets, &result); err != nil {
		return result, err
	}

	tracker := NewGroupReadTracker(
		b.client,
		GroupReadTrackerOptions{Keyspace: b.keyspace},
	)
	if err := tracker.MarkReadModelReady(ctx); err != nil {
		return result, err
	}
	result.MemoryAfter = redisUsedMemory(ctx, b.client)
	return result, nil
}

func (b *GroupReadBackfill) indexMessages(
	ctx context.Context,
	result *GroupReadBackfillResult,
) error {
	afterMessageID := uint64(0)
	for {
		rows, err := b.store.ListGroupMessageIndex(ctx, afterMessageID, b.batchSize)
		if err != nil {
			return fmt.Errorf("list group messages: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}
		pipe := b.client.Pipeline()
		for _, row := range rows {
			if row.MessageID == 0 {
				return fmt.Errorf("group message index returned message ID 0")
			}
			afterMessageID = row.MessageID
			if row.GroupID == 0 {
				continue
			}
			pipe.ZAdd(
				ctx,
				b.keyspace.groupInboxKey(row.GroupID),
				redis.Z{
					Score:  float64(row.MessageID),
					Member: strconv.FormatUint(row.MessageID, 10),
				},
			)
			result.MessagesIndexed++
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("index group messages: %w", err)
		}
	}
}

// readTargets returns the position each member must have: the greater of the
// persisted read position and that member's newest own message in the group.
func (b *GroupReadBackfill) readTargets(
	ctx context.Context,
) (map[groupMemberKey]uint64, map[groupMemberKey]uint64, error) {
	persisted := make(map[groupMemberKey]uint64)
	afterGroupID, afterUserID := uint64(0), uint64(0)
	for {
		rows, err := b.store.ListGroupMemberReads(
			ctx, afterGroupID, afterUserID, b.batchSize,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("list group member reads: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			afterGroupID, afterUserID = row.GroupID, row.UserID
			if row.GroupID == 0 || row.UserID == 0 {
				continue
			}
			persisted[groupMemberKey{row.GroupID, row.UserID}] = row.LastReadMessageID
		}
	}

	own, err := b.store.ListGroupLastOwnMessages(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("list group last own messages: %w", err)
	}
	ownPositions := make(map[groupMemberKey]uint64, len(own))
	for _, row := range own {
		if row.GroupID == 0 || row.SenderID == 0 {
			continue
		}
		key := groupMemberKey{row.GroupID, row.SenderID}
		if row.MessageID > ownPositions[key] {
			ownPositions[key] = row.MessageID
		}
	}

	targets := make(map[groupMemberKey]uint64, len(persisted))
	for key, position := range persisted {
		if last := ownPositions[key]; last > position {
			position = last
		}
		targets[key] = position
	}
	return targets, persisted, nil
}

func (b *GroupReadBackfill) reconcileReadPositions(
	ctx context.Context,
	groups []store.GroupMembershipProjection,
	targets map[groupMemberKey]uint64,
	persisted map[groupMemberKey]uint64,
	result *GroupReadBackfillResult,
) error {
	positionsByGroup := make(map[uint64]map[uint64]uint64, len(groups))
	for _, group := range groups {
		positionsByGroup[group.GroupID] = make(map[uint64]uint64)
	}
	for key, position := range targets {
		positions, ok := positionsByGroup[key.groupID]
		if !ok {
			positions = make(map[uint64]uint64)
			positionsByGroup[key.groupID] = positions
		}
		positions[key.userID] = position
	}

	tracker := NewGroupReadTracker(
		b.client,
		GroupReadTrackerOptions{Keyspace: b.keyspace},
	)
	for groupID, positions := range positionsByGroup {
		if err := tracker.ReconcileGroup(ctx, groupID, positions); err != nil {
			return fmt.Errorf("reconcile group %d reads: %w", groupID, err)
		}
		for userID, position := range positions {
			if err := tracker.SetMemberBaseline(ctx, groupID, userID, position); err != nil {
				return fmt.Errorf("raise group %d member %d read: %w", groupID, userID, err)
			}
		}
		result.ReadPositionsIndexed += len(positions)
	}

	for key, position := range targets {
		if position <= persisted[key] {
			continue
		}
		if _, err := b.store.UpsertGroupRead(
			ctx, key.groupID, key.userID, position,
		); err != nil {
			return fmt.Errorf(
				"write back group %d member %d read: %w",
				key.groupID, key.userID, err,
			)
		}
		result.ReadPositionsWrittenBack++
	}
	return nil
}

func (b *GroupReadBackfill) verifyInboxes(
	ctx context.Context,
	result *GroupReadBackfillResult,
) error {
	counts, err := b.store.CountGroupMessagesByGroup(ctx)
	if err != nil {
		return fmt.Errorf("count persisted group messages: %w", err)
	}
	if len(counts) == 0 {
		return nil
	}
	groupIDs := make([]uint64, 0, len(counts))
	for groupID := range counts {
		groupIDs = append(groupIDs, groupID)
	}
	sort.Slice(groupIDs, func(i, j int) bool { return groupIDs[i] < groupIDs[j] })

	pipe := b.client.Pipeline()
	commands := make([]*redis.IntCmd, len(groupIDs))
	for index, groupID := range groupIDs {
		commands[index] = pipe.ZCard(ctx, b.keyspace.groupInboxKey(groupID))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("count indexed group messages: %w", err)
	}
	for index, groupID := range groupIDs {
		indexed, err := commands[index].Result()
		if err != nil {
			return fmt.Errorf("count indexed group messages: %w", err)
		}
		if uint64(indexed) < counts[groupID] {
			return fmt.Errorf(
				"group %d inbox missing messages: redis=%d mysql=%d",
				groupID, indexed, counts[groupID],
			)
		}
		result.GroupsVerified++
	}
	return nil
}

func (b *GroupReadBackfill) verifyReadSizes(
	ctx context.Context,
	groups []store.GroupMembershipProjection,
) error {
	if len(groups) == 0 {
		return nil
	}
	pipe := b.client.Pipeline()
	commands := make([]*redis.IntCmd, len(groups))
	for index, group := range groups {
		commands[index] = pipe.ZCard(ctx, b.keyspace.groupReadKey(group.GroupID))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("count group read positions: %w", err)
	}
	for index, group := range groups {
		size, err := commands[index].Result()
		if err != nil {
			return fmt.Errorf("count group read positions: %w", err)
		}
		if int(size) != len(group.MemberIDs) {
			return fmt.Errorf(
				"group %d read positions do not match members: redis=%d members=%d",
				group.GroupID, size, len(group.MemberIDs),
			)
		}
	}
	return nil
}

func (b *GroupReadBackfill) verifyReadPositions(
	ctx context.Context,
	targets map[groupMemberKey]uint64,
	result *GroupReadBackfillResult,
) error {
	keys := make([]groupMemberKey, 0, len(targets))
	for key := range targets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].groupID != keys[j].groupID {
			return keys[i].groupID < keys[j].groupID
		}
		return keys[i].userID < keys[j].userID
	})

	for start := 0; start < len(keys); start += b.batchSize {
		end := start + b.batchSize
		if end > len(keys) {
			end = len(keys)
		}
		chunk := keys[start:end]

		pipe := b.client.Pipeline()
		commands := make([]*redis.FloatCmd, len(chunk))
		for index, key := range chunk {
			commands[index] = pipe.ZScore(
				ctx,
				b.keyspace.groupReadKey(key.groupID),
				strconv.FormatUint(key.userID, 10),
			)
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("read indexed group positions: %w", err)
		}
		for index, key := range chunk {
			position, err := commands[index].Result()
			if errors.Is(err, redis.Nil) {
				return fmt.Errorf(
					"group %d member %d read position missing",
					key.groupID, key.userID,
				)
			}
			if err != nil {
				return fmt.Errorf("read indexed group position: %w", err)
			}
			if uint64(position) < targets[key] {
				return fmt.Errorf(
					"group %d member %d read position behind: redis=%d mysql=%d",
					key.groupID, key.userID, uint64(position), targets[key],
				)
			}
			result.ReadPositionsVerified++
		}
	}
	return nil
}
