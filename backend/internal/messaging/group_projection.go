package messaging

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var replaceGroupProjectionScript = redis.NewScript(`
redis.call('DEL', KEYS[1])
if ARGV[1] == '1' then
  redis.call('SET', KEYS[2], '1')
else
  redis.call('DEL', KEYS[2])
end
for index = 2, #ARGV do
  redis.call('SADD', KEYS[1], ARGV[index])
end
if ARGV[1] ~= '1' then
  for _, member in ipairs(redis.call('ZRANGE', KEYS[4], 0, -1)) do
    if redis.call('SISMEMBER', KEYS[1], member) == 0 then
      redis.call('ZREM', KEYS[4], member)
    end
  end
end
redis.call('DEL', KEYS[3])
return 1
`)

// GroupMembershipProjection is the MySQL-derived group membership state used
// by the Redis acceptance path.
type GroupMembershipProjection struct {
	GroupID   uint64
	MemberIDs []uint64
	Dissolved bool
}

// GroupProjection mirrors group membership and lifecycle state in Redis.
type GroupProjection struct {
	client   redis.UniversalClient
	keyspace Keyspace
}

func NewGroupProjection(client redis.UniversalClient, keyspace Keyspace) *GroupProjection {
	return &GroupProjection{client: client, keyspace: keyspace}
}

func (p *GroupProjection) Replace(
	ctx context.Context,
	groupID uint64,
	memberIDs []uint64,
	dissolved bool,
) error {
	if p == nil || p.client == nil {
		return ErrMessageServiceUnavailable
	}
	args := make([]interface{}, 1, len(memberIDs)+1)
	if dissolved {
		args[0] = "1"
	} else {
		args[0] = "0"
	}
	for _, memberID := range memberIDs {
		if memberID == 0 {
			continue
		}
		args = append(args, strconv.FormatUint(memberID, 10))
	}
	return replaceGroupProjectionScript.Run(ctx, p.client, []string{
		p.keyspace.groupMembersKey(groupID),
		p.keyspace.groupDissolvedKey(groupID),
		p.keyspace.groupSyncRequiredKey(groupID),
		p.keyspace.groupReadKey(groupID),
	}, args...).Err()
}

// BeginMutation blocks group acceptance while a membership mutation is being
// committed and its Redis projection is being refreshed.
func (p *GroupProjection) BeginMutation(ctx context.Context, groupID uint64) error {
	if p == nil || p.client == nil {
		return ErrMessageServiceUnavailable
	}
	return p.client.Set(ctx, p.keyspace.groupSyncRequiredKey(groupID), "1", 0).Err()
}

func (p *GroupProjection) IsMember(ctx context.Context, groupID, userID uint64) (bool, error) {
	if p == nil || p.client == nil {
		return false, ErrMessageServiceUnavailable
	}
	if exists, err := p.client.Exists(ctx, p.keyspace.groupSyncRequiredKey(groupID)).Result(); err != nil {
		return false, err
	} else if exists > 0 {
		return false, ErrMessageServiceUnavailable
	}
	membersExist, err := p.client.Exists(ctx, p.keyspace.groupMembersKey(groupID)).Result()
	if err != nil {
		return false, err
	}
	if membersExist == 0 {
		return false, ErrMessageServiceUnavailable
	}
	return p.client.SIsMember(ctx, p.keyspace.groupMembersKey(groupID), userID).Result()
}

// LastSeq returns the group's conversation seq counter, or zero when the
// group has no accepted message yet.
func (p *GroupProjection) LastSeq(ctx context.Context, groupID uint64) (uint64, error) {
	if p == nil || p.client == nil {
		return 0, ErrMessageServiceUnavailable
	}
	value, err := p.client.Get(ctx, p.keyspace.groupSequenceKey(groupID)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func (p *GroupProjection) Rebuild(
	ctx context.Context,
	groups []GroupMembershipProjection,
) error {
	if p == nil || p.client == nil {
		return ErrMessageServiceUnavailable
	}
	for _, group := range groups {
		if err := p.Replace(ctx, group.GroupID, group.MemberIDs, group.Dissolved); err != nil {
			return err
		}
	}
	return nil
}
