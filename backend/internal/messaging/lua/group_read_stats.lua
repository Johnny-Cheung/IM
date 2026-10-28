-- Snapshot one group's read stats for a message seq.
--
-- KEYS[1] = group:{G}:members
-- KEYS[2] = group:{G}:dissolved
-- KEYS[3] = group:{G}:sync_required
-- KEYS[4] = group:{G}:read
-- ARGV[1] = requesting user ID
-- ARGV[2] = message seq
--
-- Returns {status} on a failure, otherwise {ok, read count, member count,
-- all read}. The requesting member is excluded from both counts.

if redis.call('EXISTS', KEYS[3]) == 1 then
  return {'projection_missing'}
end
if redis.call('EXISTS', KEYS[2]) == 1 then
  return {'dissolved'}
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'projection_missing'}
end
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return {'not_member'}
end

local members = redis.call('SCARD', KEYS[1])
local message_seq = tonumber(ARGV[2])
local readers = redis.call('ZCOUNT', KEYS[4], ARGV[2], '+inf')
local mine = tonumber(redis.call('ZSCORE', KEYS[4], ARGV[1]) or '0')
if mine >= message_seq then
  readers = readers - 1
end

local member_count = members - 1
if member_count < 0 then
  member_count = 0
end
local all_read = '0'
if member_count > 0 and readers >= member_count then
  all_read = '1'
end

return {'ok', tostring(readers), tostring(member_count), all_read}
