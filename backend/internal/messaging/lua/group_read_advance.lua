-- Advance one member's read position in a group.
--
-- KEYS[1] = group:{G}:members
-- KEYS[2] = group:{G}:dissolved
-- KEYS[3] = group:{G}:sync_required
-- KEYS[4] = group:{G}:inbox
-- KEYS[5] = group:{G}:read
-- KEYS[6] = group-read:stream
-- ARGV[1] = user ID
-- ARGV[2] = through message ID (0 means newest accepted group message)
-- ARGV[3] = updated at (unix milliseconds)
-- ARGV[4] = stream max length
-- ARGV[5] = group ID, stored on the stream event
--
-- Returns {status, final position, advanced, stream error?}.

local user_id = ARGV[1]
local through_id = tonumber(ARGV[2])
local updated_at_ms = ARGV[3]
local stream_max_len = tonumber(ARGV[4])
local group_id = ARGV[5]

if redis.call('EXISTS', KEYS[3]) == 1 then
  return {'projection_missing'}
end
if redis.call('EXISTS', KEYS[2]) == 1 then
  return {'dissolved'}
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'projection_missing'}
end
if redis.call('SISMEMBER', KEYS[1], user_id) == 0 then
  return {'not_member'}
end

local candidate = 0
if through_id > 0 then
  local values = redis.call('ZREVRANGEBYSCORE', KEYS[4], tostring(through_id), '-inf', 'LIMIT', 0, 1)
  if #values > 0 then
    candidate = tonumber(values[1])
  end
else
  local values = redis.call('ZREVRANGE', KEYS[4], 0, 0)
  if #values > 0 then
    candidate = tonumber(values[1])
  end
end

local old = tonumber(redis.call('ZSCORE', KEYS[5], user_id) or '0')
if candidate <= old then
  return {'ok', tostring(old), '0'}
end

redis.call('ZADD', KEYS[5], tostring(candidate), user_id)

-- A failed stream append must not fail an already-applied read.
local appended = redis.pcall(
  'XADD', KEYS[6], 'MAXLEN', '~', stream_max_len, '*',
  'group_id', group_id,
  'user_id', user_id,
  'last_read_message_id', tostring(candidate),
  'updated_at_ms', updated_at_ms
)
if appended == nil or appended == false or (type(appended) == 'table' and appended.err) then
  return {'ok', tostring(candidate), '1', 'stream_error'}
end

return {'ok', tostring(candidate), '1'}
