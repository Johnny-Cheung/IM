-- Advance one user's read position for a peer.
--
-- KEYS[1] = dm:conv:{user}:{peer} (messages peer sent to user)
-- KEYS[2] = dm:read:{user} (HASH peer -> last read message ID)
-- KEYS[3] = dm:read:stream
-- ARGV[1] = peer user ID
-- ARGV[2] = through message ID (0 means newest accepted incoming message)
-- ARGV[3] = updated at (unix milliseconds)
-- ARGV[4] = stream max length
-- ARGV[5] = user ID, stored on the stream event
--
-- Returns {final position, candidate position, advanced, stream error?}.

local peer_id = ARGV[1]
local through_id = tonumber(ARGV[2])
local updated_at_ms = ARGV[3]
local stream_max_len = tonumber(ARGV[4])
local user_id = ARGV[5]

local candidate = 0
if through_id > 0 then
  local values = redis.call('ZREVRANGEBYSCORE', KEYS[1], through_id, '-inf', 'LIMIT', 0, 1)
  if #values > 0 then
    candidate = tonumber(values[1])
  end
else
  local values = redis.call('ZREVRANGE', KEYS[1], 0, 0)
  if #values > 0 then
    candidate = tonumber(values[1])
  end
end

local old_raw = redis.call('HGET', KEYS[2], peer_id)
local old = tonumber(old_raw or '0')

if candidate <= old then
  return {tostring(old), tostring(candidate), '0'}
end

redis.call('HSET', KEYS[2], peer_id, tostring(candidate))

-- A failed stream append must not fail an already-applied read. The caller
-- can log it and a later advance or reconciliation will catch MySQL up.
local appended = redis.pcall(
  'XADD', KEYS[3], 'MAXLEN', '~', stream_max_len, '*',
  'user_id', user_id,
  'peer_id', peer_id,
  'last_read_message_id', tostring(candidate),
  'updated_at_ms', updated_at_ms
)
if type(appended) == 'table' and appended.err then
  return {tostring(candidate), tostring(candidate), '1', 'stream_error'}
end

return {tostring(candidate), tostring(candidate), '1'}
