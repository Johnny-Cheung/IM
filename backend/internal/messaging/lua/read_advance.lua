-- Advance one user's read watermark for a peer.
--
-- KEYS[1] = dm:seq:{low}:{high} (conversation seq counter)
-- KEYS[2] = dm:read:{user} (HASH peer -> last read seq)
-- KEYS[3] = dm:read:stream
-- ARGV[1] = peer user ID
-- ARGV[2] = through seq (0 means the conversation counter)
-- ARGV[3] = updated at (unix milliseconds)
-- ARGV[4] = stream max length
-- ARGV[5] = user ID, stored on the stream event
--
-- Returns {final position, candidate position, advanced, stream error?}.

local peer_id = ARGV[1]
local through_seq = tonumber(ARGV[2])
local updated_at_ms = ARGV[3]
local stream_max_len = tonumber(ARGV[4])
local user_id = ARGV[5]

local last_seq = tonumber(redis.call('GET', KEYS[1]) or '0')
local old = tonumber(redis.call('HGET', KEYS[2], peer_id) or '0')
if last_seq <= 0 then
  return {tostring(old), '0', '0'}
end

local candidate = last_seq
if through_seq > 0 and through_seq < last_seq then
  candidate = through_seq
end

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
  'last_read_seq', tostring(candidate),
  'updated_at_ms', updated_at_ms
)
if type(appended) == 'table' and appended.err then
  return {tostring(candidate), tostring(candidate), '1', 'stream_error'}
end

return {tostring(candidate), tostring(candidate), '1'}
