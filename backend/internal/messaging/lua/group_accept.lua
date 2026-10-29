-- KEYS[1] = group:{G}:members
-- KEYS[2] = group:{G}:dissolved
-- KEYS[3] = group:{G}:sync_required
-- KEYS[4] = group-dedup:{G}:{sender}:{client_message_id}
-- KEYS[5] = messages:next_id (global message ID counter)
-- KEYS[6] = group:{G}:inbox
-- KEYS[7] = group:{G}:seq
-- KEYS[8] = group-messages:stream
-- KEYS[9] = seq:ready
-- ARGV[1] = sender ID
-- ARGV[2] = group ID
-- ARGV[3] = client message ID
-- ARGV[4] = body
-- ARGV[5] = created at (unix milliseconds)
-- ARGV[6] = body hash
-- ARGV[7] = global ID seed floor
-- ARGV[8] = max safe global message ID
-- ARGV[9] = max safe group seq
-- ARGV[10] = dedup TTL seconds
-- ARGV[11] = message entity key prefix
-- ARGV[12] = sender username
-- ARGV[13] = sender nickname

local sender_id = ARGV[1]
local group_id = ARGV[2]
local client_message_id = ARGV[3]
local body = ARGV[4]
local created_at_ms = ARGV[5]
local body_hash = ARGV[6]
local seed_floor = tonumber(ARGV[7])
local max_safe_id = tonumber(ARGV[8])
local max_safe_seq = tonumber(ARGV[9])
local dedup_ttl = tonumber(ARGV[10])
local message_prefix = ARGV[11]
local sender_username = ARGV[12]
local sender_nickname = ARGV[13]

if redis.call('EXISTS', KEYS[9]) == 0 then
  return {'seq_not_ready'}
end

if redis.call('EXISTS', KEYS[3]) == 1 then
  return {'projection_missing'}
end
if redis.call('EXISTS', KEYS[2]) == 1 then
  return {'dissolved'}
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'projection_missing'}
end
if redis.call('SISMEMBER', KEYS[1], sender_id) == 0 then
  return {'not_member'}
end

local existing_id = redis.call('HGET', KEYS[4], 'id')
if existing_id then
  local existing_hash = redis.call('HGET', KEYS[4], 'body_hash')
  if existing_hash == body_hash then
    local existing_seq = redis.call('HGET', message_prefix .. existing_id, 'seq')
    if not existing_seq then
      return {'seq_not_ready'}
    end
    return {'duplicate', existing_id, redis.call('HGET', KEYS[4], 'created_at_ms'), existing_seq}
  end
  return {'conflict'}
end

if seed_floor > max_safe_id then
  return {'id_overflow'}
end

local last_seq_raw = redis.call('GET', KEYS[7])
if not last_seq_raw and redis.call('ZCARD', KEYS[6]) > 0 then
  return {'seq_unavailable'}
end
local last_seq = tonumber(last_seq_raw or '0')
if last_seq >= max_safe_seq then
  return {'seq_overflow'}
end

local current = tonumber(redis.call('GET', KEYS[5]) or '0')
if current < seed_floor then
  current = seed_floor
  redis.call('SET', KEYS[5], current)
end

local message_id = redis.call('INCR', KEYS[5])
if message_id > max_safe_id then
  return {'id_overflow'}
end

-- A lost or stale global counter can reallocate an ID that already has an
-- entity. Never overwrite it; fail closed until calibration repairs the
-- counter. The ID is consumed (a gap is acceptable).
local message_key = message_prefix .. tostring(message_id)
if redis.call('EXISTS', message_key) == 1 then
  return {'id_unavailable'}
end

-- The surviving inbox is authoritative for live seqs. Fail before advancing
-- the counter so an accepted-but-unpersisted seq is never reused.
local seq = last_seq + 1
if redis.call('ZCOUNT', KEYS[6], seq, seq) > 0 then
  return {'seq_unavailable'}
end
redis.call('SET', KEYS[7], tostring(seq))

redis.call('HSET', message_key,
  'id', tostring(message_id),
  'group_id', group_id,
  'sender_id', sender_id,
  'client_message_id', client_message_id,
  'body', body,
  'created_at_ms', created_at_ms,
  'sender_username', sender_username,
  'sender_nickname', sender_nickname,
  'body_hash', body_hash,
  'seq', tostring(seq))

redis.call('HSET', KEYS[4],
  'id', tostring(message_id),
  'created_at_ms', created_at_ms,
  'body_hash', body_hash)
redis.call('EXPIRE', KEYS[4], dedup_ttl)

redis.call('ZADD', KEYS[6], seq, tostring(message_id))

-- The group message stream is never trimmed: an entry leaves it only after
-- the relay published it and acknowledged it.
redis.call('XADD', KEYS[8], '*',
  'id', tostring(message_id),
  'seq', tostring(seq),
  'group_id', group_id,
  'sender_id', sender_id,
  'client_message_id', client_message_id,
  'body', body,
  'created_at_ms', created_at_ms,
  'sender_username', sender_username,
  'sender_nickname', sender_nickname,
  'body_hash', body_hash)

return {'accepted', tostring(message_id), created_at_ms, tostring(seq)}
