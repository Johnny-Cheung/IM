-- KEYS[1] = messages:next_id (global message ID counter)
-- KEYS[2] = friends:{sender}
-- KEYS[3] = dedup:{sender}:{recipient}:{client_message_id}
-- KEYS[4] = dm:feed:{sender}
-- KEYS[5] = dm:feed:{recipient}
-- KEYS[6] = dm:pair:{low}:{high}
-- KEYS[7] = dm:seq:{low}:{high}
-- KEYS[8] = dm:conv-last:{sender}
-- KEYS[9] = dm:conv-last:{recipient}
-- KEYS[10] = messages:stream
-- KEYS[11] = seq:ready
-- ARGV[1] = sender ID
-- ARGV[2] = recipient ID
-- ARGV[3] = client message ID
-- ARGV[4] = body
-- ARGV[5] = created at (unix milliseconds)
-- ARGV[6] = body hash
-- ARGV[7] = global ID seed floor
-- ARGV[8] = max safe global message ID
-- ARGV[9] = max safe conversation seq
-- ARGV[10] = dedup TTL seconds
-- ARGV[11] = message entity key prefix

local sender_id = ARGV[1]
local recipient_id = ARGV[2]
local client_message_id = ARGV[3]
local body = ARGV[4]
local created_at_ms = ARGV[5]
local body_hash = ARGV[6]
local seed_floor = tonumber(ARGV[7])
local max_safe_id = tonumber(ARGV[8])
local max_safe_seq = tonumber(ARGV[9])
local dedup_ttl = tonumber(ARGV[10])
local message_prefix = ARGV[11]

if redis.call('EXISTS', KEYS[11]) == 0 then
  return {'seq_not_ready'}
end

if sender_id == recipient_id then
  return {'cannot_message_self'}
end

local existing_id = redis.call('HGET', KEYS[3], 'id')
if existing_id then
  local existing_hash = redis.call('HGET', KEYS[3], 'body_hash')
  if existing_hash == body_hash then
    local existing_seq = redis.call('HGET', message_prefix .. existing_id, 'seq')
    if not existing_seq then
      return {'seq_not_ready'}
    end
    return {'duplicate', existing_id, redis.call('HGET', KEYS[3], 'created_at_ms'), existing_seq}
  end
  return {'conflict'}
end

if redis.call('SISMEMBER', KEYS[2], recipient_id) == 0 then
  return {'not_friends'}
end

if seed_floor > max_safe_id then
  return {'id_overflow'}
end

-- A missing counter with a non-empty index means the counter was lost;
-- restarting at 1 would collide with the existing conversation.
local last_seq_raw = redis.call('GET', KEYS[7])
if not last_seq_raw and redis.call('ZCARD', KEYS[6]) > 0 then
  return {'seq_unavailable'}
end
local last_seq = tonumber(last_seq_raw or '0')
if last_seq >= max_safe_seq then
  return {'seq_overflow'}
end

local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current < seed_floor then
  current = seed_floor
  redis.call('SET', KEYS[1], current)
end

local message_id = redis.call('INCR', KEYS[1])
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

-- The surviving index is authoritative for live seqs. Fail before advancing
-- the counter so an accepted-but-unpersisted seq is never reused.
local seq = last_seq + 1
if redis.call('ZCOUNT', KEYS[6], seq, seq) > 0 then
  return {'seq_unavailable'}
end
redis.call('SET', KEYS[7], tostring(seq))

redis.call('HSET', message_key,
  'id', tostring(message_id),
  'sender_id', sender_id,
  'recipient_id', recipient_id,
  'client_message_id', client_message_id,
  'body', body,
  'created_at_ms', created_at_ms,
  'body_hash', body_hash,
  'seq', tostring(seq))

redis.call('HSET', KEYS[3],
  'id', tostring(message_id),
  'created_at_ms', created_at_ms,
  'body_hash', body_hash)
redis.call('EXPIRE', KEYS[3], dedup_ttl)

redis.call('ZADD', KEYS[4], message_id, tostring(message_id))
redis.call('ZADD', KEYS[5], message_id, tostring(message_id))
redis.call('ZADD', KEYS[6], seq, tostring(message_id))
redis.call('HSET', KEYS[8], recipient_id, tostring(message_id))
redis.call('HSET', KEYS[9], sender_id, tostring(message_id))

-- The message stream is never trimmed: an entry leaves it only after the
-- relay published it and acknowledged it.
redis.call('XADD', KEYS[10], '*',
  'id', tostring(message_id),
  'seq', tostring(seq),
  'sender_id', sender_id,
  'recipient_id', recipient_id,
  'client_message_id', client_message_id,
  'body', body,
  'created_at_ms', created_at_ms,
  'body_hash', body_hash)

return {'accepted', tostring(message_id), created_at_ms, tostring(seq)}
