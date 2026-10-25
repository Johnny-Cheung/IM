local sender_id = ARGV[1]
local recipient_id = ARGV[2]
local client_message_id = ARGV[3]
local body = ARGV[4]
local created_at_ms = ARGV[5]
local body_hash = ARGV[6]
local seed_floor = tonumber(ARGV[7])
local max_safe_id = tonumber(ARGV[8])
local entity_ttl = tonumber(ARGV[9])
local dedup_ttl = tonumber(ARGV[10])
local inbox_keep = tonumber(ARGV[11])
local outbox_keep = tonumber(ARGV[12])
local stream_max_len = tonumber(ARGV[13])
local message_prefix = ARGV[14]

if sender_id == recipient_id then
  return {'cannot_message_self'}
end

local existing_id = redis.call('HGET', KEYS[3], 'id')
if existing_id then
  local existing_hash = redis.call('HGET', KEYS[3], 'body_hash')
  if existing_hash == body_hash then
    return {'duplicate', existing_id, redis.call('HGET', KEYS[3], 'created_at_ms')}
  end
  return {'conflict'}
end

if redis.call('SISMEMBER', KEYS[2], recipient_id) == 0 then
  return {'not_friends'}
end

if seed_floor > max_safe_id then
  return {'id_overflow'}
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

local message_key = message_prefix .. tostring(message_id)
redis.call('HSET', message_key,
  'id', tostring(message_id),
  'sender_id', sender_id,
  'recipient_id', recipient_id,
  'client_message_id', client_message_id,
  'body', body,
  'created_at_ms', created_at_ms,
  'body_hash', body_hash)
redis.call('EXPIRE', message_key, entity_ttl)

redis.call('HSET', KEYS[3],
  'id', tostring(message_id),
  'created_at_ms', created_at_ms,
  'body_hash', body_hash)
redis.call('EXPIRE', KEYS[3], dedup_ttl)

redis.call('ZADD', KEYS[4], message_id, tostring(message_id))
redis.call('ZREMRANGEBYRANK', KEYS[4], 0, -outbox_keep - 1)
redis.call('ZADD', KEYS[5], message_id, tostring(message_id))
redis.call('ZREMRANGEBYRANK', KEYS[5], 0, -inbox_keep - 1)

redis.call('XADD', KEYS[6], 'MAXLEN', '~', stream_max_len, '*',
  'id', tostring(message_id),
  'sender_id', sender_id,
  'recipient_id', recipient_id,
  'client_message_id', client_message_id,
  'body', body,
  'created_at_ms', created_at_ms,
  'body_hash', body_hash)

return {'accepted', tostring(message_id), created_at_ms}
