local sender_id = ARGV[1]
local group_id = ARGV[2]
local client_message_id = ARGV[3]
local body = ARGV[4]
local created_at_ms = ARGV[5]
local body_hash = ARGV[6]
local seed_floor = tonumber(ARGV[7])
local max_safe_id = tonumber(ARGV[8])
local dedup_ttl = tonumber(ARGV[9])
local stream_max_len = tonumber(ARGV[10])
local message_prefix = ARGV[11]
local sender_username = ARGV[12]
local sender_nickname = ARGV[13]

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
    return {'duplicate', existing_id, redis.call('HGET', KEYS[4], 'created_at_ms')}
  end
  return {'conflict'}
end

if seed_floor > max_safe_id then
  return {'id_overflow'}
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

local message_key = message_prefix .. tostring(message_id)
redis.call('HSET', message_key,
  'id', tostring(message_id),
  'group_id', group_id,
  'sender_id', sender_id,
  'client_message_id', client_message_id,
  'body', body,
  'created_at_ms', created_at_ms,
  'sender_username', sender_username,
  'sender_nickname', sender_nickname,
  'body_hash', body_hash)

redis.call('HSET', KEYS[4],
  'id', tostring(message_id),
  'created_at_ms', created_at_ms,
  'body_hash', body_hash)
redis.call('EXPIRE', KEYS[4], dedup_ttl)

redis.call('ZADD', KEYS[6], message_id, tostring(message_id))
redis.call('SET', KEYS[7], tostring(message_id))

redis.call('XADD', KEYS[8], 'MAXLEN', '~', stream_max_len, '*',
  'id', tostring(message_id),
  'group_id', group_id,
  'sender_id', sender_id,
  'client_message_id', client_message_id,
  'body', body,
  'created_at_ms', created_at_ms,
  'sender_username', sender_username,
  'sender_nickname', sender_nickname,
  'body_hash', body_hash)

return {'accepted', tostring(message_id), created_at_ms}
