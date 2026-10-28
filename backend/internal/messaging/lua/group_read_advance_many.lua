-- Advance the read position of several group members in one script call.
--
-- KEYS[1] = group:{G}:members
-- KEYS[2] = group:{G}:dissolved
-- KEYS[3] = group:{G}:sync_required
-- KEYS[4] = group:{G}:inbox
-- KEYS[5] = group:{G}:read
-- KEYS[6] = group-read:stream
-- ARGV[1] = through message ID (0 means newest accepted group message)
-- ARGV[2] = updated at (unix milliseconds)
-- ARGV[3] = stream max length
-- ARGV[4] = group ID, stored on each stream event
-- ARGV[5..] = user IDs
--
-- Returns {status} on a group-level failure, otherwise
-- {'ok', user ID, final position, advanced, stream error, ...}. Non-members
-- and duplicate user IDs are skipped.

if redis.call('EXISTS', KEYS[3]) == 1 then
  return {'projection_missing'}
end
if redis.call('EXISTS', KEYS[2]) == 1 then
  return {'dissolved'}
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'projection_missing'}
end

local through_id = tonumber(ARGV[1])
local updated_at_ms = ARGV[2]
local stream_max_len = tonumber(ARGV[3])
local group_id = ARGV[4]

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

local result = {'ok'}
local seen = {}
for index = 5, #ARGV do
  local user_id = ARGV[index]
  if not seen[user_id] then
    seen[user_id] = true
    if redis.call('SISMEMBER', KEYS[1], user_id) == 1 then
      local old = tonumber(redis.call('ZSCORE', KEYS[5], user_id) or '0')
      if candidate <= old then
        result[#result + 1] = user_id
        result[#result + 1] = tostring(old)
        result[#result + 1] = '0'
        result[#result + 1] = '0'
      else
        redis.call('ZADD', KEYS[5], tostring(candidate), user_id)
        local appended = redis.pcall(
          'XADD', KEYS[6], 'MAXLEN', '~', stream_max_len, '*',
          'group_id', group_id,
          'user_id', user_id,
          'last_read_message_id', tostring(candidate),
          'updated_at_ms', updated_at_ms
        )
        local stream_error = '0'
        if appended == nil or appended == false or (type(appended) == 'table' and appended.err) then
          stream_error = '1'
        end
        result[#result + 1] = user_id
        result[#result + 1] = tostring(candidate)
        result[#result + 1] = '1'
        result[#result + 1] = stream_error
      end
    end
  end
end

return result
