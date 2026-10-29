-- KEYS[1] = dm:conv-last:{user}
-- ARGV[1] = peer user ID (hash field)
-- ARGV[2] = global message ID
-- A message ID only grows with creation, so the preview never moves backwards.
local current = redis.call('HGET', KEYS[1], ARGV[1])
if not current or tonumber(current) < tonumber(ARGV[2]) then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
  return 1
end
return 0
