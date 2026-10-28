-- Make group:{G}:read match the given member set without lowering positions.
--
-- KEYS[1] = group:{G}:read
-- ARGV[1,2] = user ID, position, repeated for every current member
--
-- Missing members are added with the given position. Entries for users that
-- are no longer members are removed. Existing scores are never overwritten or
-- lowered.

local wanted = {}
for index = 1, #ARGV, 2 do
  local user_id = ARGV[index]
  wanted[user_id] = true
  if redis.call('ZSCORE', KEYS[1], user_id) == false then
    redis.call('ZADD', KEYS[1], ARGV[index + 1], user_id)
  end
end

local existing = redis.call('ZRANGE', KEYS[1], 0, -1)
for index = 1, #existing do
  if not wanted[existing[index]] then
    redis.call('ZREM', KEYS[1], existing[index])
  end
end

return 1
