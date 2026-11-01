-- KEYS[1] = message stream
-- ARGV[1] = consumer group
-- ARGV[2..] = stream entry IDs
--
-- XACK and XDEL must be one atomic step: an entry that is acked but left in the
-- never-trimmed stream is invisible to both the pending drain and ">" reads,
-- so nothing can ever recover or delete it. The relay passes a whole batch;
-- the loop keeps the script compatible with Redis Lua's argument limits.
local count = 0
for i = 2, #ARGV do
  redis.call('XACK', KEYS[1], ARGV[1], ARGV[i])
  redis.call('XDEL', KEYS[1], ARGV[i])
  count = count + 1
end
return count
