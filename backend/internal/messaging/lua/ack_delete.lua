-- KEYS[1] = message stream
-- ARGV[1] = consumer group
-- ARGV[2..] = stream entry IDs
--
-- XACK and XDEL must be one atomic step: an entry that is acked but left in the
-- never-trimmed stream is invisible to both the pending drain and ">" reads,
-- so nothing can ever recover or delete it. The relay passes a whole batch and
-- bounds it to 100 entries, well within Redis Lua's argument limits.
local count = #ARGV - 1
if count <= 0 then
  return 0
end
redis.call('XACK', KEYS[1], ARGV[1], unpack(ARGV, 2, #ARGV))
redis.call('XDEL', KEYS[1], unpack(ARGV, 2, #ARGV))
return count
