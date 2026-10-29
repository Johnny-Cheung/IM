-- KEYS[1] = message stream
-- ARGV[1] = consumer group
-- ARGV[2] = stream entry ID
--
-- XACK and XDEL must be one atomic step: an entry that is acked but left in the
-- never-trimmed stream is invisible to both the pending drain and ">" reads,
-- so nothing can ever recover or delete it.
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
return redis.call('XDEL', KEYS[1], ARGV[2])
