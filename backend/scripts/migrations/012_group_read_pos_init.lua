-- ============================================================================
-- 012_group_read_pos_init.lua — 群未读水位线一次性迁移
--
-- 背景：群聊未读从"发送时逐成员计数"（unread:{uid} hash）改为"水位线模型"
--      （未读 = 群最新 groupSeq − 成员已读游标 group_read_pos:{uid}）。
--
-- 本脚本为所有群成员初始化已读游标：
--     read_pos_init = 当前群 seq − 该成员 hash 中的群未读数（clamp ≥ 0）
-- 这样迁移前后每个成员的未读徽标数字完全不变。
--
-- 执行顺序（必须）：
--   1. 在旧代码（仍在写 unread hash）下执行本脚本；
--   2. 再发布新代码（新 Lua 不再写群 unread，读取端按游标差计算）。
-- 顺序反了会丢失迁移窗口内的群未读。
--
-- 运行方式（需要 redis-cli）：
--   redis-cli -p 16379 --eval 012_group_read_pos_init.lua
-- 幂等：重复执行结果不变（read_pos 按当前 hash 重新推导，可安全重跑）。
-- ============================================================================

local cursor = '0'
local total = 0

repeat
  local result = redis.call('SCAN', cursor, 'MATCH', 'group_members:*', 'COUNT', 100)
  cursor = result[1]
  for _, key in ipairs(result[2]) do
    -- key 形如 "group_members:{gid}"，"group_members:" 长度为 14
    local gid = string.sub(key, 15)
    local convID = 'g_' .. gid
    local seq = tonumber(redis.call('GET', 'group_seq:' .. gid) or '0')
    local members = redis.call('SMEMBERS', key)
    for _, uid in ipairs(members) do
      local unread = tonumber(redis.call('HGET', 'unread:' .. uid, convID) or '0')
      local pos = math.max(0, seq - unread)
      redis.call('HSET', 'group_read_pos:' .. uid, convID, tostring(pos))
      total = total + 1
    end
  end
until cursor == '0'

-- 注：发布新代码后，unread hash 中残留的群会话字段会被读取端忽略；
-- 如需清理可再执行：
--   SCAN 后对每个 unread:{uid} 执行 HDEL 掉所有 g_ 前缀字段。
return total
