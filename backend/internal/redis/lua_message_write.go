package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/goim/goim/internal/model"
	goredis "github.com/redis/go-redis/v9"
)

// MessageWriteResult is returned by the atomic send scripts.  A successful
// result means the Redis read model and the persistence Stream were updated in
// one Lua invocation; it does not mean MySQL has been written yet.
type MessageWriteResult struct {
	ErrCode    int
	MsgID      int64
	Timestamp  int64
	GroupSeq   int64
	EventID    string
	IsOnline   bool
	Recipients []int64
	Duplicate  bool
}

const (
	MsgWriteOK        = 0
	MsgWriteNotFriend = 1
	MsgWriteBlocked   = 2
	MsgWriteNotMember = 3
	MsgWriteMuted     = 4
	MsgWriteDuplicate = 5
)

// The scripts intentionally use explicit read-model keys (order ZSET + data
// HASH).  The legacy inbox/outbox ZSET is also populated during migration so
// older clients and rollback code can continue to sync messages.
const luaPrivateMsgWrite = `
local sender = ARGV[1]
local receiver = ARGV[2]
local clientID = ARGV[3]
local content = ARGV[4]
local msgType = tonumber(ARGV[5])
local convID = 'p_' .. math.min(tonumber(sender), tonumber(receiver)) .. '_' .. math.max(tonumber(sender), tonumber(receiver))

if redis.call('EXISTS', KEYS[1]) == 0 or redis.call('EXISTS', KEYS[2]) == 0 then return {1,0,0,0,0,'',0} end
if redis.call('SISMEMBER', KEYS[3], sender) == 1 then return {2,0,0,0,0,'',0} end

	local old = redis.call('GET', KEYS[4])
	if old then
	  local ok, data = pcall(cjson.decode, old)
	  if ok and data then return {5, data.id or '0', data.ts or '0', 0, redis.call('EXISTS','online:' .. receiver), '', 1} end
	  return {5, 0, 0, 0, redis.call('EXISTS','online:' .. receiver), '', 1}
	end

local t = redis.call('TIME')
local ts = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local seqKey = 'msg_id_seq:' .. ts
local seq = redis.call('INCR', seqKey)
if seq == 1 then redis.call('EXPIRE', seqKey, 2) end
if seq > 999 then return redis.error_reply('message ID sequence overflow') end
local msgID = ts * 1000 + seq
local exactMsgID = string.format('%.0f', msgID)
redis.call('SET', KEYS[4], cjson.encode({id=string.format('%.0f', msgID), ts=tostring(ts)}), 'EX', 300)

local function trimMessageBox(orderKey, dataKey, maxCount)
  local excess = redis.call('ZCARD', orderKey) - maxCount
  if excess > 0 then
    local ids = redis.call('ZRANGE', orderKey, 0, excess - 1)
    redis.call('ZREMRANGEBYRANK', orderKey, 0, excess - 1)
    if #ids > 0 then redis.call('HDEL', dataKey, unpack(ids)) end
  end
end

local msg = {msgId='__goim_msg_id__', clientMsgId=clientID, convId=convID, convType=1, fromId=tonumber(sender), toId=tonumber(receiver), msgType=msgType, content=content, readStatus=0, timestamp=ts}
local senderMsg = {msgId='__goim_msg_id__', clientMsgId=clientID, convId=convID, fromId=tonumber(sender), toId=tonumber(receiver), msgType=msgType, content=content, readStatus=1, timestamp=ts}
local msgJSON = string.gsub(cjson.encode(msg), '"__goim_msg_id__"', exactMsgID)
local senderJSON = string.gsub(cjson.encode(senderMsg), '"__goim_msg_id__"', exactMsgID)

redis.call('ZADD', 'inbox_order:' .. sender, ts, tostring(msgID))
redis.call('HSET', 'inbox_data:' .. sender, tostring(msgID), senderJSON)
redis.call('ZADD', 'inbox_order:' .. receiver, ts, tostring(msgID))
redis.call('HSET', 'inbox_data:' .. receiver, tostring(msgID), msgJSON)
redis.call('ZADD', 'inbox:' .. sender, ts, senderJSON)
redis.call('ZADD', 'inbox:' .. receiver, ts, msgJSON)
trimMessageBox('inbox_order:' .. sender, 'inbox_data:' .. sender, 2000)
trimMessageBox('inbox_order:' .. receiver, 'inbox_data:' .. receiver, 2000)
redis.call('ZREMRANGEBYRANK', 'inbox:' .. sender, 0, -2001)
redis.call('ZREMRANGEBYRANK', 'inbox:' .. receiver, 0, -2001)

local summary = cjson.encode({convId=convID, convType=1, targetId=tonumber(receiver), lastMsg=content, lastMsgTime=ts})
local receiverSummary = cjson.encode({convId=convID, convType=1, targetId=tonumber(sender), lastMsg=content, lastMsgTime=ts})
redis.call('ZADD', 'conv_order:' .. sender, ts, convID)
redis.call('HSET', 'conv_meta:' .. sender, convID, summary)
redis.call('ZADD', 'conv_order:' .. receiver, ts, convID)
redis.call('HSET', 'conv_meta:' .. receiver, convID, receiverSummary)
redis.call('HINCRBY', 'unread:' .. receiver, convID, 1)

local event = string.gsub(cjson.encode({serverMsgId='__goim_msg_id__', clientMsgId=clientID, convType=1, senderId=tonumber(sender), receiverId=tonumber(receiver), content=content, msgType=msgType, serverTimestamp=ts, recipients={tonumber(sender),tonumber(receiver)}}), '"__goim_msg_id__"', exactMsgID)
local eventID = redis.call('XADD', KEYS[5], '*', 'payload', event, 'serverMsgId', tostring(msgID), 'convType', '1')
return {0,msgID,ts,0,redis.call('EXISTS','online:' .. receiver),eventID,0}
`

const luaGroupMsgWrite = `
local groupID = ARGV[1]
local sender = ARGV[2]
local clientID = ARGV[3]
local content = ARGV[4]
local msgType = tonumber(ARGV[5])
local convID = 'g_' .. groupID

if redis.call('SISMEMBER', KEYS[1], sender) == 0 then return {3,0,0,0,'',0} end
local memberInfo = redis.call('HGET', KEYS[2], sender)
if memberInfo then
  local ok, info = pcall(cjson.decode, memberInfo)
  if ok and info and info.muted then return {4,0,0,0,'',0} end
end
	local old = redis.call('GET', KEYS[3])
	if old then
	  local ok, data = pcall(cjson.decode, old)
	  if ok and data then return {5, data.id or '0', data.gs or '0', data.ts or '0', '', 1} end
	  return {5, 0, 0, 0, '', 1}
	end

local t = redis.call('TIME')
local ts = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local seqKey = 'msg_id_seq:' .. ts
local seq = redis.call('INCR', seqKey)
if seq == 1 then redis.call('EXPIRE', seqKey, 2) end
if seq > 999 then return redis.error_reply('message ID sequence overflow') end
local msgID = ts * 1000 + seq
local exactMsgID = string.format('%.0f', msgID)
local groupSeq = redis.call('INCR', KEYS[4])
redis.call('SET', KEYS[3], cjson.encode({id=string.format('%.0f', msgID), ts=tostring(ts), gs=tostring(groupSeq)}), 'EX', 300)

local function trimMessageBox(orderKey, dataKey, maxCount)
  local excess = redis.call('ZCARD', orderKey) - maxCount
  if excess > 0 then
    local ids = redis.call('ZRANGE', orderKey, 0, excess - 1)
    redis.call('ZREMRANGEBYRANK', orderKey, 0, excess - 1)
    if #ids > 0 then redis.call('HDEL', dataKey, unpack(ids)) end
  end
end

local members = redis.call('SMEMBERS', KEYS[1])
local recipients = {}
local msg = {msgId='__goim_msg_id__', clientMsgId=clientID, convId=convID, convType=2, fromId=tonumber(sender), toId=tonumber(groupID), msgType=msgType, content=content, groupSeq=groupSeq, timestamp=ts}
local msgJSON = string.gsub(cjson.encode(msg), '"__goim_msg_id__"', exactMsgID)
redis.call('ZADD', 'outbox_order:' .. groupID, ts, tostring(msgID))
redis.call('HSET', 'outbox_data:' .. groupID, tostring(msgID), msgJSON)
redis.call('ZADD', 'outbox:' .. groupID, ts, msgJSON)
trimMessageBox('outbox_order:' .. groupID, 'outbox_data:' .. groupID, 2000)
redis.call('ZREMRANGEBYRANK', 'outbox:' .. groupID, 0, -2001)
local summary = cjson.encode({convId=convID, convType=2, targetId=tonumber(groupID), lastMsg=content, lastMsgTime=ts})
for _, uid in ipairs(members) do
  local id = tonumber(uid)
  table.insert(recipients, id)
  redis.call('ZADD', 'conv_order:' .. uid, ts, convID)
  redis.call('HSET', 'conv_meta:' .. uid, convID, summary)
  if id ~= tonumber(sender) then redis.call('HINCRBY', 'unread:' .. uid, convID, 1) end
end
local event = string.gsub(cjson.encode({serverMsgId='__goim_msg_id__', clientMsgId=clientID, convType=2, senderId=tonumber(sender), groupId=tonumber(groupID), content=content, msgType=msgType, serverTimestamp=ts, groupSeq=groupSeq, recipients=recipients}), '"__goim_msg_id__"', exactMsgID)
local eventID = redis.call('XADD', KEYS[5], '*', 'payload', event, 'serverMsgId', tostring(msgID), 'convType', '2')
return {0,msgID,groupSeq,ts,eventID,#recipients}
`

func parseIntResult(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	case []byte:
		i, _ := strconv.ParseInt(string(n), 10, 64)
		return i
	}
	return 0
}

func ExecPrivateMsgWrite(rdb *goredis.Client, ctx context.Context, senderID, receiverID int64, req *model.SendMessage) (*MessageWriteResult, error) {
	keys := []string{fmt.Sprintf("friend:%d:%d", senderID, receiverID), fmt.Sprintf("friend:%d:%d", receiverID, senderID), fmt.Sprintf("blacklist:%d", receiverID), fmt.Sprintf("msg_dedup:%d:%s", senderID, req.ClientMsgID), "message_persist_stream"}
	vals := []interface{}{senderID, receiverID, req.ClientMsgID, req.Content, req.MsgType}
	raw, err := rdb.Eval(ctx, luaPrivateMsgWrite, keys, vals...).Result()
	if err != nil {
		return nil, err
	}
	arr, ok := raw.([]interface{})
	if !ok || len(arr) < 7 {
		return nil, fmt.Errorf("invalid private message lua result")
	}
	r := &MessageWriteResult{ErrCode: int(parseIntResult(arr[0])), MsgID: parseIntResult(arr[1]), Timestamp: parseIntResult(arr[2]), IsOnline: parseIntResult(arr[4]) == 1, EventID: stringValue(arr[5]), Duplicate: parseIntResult(arr[6]) == 1}
	if r.MsgID > 0 {
		r.Recipients = []int64{senderID, receiverID}
	}
	return r, nil
}

func ExecGroupMsgWrite(rdb *goredis.Client, ctx context.Context, groupID, senderID int64, req *model.SendMessage) (*MessageWriteResult, error) {
	keys := []string{fmt.Sprintf("group_members:%d", groupID), fmt.Sprintf("group_member_info:%d", groupID), fmt.Sprintf("msg_dedup:%d:%s", senderID, req.ClientMsgID), fmt.Sprintf("group_seq:%d", groupID), "message_persist_stream"}
	vals := []interface{}{groupID, senderID, req.ClientMsgID, req.Content, req.MsgType}
	raw, err := rdb.Eval(ctx, luaGroupMsgWrite, keys, vals...).Result()
	if err != nil {
		return nil, err
	}
	arr, ok := raw.([]interface{})
	if !ok || len(arr) < 6 {
		return nil, fmt.Errorf("invalid group message lua result")
	}
	r := &MessageWriteResult{ErrCode: int(parseIntResult(arr[0])), MsgID: parseIntResult(arr[1]), GroupSeq: parseIntResult(arr[2]), Timestamp: parseIntResult(arr[3]), EventID: stringValue(arr[4]), Duplicate: parseIntResult(arr[5]) == 1}
	return r, nil
}

func stringValue(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	}
	return ""
}

// DecodePersistEvent validates and decodes the payload stored in a Stream or
// RabbitMQ message.
func DecodePersistEvent(raw []byte) (*model.MessagePersistEvent, error) {
	var e model.MessagePersistEvent
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, err
	}
	if e.ServerMsgID == 0 || e.SenderID == 0 {
		return nil, fmt.Errorf("invalid persist event")
	}
	return &e, nil
}
