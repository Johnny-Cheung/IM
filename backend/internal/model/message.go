package model

import "time"

type PrivateMessage struct {
	ID          int64     `json:"msgId"`
	ClientMsgID string    `json:"clientMsgId,omitempty"`
	SenderID    int64     `json:"fromId"`
	ReceiverID  int64     `json:"toId"`
	Content     string    `json:"content"`
	MsgType     int       `json:"msgType"`
	CreatedAt   time.Time `json:"timestamp"`
}

type GroupMessage struct {
	ID          int64     `json:"msgId"`
	ClientMsgID string    `json:"clientMsgId,omitempty"`
	GroupID     int64     `json:"groupId"`
	SenderID    int64     `json:"fromId"`
	Content     string    `json:"content"`
	MsgType     int       `json:"msgType"`
	GroupSeq    int64     `json:"groupSeq"`
	CreatedAt   time.Time `json:"timestamp"`
}

type MsgRevoked struct {
	ID         int64     `json:"id"`
	MsgID      int64     `json:"msg_id"`
	ConvID     string    `json:"conv_id"`
	OperatorID int64     `json:"operator_id"`
	RevokedAt  time.Time `json:"revoked_at"`
}

// InboxMessage — 存储在 Redis inbox/outbox ZSet 中的消息
type InboxMessage struct {
	MsgID       int64  `json:"msgId"`
	ClientMsgID string `json:"clientMsgId,omitempty"`
	ConvID      string `json:"convId"`
	ConvType    int    `json:"convType"`
	FromID      int64  `json:"fromId"`
	ToID        int64  `json:"toId"`
	MsgType     int    `json:"msgType"`
	Content     string `json:"content"`
	ReadStatus  int    `json:"readStatus"`         // 0=未读, 1=已读 (仅私聊)
	GroupSeq    int64  `json:"groupSeq,omitempty"` // 群消息序号 (仅群聊)
	Timestamp   int64  `json:"timestamp"`
}

// MessagePersistEvent is the durable event written by the atomic Redis send
// script and relayed to RabbitMQ.  Redis owns the routing/state changes; the
// consumer only persists this immutable event to MySQL.
type MessagePersistEvent struct {
	EventID         string  `json:"eventId,omitempty"`
	ServerMsgID     int64   `json:"serverMsgId"`
	ClientMsgID     string  `json:"clientMsgId"`
	ConvType        int     `json:"convType"`
	SenderID        int64   `json:"senderId"`
	ReceiverID      int64   `json:"receiverId,omitempty"`
	GroupID         int64   `json:"groupId,omitempty"`
	Content         string  `json:"content"`
	MsgType         int     `json:"msgType"`
	ServerTimestamp int64   `json:"serverTimestamp"`
	GroupSeq        int64   `json:"groupSeq,omitempty"`
	Recipients      []int64 `json:"recipients,omitempty"`
}

// ServerAck — 消息到达服务器后返回给发送者的回执
type ServerAck struct {
	ClientMsgID string `json:"clientMsgId"`
	ServerMsgID int64  `json:"serverMsgId"`
	GroupSeq    int64  `json:"groupSeq,omitempty"`
	Timestamp   int64  `json:"timestamp"`
}

// DeliverAck — 接收者确认消息已送达
type DeliverAck struct {
	ServerMsgID int64 `json:"serverMsgId"`
}

// ReadAck — 用户将会话标记为已读
type ReadAck struct {
	ConvID string `json:"convId"`
	// LastReadGroupSeq 群聊已读水位线：客户端已展示的最后一条群消息序号。
	// 缺省(0)时服务端降级为"标记到当前最新"（等价旧的整会话清零）；私聊忽略此字段。
	LastReadGroupSeq int64 `json:"lastReadGroupSeq,omitempty"`
}

// SyncReq — 客户端请求离线消息同步
type SyncReq struct {
	LastSyncTime  int64 `json:"lastSyncTime"`
	LastSyncMsgID int64 `json:"lastSyncMsgId,omitempty"`
	BatchSize     int   `json:"batchSize"`
}

// SyncBatch — 服务端分批返回离线消息
type SyncBatch struct {
	Messages  []InboxMessage `json:"msgs"`
	HasMore   bool           `json:"hasMore"`
	SyncTime  int64          `json:"syncTime,omitempty"`
	SyncMsgID int64          `json:"syncMsgId,omitempty"`
}

// ConvSync — 同步时推送的会话列表及未读数
type ConvSync struct {
	Conversations []ConvSummary    `json:"conversations"`
	UnreadMap     map[string]int64 `json:"unreadMap"`
}

// SendMessage — 客户端通过 WebSocket 发送的聊天消息
type SendMessage struct {
	ClientMsgID string `json:"msgId"`    // 客户端生成的ID，用于去重
	ConvType    int    `json:"convType"` // 1=私聊, 2=群聊
	ToID        int64  `json:"toId"`     // 接收者ID(私聊) 或 群组ID(群聊)
	MsgType     int    `json:"msgType"`  // 1=文字, 2=图片, 3=视频, 等
	Content     string `json:"content"`
	Timestamp   int64  `json:"timestamp"`
}

// RevokeMsgReq — 客户端请求撤回一条消息
type RevokeMsgReq struct {
	ConvID      string `json:"convId"`
	ServerMsgID int64  `json:"serverMsgId"`
}

// RevokedNotification — 消息被撤回后推送给对方的通知
type RevokedNotification struct {
	ConvID      string `json:"convId"`
	ServerMsgID int64  `json:"serverMsgId"`
	OperatorID  int64  `json:"operatorId"`
}

// ConvSummary — conv_list ZSet 中的单条会话摘要
type ConvSummary struct {
	ConvID       string `json:"convId"`
	ConvType     int    `json:"convType"`
	TargetID     int64  `json:"targetId"`
	TargetName   string `json:"targetName"`
	TargetAvatar string `json:"targetAvatar"`
	LastMsg      string `json:"lastMsg"`
	LastMsgTime  int64  `json:"lastMsgTime"`
}
