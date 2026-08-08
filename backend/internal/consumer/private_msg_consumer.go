package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/protocol"
	"github.com/goim/goim/internal/repository"
)

// ── 常量 ──

const (
	privateMsgQueue = "private_msg_persist"
	inboxMaxCount   = 2000 // 每个用户收件箱在裁剪后保留的最大条目数
)

// PrivateMsgConsumer 处理来自 private_msg_persist 队列的消息。
// 对于每条 PrivateMessage，它执行以下操作：
//  1. 写入接收者的收件箱（ZADD inbox:{receiverID}，readStatus=0）
//  2. 写入发送者的收件箱（ZADD inbox:{senderID}，readStatus=1 — 发送者"已读"自己的消息）
//  3. 更新双方的 conv_list
//  4. 增加接收者的未读计数
//  5. 通过 WebSocket 将 InboxMessage 推送给在线的接收者
//  6. 将 PrivateMessage 插入 MySQL
//  7. 裁剪双方的收件箱 ZSet
//  8. 向发送者发送 serverAck（此时消息已可撤回、可删除）
type PrivateMsgConsumer struct {
	ch        *amqp.Channel
	mysqlRepo repository.MySQLRepo
	redisRepo repository.RedisRepo
	cm        *conn.ConnectionManager
	logger    *zap.Logger
}

// NewPrivateMsgConsumer 创建一个新的 PrivateMsgConsumer。
func NewPrivateMsgConsumer(ch *amqp.Channel, mysqlRepo repository.MySQLRepo, redisRepo repository.RedisRepo, cm *conn.ConnectionManager, logger *zap.Logger) *PrivateMsgConsumer {
	return &PrivateMsgConsumer{
		ch:        ch,
		mysqlRepo: mysqlRepo,
		redisRepo: redisRepo,
		cm:        cm,
		logger:    logger,
	}
}

// Start 开始从 private_msg_persist 队列消费消息。
// 在 goroutine 中运行；阻塞直到 channel 关闭或 context 被取消。
// 使用手动确认：成功时 ack，失败时 nack+requeue。
func (c *PrivateMsgConsumer) Start(ctx context.Context) error {
	deliveries, err := c.ch.Consume(
		privateMsgQueue,
		"goim-private-msg-consumer", // 消费者标签
		false,                       // autoAck — 我们手动确认
		false,                       // exclusive
		false,                       // noLocal
		false,                       // noWait
		nil,                         // args
	)
	if err != nil {
		return fmt.Errorf("消费 private_msg_persist 失败: %w", err)
	}

	c.logger.Info("私聊消息消费者已启动")

	go func() {
		for d := range deliveries {
			c.handleDelivery(ctx, d)
		}
		c.logger.Info("私聊消息消费者投递通道已关闭")
	}()

	return nil
}

// handleDelivery 处理单条 AMQP 投递消息。
// 成功时：ack。失败时：nack 并 requeue，以便消息重试。
func (c *PrivateMsgConsumer) handleDelivery(ctx context.Context, d amqp.Delivery) {
	msg, err := deserializePrivateMsg(d.Body)
	if err != nil {
		c.logger.Error("反序列化私聊消息失败", zap.Error(err))
		// 格式错误的消息 — nack 不 requeue（丢弃），避免无限重试
		d.Nack(false, false)
		return
	}

	if err := c.process(ctx, msg); err != nil {
		c.logger.Error("处理私聊消息失败",
			zap.Int64("msgID", msg.ID),
			zap.Int64("senderID", msg.SenderID),
			zap.Int64("receiverID", msg.ReceiverID),
			zap.Error(err),
		)
		// 临时性故障 — nack 并 requeue 以重试
		d.Nack(false, true)
		return
	}

	d.Ack(false)
}

// process 执行私聊消息的完整扇出逻辑。
func (c *PrivateMsgConsumer) process(ctx context.Context, msg *model.PrivateMessage) error {
	convID := model.BuildConvID(model.ConvTypePrivate, msg.SenderID, msg.ReceiverID)
	timestamp := msg.CreatedAt.UnixMilli()

	// ── 1. 为接收者构建 InboxMessage（readStatus=0）──
	receiverInboxMsg := &model.InboxMessage{
		MsgID:       msg.ID,
		ClientMsgID: msg.ClientMsgID,
		ConvID:      convID,
		ConvType:    model.ConvTypePrivate,
		FromID:      msg.SenderID,
		ToID:        msg.ReceiverID,
		MsgType:     msg.MsgType,
		Content:     msg.Content,
		ReadStatus:  0, // 接收者未读
		Timestamp:   timestamp,
	}

	// ── 2. 为发送者构建 InboxMessage（readStatus=1）──
	senderInboxMsg := &model.InboxMessage{
		MsgID:       msg.ID,
		ClientMsgID: msg.ClientMsgID,
		ConvID:      convID,
		ConvType:    model.ConvTypePrivate,
		FromID:      msg.SenderID,
		ToID:        msg.ReceiverID,
		MsgType:     msg.MsgType,
		Content:     msg.Content,
		ReadStatus:  1, // 发送者"已读"自己的消息
		Timestamp:   timestamp,
	}

	// ── 3. 写入双方收件箱 ──
	if err := c.redisRepo.WriteInbox(ctx, msg.ReceiverID, receiverInboxMsg); err != nil {
		return fmt.Errorf("写入接收者收件箱失败: %w", err)
	}
	if err := c.redisRepo.WriteInbox(ctx, msg.SenderID, senderInboxMsg); err != nil {
		return fmt.Errorf("写入发送者收件箱失败: %w", err)
	}

	// ── 4. 更新双方的 conv_list ──
	senderSummary := buildPrivateConvSummaryForTarget(convID, msg, msg.ReceiverID)
	receiverSummary := buildPrivateConvSummaryForTarget(convID, msg, msg.SenderID)
	if c.mysqlRepo != nil {
		if user, _ := c.mysqlRepo.GetUserByID(ctx, msg.ReceiverID); user != nil {
			senderSummary.TargetName, senderSummary.TargetAvatar = user.Username, user.AvatarURL
		}
		if user, _ := c.mysqlRepo.GetUserByID(ctx, msg.SenderID); user != nil {
			receiverSummary.TargetName, receiverSummary.TargetAvatar = user.Username, user.AvatarURL
		}
	}
	senderJSON, err := json.Marshal(senderSummary)
	if err != nil {
		c.logger.Warn("序列化会话摘要失败", zap.Error(err))
		senderJSON = []byte(convID)
	}
	receiverJSON, err := json.Marshal(receiverSummary)
	if err != nil {
		receiverJSON = []byte(convID)
	}

	if err := c.redisRepo.UpdateConvList(ctx, msg.SenderID, convID, string(senderJSON), timestamp); err != nil {
		c.logger.Warn("更新发送者 conv_list 失败", zap.Error(err))
		// 非关键操作 — 不要因此使整个投递失败
	}
	if err := c.redisRepo.UpdateConvList(ctx, msg.ReceiverID, convID, string(receiverJSON), timestamp); err != nil {
		c.logger.Warn("更新接收者 conv_list 失败", zap.Error(err))
	}

	// ── 5. 增加接收者的未读计数 ──
	if err := c.redisRepo.IncrementUnread(ctx, msg.ReceiverID, convID); err != nil {
		c.logger.Warn("增加未读计数失败", zap.Error(err))
		// 非关键操作
	}

	// ── 6. 通过 WebSocket 推送给在线的接收者 ──
	pushToConnection(c.cm, c.logger, msg.ReceiverID, protocol.TypeMsg, receiverInboxMsg)

	// ── 7. 插入 MySQL ──
	if c.mysqlRepo != nil {
		if err := c.mysqlRepo.InsertPrivateMessage(ctx, msg); err != nil {
			return fmt.Errorf("插入私聊消息到 MySQL 失败: %w", err)
		}
	}

	// ── 8. 裁剪双方的收件箱 ZSet ──
	if err := c.redisRepo.TrimInbox(ctx, msg.ReceiverID, inboxMaxCount); err != nil {
		c.logger.Warn("裁剪接收者收件箱失败", zap.Error(err))
	}
	if err := c.redisRepo.TrimInbox(ctx, msg.SenderID, inboxMaxCount); err != nil {
		c.logger.Warn("裁剪发送者收件箱失败", zap.Error(err))
	}

	// ── 8. 消息处理成功后再向发送者确认 ──
	// ACK 不能早于 Redis 收件箱写入，否则客户端收到 ACK 后立即撤回/删除会查不到消息。
	if msg.ClientMsgID != "" {
		ack := &model.ServerAck{
			ClientMsgID: msg.ClientMsgID,
			ServerMsgID: msg.ID,
			Timestamp:   time.Now().UnixMilli(),
		}
		pushToConnection(c.cm, c.logger, msg.SenderID, protocol.TypeServerAck, ack)
	}

	c.logger.Debug("私聊消息处理完成",
		zap.Int64("msgID", msg.ID),
		zap.Int64("senderID", msg.SenderID),
		zap.Int64("receiverID", msg.ReceiverID),
	)

	return nil
}

// ── 辅助函数 ──

// deserializePrivateMsg 将 AMQP 消息体解析为 PrivateMessage。
func deserializePrivateMsg(body []byte) (*model.PrivateMessage, error) {
	var msg model.PrivateMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		return nil, fmt.Errorf("反序列化私聊消息失败: %w", err)
	}
	if msg.SenderID == 0 || msg.ReceiverID == 0 {
		return nil, fmt.Errorf("无效的私聊消息: senderID=%d, receiverID=%d", msg.SenderID, msg.ReceiverID)
	}
	return &msg, nil
}

// buildPrivateConvSummary 为私聊会话创建 ConvSummary。
// TargetName 和 TargetAvatar 留空 — 它们将在同步时
// 由消息服务或单独的用户查询来填充。
func buildPrivateConvSummary(convID string, msg *model.PrivateMessage) *model.ConvSummary {
	return buildPrivateConvSummaryForTarget(convID, msg, msg.ReceiverID)
}

func buildPrivateConvSummaryForTarget(convID string, msg *model.PrivateMessage, targetID int64) *model.ConvSummary {
	return &model.ConvSummary{
		ConvID:      convID,
		ConvType:    model.ConvTypePrivate,
		TargetID:    targetID,
		LastMsg:     truncateContent(msg.Content, 50),
		LastMsgTime: msg.CreatedAt.UnixMilli(),
	}
}

// truncateContent 将内容截断到 maxLen 个字符，用于会话摘要显示。
func truncateContent(content string, maxLen int) string {
	if len(content) <= maxLen {
		return content
	}
	if strings.Contains(content, "\n") {
		// 对于多行内容，显示第一行 + 指示符
		firstLine := strings.Split(content, "\n")[0]
		if len(firstLine) > maxLen {
			return firstLine[:maxLen] + "..."
		}
		return firstLine + "\n..."
	}
	return content[:maxLen] + "..."
}

// pushToConnection 通过 WebSocket 向在线用户发送消息。
// 如果用户离线，消息将被静默丢弃（用户将通过同步获取）。
// 此辅助函数由两个消费者共享。
func pushToConnection(cm *conn.ConnectionManager, logger *zap.Logger, userID int64, msgType string, data interface{}) {
	encoded, err := protocol.EncodeMsg(msgType, data)
	if err != nil {
		logger.Error("EncodeMsg 编码失败", zap.String("type", msgType), zap.Error(err))
		return
	}

	client, ok := cm.Get(userID)
	if !ok {
		// 用户离线 — 将通过同步获取消息
		return
	}

	select {
	case client.SendCh <- encoded:
		// 发送成功
	default:
		// 缓冲区满 — 丢弃消息（用户稍后将同步）
		logger.Warn("SendCh 缓冲区满，丢弃消息",
			zap.Int64("userID", userID),
			zap.String("type", msgType),
		)
	}
}

// nowUnixMilli 返回当前时间的 Unix 毫秒数。
func nowUnixMilli() int64 {
	return time.Now().UnixMilli()
}
