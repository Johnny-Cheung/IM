package consumer

import (
	"context"
	"encoding/json"
	"fmt"
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
	groupMsgQueue  = "group_msg_fanout"
	outboxMaxCount = 2000 // 每个群组发件箱裁剪后保留的最大条目数
)

// GroupMsgConsumer 处理来自 group_msg_fanout 队列的消息。
// 对于每条 GroupMessage，它执行以下操作：
//  1. 写入群组发件箱（ZADD outbox:{groupID}，convType=2 的 InboxMessage）
//  2. 获取群组成员列表（从 Redis SET group_members:{groupID}）
//  3. 对于每个成员：
//     a. 更新 conv_list（ZADD conv_list:{userID}）
//     b. 增加未读计数（跳过发送者）
//  4. 通过 WebSocket 将 InboxMessage 推送给在线成员（跳过发送者）
//  5. 将 GroupMessage 插入 MySQL
//  6. 裁剪发件箱 ZSet
//  7. 向发送者发送 serverAck（此时消息已可撤回、可删除）
type GroupMsgConsumer struct {
	ch        *amqp.Channel
	mysqlRepo repository.MySQLRepo
	redisRepo repository.RedisRepo
	cm        *conn.ConnectionManager
	logger    *zap.Logger
}

// NewGroupMsgConsumer 创建一个新的 GroupMsgConsumer。
func NewGroupMsgConsumer(ch *amqp.Channel, mysqlRepo repository.MySQLRepo, redisRepo repository.RedisRepo, cm *conn.ConnectionManager, logger *zap.Logger) *GroupMsgConsumer {
	return &GroupMsgConsumer{
		ch:        ch,
		mysqlRepo: mysqlRepo,
		redisRepo: redisRepo,
		cm:        cm,
		logger:    logger,
	}
}

// Start 开始消费来自 group_msg_fanout 队列的消息。
// 在 goroutine 中运行；阻塞直到通道关闭或上下文被取消。
// 使用手动确认：成功时 ack，失败时 nack+重新入队。
func (c *GroupMsgConsumer) Start(ctx context.Context) error {
	deliveries, err := c.ch.Consume(
		groupMsgQueue,
		"goim-group-msg-consumer", // 消费者标签
		false,                     // autoAck — 我们手动确认
		false,                     // exclusive
		false,                     // noLocal
		false,                     // noWait
		nil,                       // args
	)
	if err != nil {
		return fmt.Errorf("消费 group_msg_fanout 失败: %w", err)
	}

	c.logger.Info("群消息消费者已启动")

	go func() {
		for d := range deliveries {
			c.handleDelivery(ctx, d)
		}
		c.logger.Info("群消息消费者投递通道已关闭")
	}()

	return nil
}

// handleDelivery 处理单条 AMQP 投递消息。
// 成功时：ack。失败时：nack 并重新入队以重试。
func (c *GroupMsgConsumer) handleDelivery(ctx context.Context, d amqp.Delivery) {
	msg, err := deserializeGroupMsg(d.Body)
	if err != nil {
		c.logger.Error("反序列化群消息失败", zap.Error(err))
		// 消息格式错误 — 丢弃以避免对坏数据进行无限重试
		d.Nack(false, false)
		return
	}

	if err := c.process(ctx, msg); err != nil {
		c.logger.Error("处理群消息失败",
			zap.Int64("msgID", msg.ID),
			zap.Int64("groupID", msg.GroupID),
			zap.Int64("senderID", msg.SenderID),
			zap.Error(err),
		)
		// 临时性错误 — 重新入队以重试
		d.Nack(false, true)
		return
	}

	d.Ack(false)
}

// process 执行群消息的完整扇出逻辑。
func (c *GroupMsgConsumer) process(ctx context.Context, msg *model.GroupMessage) error {
	convID := model.BuildConvID(model.ConvTypeGroup, msg.GroupID, 0)
	timestamp := msg.CreatedAt.UnixMilli()

	// ── 1. 构建群组发件箱的 InboxMessage ──
	outboxMsg := &model.InboxMessage{
		MsgID:       msg.ID,
		ClientMsgID: msg.ClientMsgID,
		ConvID:      convID,
		ConvType:    model.ConvTypeGroup,
		FromID:      msg.SenderID,
		ToID:        msg.GroupID,
		MsgType:     msg.MsgType,
		Content:     msg.Content,
		GroupSeq:    msg.GroupSeq,
		Timestamp:   timestamp,
	}

	// ── 2. 写入群组发件箱 ──
	if err := c.redisRepo.WriteOutbox(ctx, msg.GroupID, outboxMsg); err != nil {
		return fmt.Errorf("写入群组发件箱失败: %w", err)
	}

	// ── 3. 获取群组成员 ──
	memberIDs, err := c.redisRepo.GetGroupMembers(ctx, msg.GroupID)
	if err != nil {
		return fmt.Errorf("获取群组成员失败: %w", err)
	}

	if len(memberIDs) == 0 {
		c.logger.Warn("群组没有成员",
			zap.Int64("groupID", msg.GroupID),
		)
		// 即使没有成员，仍继续持久化到 MySQL — 发件箱已写入
	}

	// ── 4. 对每个成员：更新 conv_list、增加未读计数、通过 WS 推送 ──
	convSummary := buildGroupConvSummary(convID, msg)
	summaryJSON, err := json.Marshal(convSummary)
	if err != nil {
		c.logger.Warn("序列化对话摘要失败", zap.Error(err))
		summaryJSON = []byte(convID) // 回退方案
	}

	for _, memberID := range memberIDs {
		// 更新每个成员（包括发送者）的 conv_list
		if err := c.redisRepo.UpdateConvList(ctx, memberID, convID, string(summaryJSON), timestamp); err != nil {
			c.logger.Warn("更新成员 conv_list 失败",
				zap.Int64("memberID", memberID),
				zap.Error(err),
			)
			// 非关键操作
		}

		// 对除发送者外的所有成员增加未读计数
		if memberID != msg.SenderID {
			if err := c.redisRepo.IncrementUnread(ctx, memberID, convID); err != nil {
				c.logger.Warn("增加成员未读计数失败",
					zap.Int64("memberID", memberID),
					zap.Error(err),
				)
				// 非关键操作
			}

			// 将 InboxMessage 推送给在线成员（跳过发送者）
			pushToConnection(c.cm, c.logger, memberID, protocol.TypeMsg, outboxMsg)
		}
	}

	// ── 5. 插入 MySQL ──
	if c.mysqlRepo != nil {
		if err := c.mysqlRepo.InsertGroupMessage(ctx, msg); err != nil {
			return fmt.Errorf("插入群消息到 MySQL: %w", err)
		}
	}

	// ── 6. 裁剪发件箱 ZSet ──
	if err := c.redisRepo.TrimOutbox(ctx, msg.GroupID, outboxMaxCount); err != nil {
		c.logger.Warn("裁剪群组发件箱失败", zap.Error(err))
	}

	// ── 7. 消息处理成功后再向发送者确认 ──
	// ACK 不能早于 Redis 发件箱写入，否则客户端收到 ACK 后立即撤回/删除会查不到消息。
	if msg.ClientMsgID != "" {
		ack := &model.ServerAck{
			ClientMsgID: msg.ClientMsgID,
			ServerMsgID: msg.ID,
			GroupSeq:    msg.GroupSeq,
			Timestamp:   time.Now().UnixMilli(),
		}
		pushToConnection(c.cm, c.logger, msg.SenderID, protocol.TypeServerAck, ack)
	}

	c.logger.Debug("群消息处理完成",
		zap.Int64("msgID", msg.ID),
		zap.Int64("groupID", msg.GroupID),
		zap.Int64("groupSeq", msg.GroupSeq),
		zap.Int64("senderID", msg.SenderID),
		zap.Int("memberCount", len(memberIDs)),
	)

	return nil
}

// ── 辅助函数 ──

// deserializeGroupMsg 将 AMQP 消息体解析为 GroupMessage。
func deserializeGroupMsg(body []byte) (*model.GroupMessage, error) {
	var msg model.GroupMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		return nil, fmt.Errorf("反序列化群消息: %w", err)
	}
	if msg.GroupID == 0 {
		return nil, fmt.Errorf("无效的群消息: groupID=%d", msg.GroupID)
	}
	return &msg, nil
}

// buildGroupConvSummary 为群组对话创建 ConvSummary。
// TargetName 留空 — 在同步期间通过群组信息查询填充。
func buildGroupConvSummary(convID string, msg *model.GroupMessage) *model.ConvSummary {
	return &model.ConvSummary{
		ConvID:      convID,
		ConvType:    model.ConvTypeGroup,
		TargetID:    msg.GroupID,
		LastMsg:     truncateContent(msg.Content, 50),
		LastMsgTime: msg.CreatedAt.UnixMilli(),
	}
}
