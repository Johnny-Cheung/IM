package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/goim/goim/internal/model"
)

// MQRepo 定义了消息服务所需的 MQ 发布操作。
// 该接口便于在测试中进行 mock。
type MQRepo interface {
	PublishPrivateMsg(ctx context.Context, msg *model.PrivateMessage) error
	PublishGroupMsg(ctx context.Context, msg *model.GroupMessage) error
	PublishMomentPush(ctx context.Context, moment *model.Moment) error
	PublishLikeEvent(ctx context.Context, evt *model.LikeEvent) error
}

// ──────────────────────────────────────────────────────
// MQRepoImpl — 基于 amqp091-go 的具体实现
// ──────────────────────────────────────────────────────

type MQRepoImpl struct {
	ch          *amqp.Channel
	confirmOnce sync.Once
	confirmCh   chan amqp.Confirmation
	returnCh    chan amqp.Return
	confirmErr  error
	publishMu   sync.Mutex
}

func NewMQRepo(ch *amqp.Channel) *MQRepoImpl {
	return &MQRepoImpl{ch: ch}
}

// PublishPersistEvent publishes the Redis Stream event and waits for a
// RabbitMQ publisher confirmation.  A successful PublishWithContext call only
// means the client accepted the frame; the confirm is the durability boundary
// used by the Stream relay before it ACKs/deletes the Stream entry.
func (m *MQRepoImpl) PublishPersistEvent(ctx context.Context, evt *model.MessagePersistEvent) error {
	if m.ch == nil {
		return fmt.Errorf("amqp channel is nil")
	}
	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal persist event: %w", err)
	}
	m.publishMu.Lock()
	defer m.publishMu.Unlock()
	m.confirmOnce.Do(func() {
		m.confirmErr = m.ch.Confirm(false)
		if m.confirmErr == nil {
			m.confirmCh = m.ch.NotifyPublish(make(chan amqp.Confirmation, 1))
			m.returnCh = m.ch.NotifyReturn(make(chan amqp.Return, 1))
		}
	})
	if m.confirmErr != nil {
		return fmt.Errorf("enable publisher confirm: %w", m.confirmErr)
	}
	pubCtx, cancel := context.WithTimeout(ctx, mqPublishTimeout)
	defer cancel()
	if err := m.ch.PublishWithContext(pubCtx, "", "message_persist", true, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: 2, Body: body, MessageId: strconv.FormatInt(evt.ServerMsgID, 10)}); err != nil {
		return err
	}
	select {
	case conf, ok := <-m.confirmCh:
		if !ok {
			return fmt.Errorf("publisher confirm channel closed")
		}
		if !conf.Ack {
			return fmt.Errorf("rabbitmq publisher nack for message %d", evt.ServerMsgID)
		}
		return nil
	case returned, ok := <-m.returnCh:
		if !ok {
			return fmt.Errorf("publisher return channel closed")
		}
		return fmt.Errorf("rabbitmq returned persistence message: code=%d text=%s", returned.ReplyCode, returned.ReplyText)
	case <-pubCtx.Done():
		return fmt.Errorf("publisher confirm timeout: %w", pubCtx.Err())
	}
}

const mqPublishTimeout = 5 * time.Second

func (m *MQRepoImpl) PublishPrivateMsg(ctx context.Context, msg *model.PrivateMessage) error {
	if m.ch == nil {
		return fmt.Errorf("amqp 通道为空")
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal 私聊消息失败: %w", err)
	}
	publishCtx, cancel := context.WithTimeout(ctx, mqPublishTimeout)
	defer cancel()
	return m.ch.PublishWithContext(
		publishCtx,
		"",                    // exchange（默认）
		"private_msg_persist", // routing key = 队列名称
		false,                 // 强制
		false,                 // 立即
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: 2, // 持久化
		},
	)
}

func (m *MQRepoImpl) PublishGroupMsg(ctx context.Context, msg *model.GroupMessage) error {
	if m.ch == nil {
		return fmt.Errorf("amqp 通道为空")
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal 群聊消息失败: %w", err)
	}
	publishCtx, cancel := context.WithTimeout(ctx, mqPublishTimeout)
	defer cancel()
	return m.ch.PublishWithContext(
		publishCtx,
		"",                 // exchange（默认）
		"group_msg_fanout", // routing key = 队列名称
		false,              // 强制
		false,              // 立即
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: 2, // 持久化
		},
	)
}

func (m *MQRepoImpl) PublishMomentPush(ctx context.Context, moment *model.Moment) error {
	if m.ch == nil {
		return fmt.Errorf("amqp 通道为空")
	}
	body, err := json.Marshal(moment)
	if err != nil {
		return fmt.Errorf("marshal 朋友圈消息失败: %w", err)
	}
	publishCtx, cancel := context.WithTimeout(ctx, mqPublishTimeout)
	defer cancel()
	return m.ch.PublishWithContext(
		publishCtx,
		"",            // exchange（默认）
		"moment_push", // routing key = 队列名称
		false,         // 强制
		false,         // 立即
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: 2, // 持久化
		},
	)
}

// PublishLikeEvent 将点赞/取消赞事件投递到 like_persist 队列，由消费者异步批量削峰写入 MySQL。
func (m *MQRepoImpl) PublishLikeEvent(ctx context.Context, evt *model.LikeEvent) error {
	if m.ch == nil {
		return fmt.Errorf("amqp 通道为空")
	}
	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal 点赞事件失败: %w", err)
	}
	publishCtx, cancel := context.WithTimeout(ctx, mqPublishTimeout)
	defer cancel()
	return m.ch.PublishWithContext(
		publishCtx,
		"",             // exchange（默认）
		"like_persist", // routing key = 队列名称
		false,          // 强制
		false,          // 立即
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: 2, // 持久化
		},
	)
}
