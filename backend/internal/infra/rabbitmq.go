package infra

import (
	"context"
	"fmt"
	"time"

	"github.com/goim/goim/internal/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

// QueueNames 列出 IM 系统所需的所有持久队列。
var QueueNames = []string{
	"message_persist",
	"message_persist_dlq",
	"message_persist_delay",
	"private_msg_persist",
	"group_msg_fanout",
	"moment_push",
	"like_persist",
	"comment_persist",
}

// NewRabbitMQConn 建立 RabbitMQ 连接并打开一个通道。
func NewRabbitMQConn(cfg *config.RabbitMQConfig) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, ch, nil
}

// ConnectRabbitMQWithRetry 持续尝试建立 RabbitMQ 连接，直到成功或 ctx 被取消。
// waitFn 会在每次失败后收到本次错误和下一次等待时长，可用于记录日志。
func ConnectRabbitMQWithRetry(
	ctx context.Context,
	cfg *config.RabbitMQConfig,
	initialDelay time.Duration,
	maxDelay time.Duration,
	waitFn func(error, time.Duration),
) (*amqp.Connection, *amqp.Channel, error) {
	if initialDelay <= 0 {
		initialDelay = time.Second
	}
	if maxDelay < initialDelay {
		maxDelay = initialDelay
	}

	delay := initialDelay
	for {
		conn, ch, err := NewRabbitMQConn(cfg)
		if err == nil {
			err = DeclareQueues(ch)
			if err != nil {
				_ = ch.Close()
				_ = conn.Close()
			}
		}
		if err == nil {
			return conn, ch, nil
		}
		if waitFn != nil {
			waitFn(err, delay)
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, nil, fmt.Errorf("等待 RabbitMQ 连接时终止: %w", ctx.Err())
		case <-timer.C:
		}

		if delay < maxDelay {
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}
}

// DeclareQueues 在给定通道上声明所有持久队列。
// message_persist_delay 是落库重试的"等待室"：消息进队 5 秒后过期，
// 经死信交换器送回主队列，实现固定间隔退避（无需延迟插件）。
func DeclareQueues(ch *amqp.Channel) error {
	for _, name := range QueueNames {
		var args amqp.Table
		switch name {
		case "message_persist":
			args = amqp.Table{
				"x-dead-letter-exchange":    "",
				"x-dead-letter-routing-key": "message_persist_dlq",
			}
		case "message_persist_delay":
			args = amqp.Table{
				"x-dead-letter-exchange":    "",
				"x-dead-letter-routing-key": "message_persist",
				"x-message-ttl":             5000, // 5 秒延迟
			}
		}
		_, err := ch.QueueDeclare(name, true, false, false, false, args)
		if err != nil {
			return err
		}
	}
	return nil
}
