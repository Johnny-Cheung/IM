package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

const persistMessageQueue = "message_persist"

// MessagePersistConsumer has one responsibility: idempotently archive the
// immutable event in MySQL.  Redis read-model writes and WebSocket fanout are
// intentionally absent; they already happened in the sending Lua script and
// relay respectively.
type MessagePersistConsumer struct {
	ch         *amqp.Channel
	mysqlRepo  repository.MySQLRepo
	logger     *zap.Logger
	maxRetries int
}

func NewMessagePersistConsumer(ch *amqp.Channel, mysqlRepo repository.MySQLRepo, logger *zap.Logger) *MessagePersistConsumer {
	return &MessagePersistConsumer{ch: ch, mysqlRepo: mysqlRepo, logger: logger, maxRetries: 5}
}

func (c *MessagePersistConsumer) Start(ctx context.Context) error {
	if c.ch == nil {
		return fmt.Errorf("message persist consumer channel is nil")
	}
	deliveries, err := c.ch.Consume(persistMessageQueue, "goim-message-persist-consumer", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", persistMessageQueue, err)
	}
	go func() {
		for d := range deliveries {
			c.handle(ctx, d)
		}
		c.logger.Info("message persistence consumer stopped")
	}()
	return nil
}

func (c *MessagePersistConsumer) handle(ctx context.Context, d amqp.Delivery) {
	var evt model.MessagePersistEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil || evt.ServerMsgID == 0 {
		c.logger.Error("invalid message persistence event", zap.Error(err))
		_ = d.Nack(false, false)
		return
	}
	if c.mysqlRepo == nil {
		_ = d.Ack(false)
		return
	}
	var err error
	if evt.ConvType == model.ConvTypePrivate {
		err = c.mysqlRepo.InsertPrivateMessage(ctx, &model.PrivateMessage{ID: evt.ServerMsgID, ClientMsgID: evt.ClientMsgID, SenderID: evt.SenderID, ReceiverID: evt.ReceiverID, Content: evt.Content, MsgType: evt.MsgType, CreatedAt: time.UnixMilli(evt.ServerTimestamp)})
	} else if evt.ConvType == model.ConvTypeGroup {
		err = c.mysqlRepo.InsertGroupMessage(ctx, &model.GroupMessage{ID: evt.ServerMsgID, ClientMsgID: evt.ClientMsgID, GroupID: evt.GroupID, SenderID: evt.SenderID, Content: evt.Content, MsgType: evt.MsgType, GroupSeq: evt.GroupSeq, CreatedAt: time.UnixMilli(evt.ServerTimestamp)})
	} else {
		_ = d.Nack(false, false)
		return
	}
	if err == nil {
		_ = d.Ack(false)
		return
	}
	c.retryOrDeadLetter(d, err)
}

func (c *MessagePersistConsumer) retryOrDeadLetter(d amqp.Delivery, cause error) {
	count := 0
	if raw, ok := d.Headers["x-retry-count"]; ok {
		switch v := raw.(type) {
		case int:
			count = v
		case int32:
			count = int(v)
		case int64:
			count = int(v)
		case string:
			fmt.Sscanf(v, "%d", &count)
		}
	}
	if count >= c.maxRetries {
		c.logger.Error("message persistence moved to dead letter", zap.Int("retries", count), zap.Error(cause))
		_ = d.Nack(false, false)
		return
	}
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers["x-retry-count"] = count + 1
	if err := c.ch.Publish("", "message_persist", false, false, amqp.Publishing{ContentType: d.ContentType, DeliveryMode: 2, Body: d.Body, Headers: headers}); err != nil {
		c.logger.Error("republish message persistence retry failed", zap.Error(err))
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}
