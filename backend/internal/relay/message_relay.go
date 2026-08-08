// Package relay moves durable message events from Redis Stream to RabbitMQ.
// It is intentionally process-local for the first rollout, but only depends
// on interfaces so it can later be extracted into its own binary unchanged.
package relay

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/protocol"
	redislua "github.com/goim/goim/internal/redis"
	"github.com/goim/goim/internal/repository"
)

const (
	persistGroup = "goim-message-relay"
)

type MessageRelay struct {
	store     repository.StreamStore
	publisher repository.PersistPublisher
	cm        *conn.ConnectionManager
	logger    *zap.Logger
	group     string
	consumer  string
}

func NewMessageRelay(store repository.StreamStore, publisher repository.PersistPublisher, cm *conn.ConnectionManager, logger *zap.Logger) *MessageRelay {
	host, _ := os.Hostname()
	return &MessageRelay{store: store, publisher: publisher, cm: cm, logger: logger, group: persistGroup, consumer: fmt.Sprintf("%s-%d", host, os.Getpid())}
}

func (r *MessageRelay) Start(ctx context.Context) error {
	if r.store == nil || r.publisher == nil {
		return fmt.Errorf("message relay dependencies are nil")
	}
	if err := r.store.EnsurePersistGroup(ctx, r.group, r.consumer); err != nil {
		return fmt.Errorf("create persist stream group: %w", err)
	}
	go r.run(ctx)
	r.logger.Info("message stream relay started", zap.String("group", r.group), zap.String("consumer", r.consumer))
	return nil
}

func (r *MessageRelay) run(ctx context.Context) {
	retry := time.NewTimer(0)
	defer retry.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-retry.C:
		}
		entries, err := r.store.ClaimPersistEvents(ctx, r.group, r.consumer, 10*time.Second, 32)
		if err != nil {
			r.logger.Warn("claim pending stream messages failed", zap.Error(err))
			retry.Reset(time.Second)
			continue
		}
		if len(entries) == 0 {
			entries, err = r.store.ReadPersistEvents(ctx, r.group, r.consumer, time.Second, 32)
			if err != nil {
				r.logger.Warn("read message persistence stream failed", zap.Error(err))
				retry.Reset(time.Second)
				continue
			}
		}
		if len(entries) == 0 {
			retry.Reset(0)
			continue
		}
		failed := false
		for _, entry := range entries {
			if err := r.handle(ctx, entry); err != nil {
				r.logger.Warn("relay message failed; leaving Stream entry pending", zap.String("streamID", entry.ID), zap.Error(err))
				failed = true
				break
			}
		}
		if failed {
			retry.Reset(time.Second)
		} else {
			retry.Reset(0)
		}
	}
}

func (r *MessageRelay) handle(ctx context.Context, entry repository.PersistStreamEntry) error {
	evt, err := redislua.DecodePersistEvent(entry.Payload)
	if err != nil {
		// A malformed event cannot be retried successfully.  Log it loudly and
		// remove it so it cannot block the consumer group's pending list.
		r.logger.Error("discarding malformed message persistence event", zap.String("streamID", entry.ID), zap.Error(err))
		return r.store.AckDeletePersistEvent(ctx, r.group, entry.ID)
	}
	if err := r.publisher.PublishPersistEvent(ctx, evt); err != nil {
		return fmt.Errorf("publish event %d with confirm: %w", evt.ServerMsgID, err)
	}
	if err := r.store.AckDeletePersistEvent(ctx, r.group, entry.ID); err != nil {
		return fmt.Errorf("ack/delete stream entry after confirm: %w", err)
	}
	r.pushAfterConfirm(evt)
	return nil
}

func (r *MessageRelay) pushAfterConfirm(evt *model.MessagePersistEvent) {
	if evt.ClientMsgID != "" {
		r.push(evt.SenderID, protocol.TypeServerAck, &model.ServerAck{ClientMsgID: evt.ClientMsgID, ServerMsgID: evt.ServerMsgID, GroupSeq: evt.GroupSeq, Timestamp: evt.ServerTimestamp})
	}
	msg := &model.InboxMessage{MsgID: evt.ServerMsgID, ClientMsgID: evt.ClientMsgID, ConvType: evt.ConvType, FromID: evt.SenderID, MsgType: evt.MsgType, Content: evt.Content, GroupSeq: evt.GroupSeq, Timestamp: evt.ServerTimestamp}
	if evt.ConvType == model.ConvTypePrivate {
		msg.ConvID = model.BuildConvID(model.ConvTypePrivate, evt.SenderID, evt.ReceiverID)
		msg.ToID = evt.ReceiverID
		r.push(evt.ReceiverID, protocol.TypeMsg, msg)
		return
	}
	msg.ConvID = model.BuildConvID(model.ConvTypeGroup, evt.GroupID, 0)
	msg.ToID = evt.GroupID
	for _, uid := range evt.Recipients {
		if uid != evt.SenderID {
			r.push(uid, protocol.TypeMsg, msg)
		}
	}
}

func (r *MessageRelay) push(userID int64, typ string, data interface{}) {
	if r.cm == nil {
		return
	}
	raw, err := protocol.EncodeMsg(typ, data)
	if err != nil {
		r.logger.Error("encode relay websocket message", zap.Error(err))
		return
	}
	client, ok := r.cm.Get(userID)
	if !ok {
		return
	}
	select {
	case client.SendCh <- raw:
	default:
		r.logger.Warn("websocket send buffer full", zap.Int64("userID", userID), zap.String("type", typ))
	}
}
