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

	// failBacklogAgeThreshold 是事件在 Stream 内重试的最大年龄：投递失败且
	// 年龄超过阈值的事件移入失败待办并从 Stream 摘除，防止 Stream 无限堆积。
	failBacklogAgeThreshold = 30 * time.Minute
	// failBacklogWarnThreshold 是失败待办长度的告警阈值。
	failBacklogWarnThreshold = 1000
	// backlogReplayBatch 是每轮循环最多重放的待办条数。
	backlogReplayBatch = 100
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
			r.replayBacklog(ctx)
			retry.Reset(0)
			continue
		}
		for _, entry := range entries {
			if err := r.handle(ctx, entry); err != nil {
				r.logger.Warn("relay message failed; leaving Stream entry pending", zap.String("streamID", entry.ID), zap.Error(err))
				// 失败隔离：不中断本批。失败条目留在 pending，由 10s 后的
				// XAUTOCLAIM 单独接管重试，同批其余消息照常投递。
			}
		}
		r.replayBacklog(ctx)
		retry.Reset(0)
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
		// 投递失败且事件已超龄（Stream 内已重试约 30 分钟）：移入失败待办并从
		// Stream 摘除，防止 Stream 无限堆积。待办由 replayBacklog 持续重放，
		// 直到环境恢复，因此消息并未丢失。
		if age := time.Since(time.UnixMilli(evt.ServerTimestamp)); age >= failBacklogAgeThreshold {
			if backlogErr := r.moveToBacklog(ctx, entry); backlogErr != nil {
				r.logger.Error("move event to failure backlog failed",
					zap.String("streamID", entry.ID), zap.Int64("msgID", evt.ServerMsgID), zap.Error(backlogErr))
			} else {
				r.logger.Warn("event moved to failure backlog",
					zap.String("streamID", entry.ID), zap.Int64("msgID", evt.ServerMsgID), zap.Duration("age", age))
				return nil
			}
		}
		return fmt.Errorf("publish event %d with confirm: %w", evt.ServerMsgID, err)
	}
	if err := r.store.AckDeletePersistEvent(ctx, r.group, entry.ID); err != nil {
		return fmt.Errorf("ack/delete stream entry after confirm: %w", err)
	}
	r.pushAfterConfirm(evt)
	return nil
}

// moveToBacklog 把事件移入失败待办：先写待办、再摘除 Stream 条目。
// 若两步之间崩溃，事件会同时存在于待办与 Stream（重复投递由 MySQL 幂等消化），
// 但绝不会出现"已摘除却无处保存"的丢失窗口。
func (r *MessageRelay) moveToBacklog(ctx context.Context, entry repository.PersistStreamEntry) error {
	if err := r.store.PushFailedEvent(ctx, entry.Payload); err != nil {
		return err
	}
	return r.store.AckDeletePersistEvent(ctx, r.group, entry.ID)
}

// replayBacklog 尝试重放失败待办中的事件：投递成功则移除，失败则保留待下轮重试。
// 待办无时间压力——环境恢复后自然消化；长度超阈值时告警提示系统处于降级状态。
func (r *MessageRelay) replayBacklog(ctx context.Context) {
	count, err := r.store.FailedEventCount(ctx)
	if err != nil {
		r.logger.Warn("read failure backlog size failed", zap.Error(err))
	} else if count >= failBacklogWarnThreshold {
		r.logger.Error("failure backlog exceeds warning threshold",
			zap.Int64("count", count), zap.Duration("ageThreshold", failBacklogAgeThreshold))
	}
	payloads, err := r.store.ListFailedEvents(ctx, backlogReplayBatch)
	if err != nil {
		r.logger.Warn("read failure backlog failed", zap.Error(err))
		return
	}
	for _, payload := range payloads {
		evt, err := redislua.DecodePersistEvent(payload)
		if err != nil {
			r.logger.Error("discarding malformed backlog event", zap.Error(err))
			_ = r.store.RemoveFailedEvent(ctx, payload)
			continue
		}
		if err := r.publisher.PublishPersistEvent(ctx, evt); err != nil {
			r.logger.Warn("backlog replay failed; keeping event for next round", zap.Int64("msgID", evt.ServerMsgID), zap.Error(err))
			continue
		}
		if err := r.store.RemoveFailedEvent(ctx, payload); err != nil {
			r.logger.Warn("remove replayed backlog event failed", zap.Int64("msgID", evt.ServerMsgID), zap.Error(err))
		}
	}
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
