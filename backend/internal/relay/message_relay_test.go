package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/protocol"
	"github.com/goim/goim/internal/repository"
)

type relayStoreStub struct {
	acked      string
	pushed     [][]byte
	removed    [][]byte
	backlog    [][]byte
	backlogErr error
}

func (s *relayStoreStub) EnsurePersistGroup(context.Context, string, string) error { return nil }
func (s *relayStoreStub) ReadPersistEvents(context.Context, string, string, time.Duration, int) ([]repository.PersistStreamEntry, error) {
	return nil, nil
}
func (s *relayStoreStub) ClaimPersistEvents(context.Context, string, string, time.Duration, int) ([]repository.PersistStreamEntry, error) {
	return nil, nil
}
func (s *relayStoreStub) AckDeletePersistEvent(_ context.Context, _ string, id string) error {
	s.acked = id
	return nil
}
func (s *relayStoreStub) TrimPersistStream(context.Context, string, time.Duration, int) error {
	return nil
}
func (s *relayStoreStub) PushFailedEvent(_ context.Context, payload []byte) error {
	s.pushed = append(s.pushed, payload)
	return nil
}
func (s *relayStoreStub) ListFailedEvents(_ context.Context, limit int) ([][]byte, error) {
	if s.backlogErr != nil {
		return nil, s.backlogErr
	}
	if limit <= 0 || limit > len(s.backlog) {
		limit = len(s.backlog)
	}
	return s.backlog[:limit], nil
}
func (s *relayStoreStub) RemoveFailedEvent(_ context.Context, payload []byte) error {
	s.removed = append(s.removed, payload)
	for i, p := range s.backlog {
		if string(p) == string(payload) {
			s.backlog = append(s.backlog[:i], s.backlog[i+1:]...)
			break
		}
	}
	return nil
}
func (s *relayStoreStub) FailedEventCount(context.Context) (int64, error) {
	return int64(len(s.backlog)), nil
}

type relayPublisherStub struct {
	event *model.MessagePersistEvent
	err   error
}

func (p *relayPublisherStub) PublishPersistEvent(_ context.Context, e *model.MessagePersistEvent) error {
	p.event = e
	return p.err
}

func TestRelayPublishesThenAcknowledgesAndPushes(t *testing.T) {
	store := &relayStoreStub{}
	publisher := &relayPublisherStub{}
	cm := conn.NewConnectionManager()
	sender := conn.NewClientConnection(1, nil)
	receiver := conn.NewClientConnection(2, nil)
	cm.Register(1, sender)
	cm.Register(2, receiver)
	r := NewMessageRelay(store, publisher, cm, zap.NewNop())
	evt := model.MessagePersistEvent{ServerMsgID: 1001, ClientMsgID: "c1", ConvType: model.ConvTypePrivate, SenderID: 1, ReceiverID: 2, Content: "hello", MsgType: model.MsgTypeText, ServerTimestamp: 1234}
	raw, err := json.Marshal(evt)
	require.NoError(t, err)
	require.NoError(t, r.handle(context.Background(), repository.PersistStreamEntry{ID: "1-0", Payload: raw}))
	require.Equal(t, evt.ServerMsgID, publisher.event.ServerMsgID)
	require.Equal(t, "1-0", store.acked)
	ackRaw := <-sender.SendCh
	ack, err := protocol.DecodeMsg(ackRaw)
	require.NoError(t, err)
	require.Equal(t, protocol.TypeServerAck, ack.Type)
	msgRaw := <-receiver.SendCh
	msg, err := protocol.DecodeMsg(msgRaw)
	require.NoError(t, err)
	require.Equal(t, protocol.TypeMsg, msg.Type)
}

func TestRelayMovesAgedEventToBacklog(t *testing.T) {
	store := &relayStoreStub{}
	publisher := &relayPublisherStub{err: fmt.Errorf("publish failed")}
	r := NewMessageRelay(store, publisher, nil, zap.NewNop())
	// 超龄事件（30 分钟前）：投递失败 → 移入失败待办 + 从 Stream 摘除，且不报错
	evt := model.MessagePersistEvent{ServerMsgID: 1002, ConvType: model.ConvTypePrivate, SenderID: 1, ReceiverID: 2, Content: "old", MsgType: model.MsgTypeText, ServerTimestamp: time.Now().Add(-31 * time.Minute).UnixMilli()}
	raw, err := json.Marshal(evt)
	require.NoError(t, err)
	err = r.handle(context.Background(), repository.PersistStreamEntry{ID: "1-0", Payload: raw})
	require.NoError(t, err, "超龄事件移入待办后应视为已处理")
	require.Len(t, store.pushed, 1, "事件应被压入失败待办")
	require.Equal(t, string(raw), string(store.pushed[0]))
	require.Equal(t, "1-0", store.acked, "待办写入成功后应从 Stream 摘除")
	require.Equal(t, evt.ServerMsgID, publisher.event.ServerMsgID, "publish 被尝试过（失败后才触发待办转移）")
	require.NotNil(t, publisher.err, "publish 应处于失败状态")
}

func TestRelayKeepsFreshEventPendingOnPublishFailure(t *testing.T) {
	store := &relayStoreStub{}
	publisher := &relayPublisherStub{err: fmt.Errorf("publish failed")}
	r := NewMessageRelay(store, publisher, nil, zap.NewNop())
	// 新事件投递失败：留在 pending 重试，不移动待办、不摘除
	evt := model.MessagePersistEvent{ServerMsgID: 1003, ConvType: model.ConvTypePrivate, SenderID: 1, ReceiverID: 2, Content: "fresh", MsgType: model.MsgTypeText, ServerTimestamp: time.Now().UnixMilli()}
	raw, err := json.Marshal(evt)
	require.NoError(t, err)
	err = r.handle(context.Background(), repository.PersistStreamEntry{ID: "2-0", Payload: raw})
	require.Error(t, err, "新事件投递失败应返回错误，留在 pending 重试")
	require.Empty(t, store.pushed, "新事件不应移入待办")
	require.Empty(t, store.acked, "新事件不应被摘除")
}

func TestRelayReplayBacklog(t *testing.T) {
	store := &relayStoreStub{}
	publisher := &relayPublisherStub{}
	r := NewMessageRelay(store, publisher, nil, zap.NewNop())
	evt := model.MessagePersistEvent{ServerMsgID: 1004, ConvType: model.ConvTypePrivate, SenderID: 1, ReceiverID: 2, Content: "replay", MsgType: model.MsgTypeText, ServerTimestamp: time.Now().UnixMilli()}
	raw, err := json.Marshal(evt)
	require.NoError(t, err)
	store.backlog = [][]byte{raw}
	r.replayBacklog(context.Background())
	require.Equal(t, evt.ServerMsgID, publisher.event.ServerMsgID, "待办事件应被重新投递")
	require.Len(t, store.removed, 1, "重放成功后应从待办移除")
	require.Empty(t, store.backlog)
}

func TestRelayReplayBacklogKeepsFailedEvent(t *testing.T) {
	store := &relayStoreStub{}
	publisher := &relayPublisherStub{err: fmt.Errorf("publish still failing")}
	r := NewMessageRelay(store, publisher, nil, zap.NewNop())
	evt := model.MessagePersistEvent{ServerMsgID: 1005, ConvType: model.ConvTypePrivate, SenderID: 1, ReceiverID: 2, Content: "stuck", MsgType: model.MsgTypeText, ServerTimestamp: time.Now().UnixMilli()}
	raw, err := json.Marshal(evt)
	require.NoError(t, err)
	store.backlog = [][]byte{raw}
	r.replayBacklog(context.Background())
	require.Empty(t, store.removed, "重放失败的事件应保留在待办，下轮再试")
	require.Len(t, store.backlog, 1)
}
