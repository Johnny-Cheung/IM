package relay

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/protocol"
	"github.com/goim/goim/internal/repository"
)

type relayStoreStub struct{ acked string }

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
