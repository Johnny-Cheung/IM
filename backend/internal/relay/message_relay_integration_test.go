package relay

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/infra"
	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

func TestMessageRelayRedisStreamToRabbitMQ(t *testing.T) {
	redisAddr := os.Getenv("GOIM_TEST_REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:16379"
	}
	rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr, DB: 1})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis unavailable: %v", err)
	}
	defer rdb.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A test-local Stream namespace is not currently configurable, so reset the
	// dedicated persistence Stream before and after this isolated integration test.
	require.NoError(t, rdb.Del(ctx, "message_persist_stream", "message_persist_confirmed").Err())
	defer rdb.Del(context.Background(), "message_persist_stream", "message_persist_confirmed")

	amqpURL := os.Getenv("GOIM_TEST_RABBITMQ_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5673/"
	}
	mqConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Skipf("RabbitMQ unavailable: %v", err)
	}
	defer mqConn.Close()
	consumerCh, err := mqConn.Channel()
	require.NoError(t, err)
	defer consumerCh.Close()
	publisherCh, err := mqConn.Channel()
	require.NoError(t, err)
	defer publisherCh.Close()
	require.NoError(t, infra.DeclareQueues(consumerCh))
	_, err = consumerCh.QueuePurge("message_persist", false)
	require.NoError(t, err)
	deliveries, err := consumerCh.Consume("message_persist", "relay-integration-test", false, false, false, false, nil)
	require.NoError(t, err)

	redisRepo := repository.NewRedisRepo(rdb)
	relay := NewMessageRelay(redisRepo, repository.NewMQRepo(publisherCh), conn.NewConnectionManager(), zap.NewNop())
	require.NoError(t, relay.Start(ctx))
	clientID := "relay-integration-test"
	cleanupKeys := []string{"friend:92001:92002", "friend:92002:92001", "msg_dedup:92001:" + clientID, "inbox_order:92001", "inbox_order:92002", "inbox_data:92001", "inbox_data:92002", "conv_order:92001", "conv_order:92002", "conv_meta:92001", "conv_meta:92002", "unread:92002"}
	rdb.Del(ctx, cleanupKeys...)
	defer rdb.Del(context.Background(), cleanupKeys...)
	require.NoError(t, rdb.Set(ctx, "friend:92001:92002", "1", 0).Err())
	require.NoError(t, rdb.Set(ctx, "friend:92002:92001", "1", 0).Err())
	result, err := redisRepo.WritePrivateMessageAtomic(ctx, 92001, &model.SendMessage{ClientMsgID: clientID, ConvType: model.ConvTypePrivate, ToID: 92002, MsgType: model.MsgTypeText, Content: "relay"})
	require.NoError(t, err)
	require.Equal(t, 0, result.ErrCode)
	select {
	case d := <-deliveries:
		var event model.MessagePersistEvent
		require.NoError(t, json.Unmarshal(d.Body, &event))
		require.Equal(t, result.MsgID, event.ServerMsgID)
		require.NoError(t, d.Ack(false))
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for relayed RabbitMQ event")
	}
	require.Eventually(t, func() bool { return rdb.XLen(ctx, "message_persist_stream").Val() == 0 }, 5*time.Second, 50*time.Millisecond)
}
