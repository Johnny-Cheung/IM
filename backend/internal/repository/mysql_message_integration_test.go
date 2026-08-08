package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/goim/goim/internal/config"
	"github.com/goim/goim/internal/infra"
	"github.com/goim/goim/internal/model"
	"github.com/stretchr/testify/require"
)

func TestPrivateMessageInsertIsIdempotentAndRejectsConflict(t *testing.T) {
	configPath := "../../configs/config.test.yaml"
	if os.Getenv("GOIM_TEST_CONFIG") != "" {
		configPath = os.Getenv("GOIM_TEST_CONFIG")
	}
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Skipf("test config unavailable: %v", err)
	}
	db, err := infra.NewMySQLPool(&cfg.MySQL)
	if err != nil {
		t.Skipf("MySQL unavailable: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("MySQL unavailable: %v", err)
	}
	repo := NewMySQLRepo(db)
	id := time.Now().UnixMilli()*1000 + 777
	defer db.ExecContext(context.Background(), "DELETE FROM private_messages WHERE id = ?", id)
	msg := &model.PrivateMessage{ID: id, ClientMsgID: "mysql-idempotency-test", SenderID: 93001, ReceiverID: 93002, Content: "same", MsgType: model.MsgTypeText, CreatedAt: time.Now()}
	require.NoError(t, repo.InsertPrivateMessage(context.Background(), msg))
	require.NoError(t, repo.InsertPrivateMessage(context.Background(), msg))
	conflict := *msg
	conflict.Content = "different"
	require.Error(t, repo.InsertPrivateMessage(context.Background(), &conflict))
}
