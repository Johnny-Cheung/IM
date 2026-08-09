package repository

import (
	"context"
	"os"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// setupTestRedis 返回测试专用 Redis 客户端（DB 1，与本地开发库隔离）；不可用时跳过。
func setupTestRedis(t *testing.T) *goredis.Client {
	t.Helper()
	addr := os.Getenv("GOIM_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:16379"
	}
	rdb := goredis.NewClient(&goredis.Options{Addr: addr, DB: 1})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis 不可用: %v", err)
	}
	return rdb
}

func TestGetUnreadMapWatermark(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()
	ctx := context.Background()
	keys := []string{"unread:93001", "group_seq:93002", "group_read_pos:93001", "user_groups:93001"}
	rdb.Del(ctx, keys...)
	defer rdb.Del(ctx, keys...)

	// 私聊未读走 hash；群会话 hash 字段是迁移残留，必须被忽略
	rdb.HSet(ctx, "unread:93001", "p_1_2", "3", "g_93002", "99")
	// 群：最新 seq=10，我已读到 4 → 未读 6
	rdb.Set(ctx, "group_seq:93002", "10", 0)
	rdb.HSet(ctx, "group_read_pos:93001", "g_93002", "4")
	rdb.SAdd(ctx, "user_groups:93001", "93002")

	repo := NewRedisRepo(rdb)
	got, err := repo.GetUnreadMap(ctx, 93001)
	require.NoError(t, err)
	require.Equal(t, int64(3), got["p_1_2"])
	require.Equal(t, int64(6), got["g_93002"], "群未读 = 群最新 seq − 已读游标")
}

func TestGetUnreadMapWatermarkNoUnread(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()
	ctx := context.Background()
	keys := []string{"group_seq:93003", "group_read_pos:93001", "user_groups:93001", "unread:93001"}
	rdb.Del(ctx, keys...)
	defer rdb.Del(ctx, keys...)

	// 游标 ≥ 最新 seq → 未读为 0，不应出现在结果中
	rdb.Set(ctx, "group_seq:93003", "5", 0)
	rdb.HSet(ctx, "group_read_pos:93001", "g_93003", "5")
	rdb.SAdd(ctx, "user_groups:93001", "93003")

	repo := NewRedisRepo(rdb)
	got, err := repo.GetUnreadMap(ctx, 93001)
	require.NoError(t, err)
	require.NotContains(t, got, "g_93003")
}

func TestGetUnreadMapWatermarkDefensiveNoReadPos(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()
	ctx := context.Background()
	keys := []string{"group_seq:93004", "group_read_pos:93001", "user_groups:93001", "unread:93001"}
	rdb.Del(ctx, keys...)
	defer rdb.Del(ctx, keys...)

	// 游标缺失（迁移/初始化遗漏）：防御性视为已读到最新，未读 0，避免徽标爆炸
	rdb.Set(ctx, "group_seq:93004", "9", 0)
	rdb.SAdd(ctx, "user_groups:93001", "93004")

	repo := NewRedisRepo(rdb)
	got, err := repo.GetUnreadMap(ctx, 93001)
	require.NoError(t, err)
	require.NotContains(t, got, "g_93004")
}

func TestAddGroupMemberRedisInitializesReadPos(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()
	ctx := context.Background()
	keys := []string{"group_members:93020", "user_groups:93021", "group_member_info:93020", "group_read_pos:93021", "group_seq:93020"}
	rdb.Del(ctx, keys...)
	defer rdb.Del(ctx, keys...)

	rdb.Set(ctx, "group_seq:93020", "8", 0)
	repo := NewRedisRepo(rdb)
	require.NoError(t, repo.AddGroupMemberRedis(ctx, 93020, 93021))

	pos, err := repo.GetGroupReadPos(ctx, 93021, "g_93020")
	require.NoError(t, err)
	require.Equal(t, int64(8), pos, "新成员游标应初始化为当前群 seq（历史消息不计入未读）")
}

// TestMigrationGroupReadPosInit 真实执行迁移脚本，验证徽标连续性：
// read_pos = 当前群 seq − hash 中的群未读数，迁移后按水位线算出的未读与迁移前一致。
func TestMigrationGroupReadPosInit(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()
	ctx := context.Background()
	keys := []string{
		"group_members:93010", "group_seq:93010",
		"unread:93011", "unread:93012",
		"group_read_pos:93011", "group_read_pos:93012",
	}
	rdb.Del(ctx, keys...)
	defer rdb.Del(ctx, keys...)

	// 群 93010：最新 seq=10；成员 93011 未读 3（迁移前徽标=3），成员 93012 未读 0
	rdb.Set(ctx, "group_seq:93010", "10", 0)
	rdb.SAdd(ctx, "group_members:93010", "93011", "93012")
	rdb.HSet(ctx, "unread:93011", "g_93010", "3")

	script, err := os.ReadFile("../../scripts/migrations/012_group_read_pos_init.lua")
	require.NoError(t, err)
	_, err = rdb.Eval(ctx, string(script), nil).Result()
	require.NoError(t, err)

	// 迁移后游标：10−3=7 与 10−0=10；水位线计算未读分别为 3 与 0（与迁移前一致）
	pos1, err := rdb.HGet(ctx, "group_read_pos:93011", "g_93010").Result()
	require.NoError(t, err)
	require.Equal(t, "7", pos1)
	pos2, err := rdb.HGet(ctx, "group_read_pos:93012", "g_93010").Result()
	require.NoError(t, err)
	require.Equal(t, "10", pos2)
}
