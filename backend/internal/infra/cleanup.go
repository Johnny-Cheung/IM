package infra

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ──────────────────────────────────────────────────────
// 常量
// ──────────────────────────────────────────────────────

const (
	// 时间阈值：删除超过3天的条目。
	cleanupAgeDays = 3

	// 每种键类型保留的最大条目数。
	maxInboxEntries    = 1000
	maxOutboxEntries   = 500
	maxTimelineEntries = 100

	// SCAN 批次大小。
	scanBatchSize = 100
)

// SCAN 使用的键前缀模式。
var cleanupPatterns = []string{
	"inbox:*",
	"outbox:*",
	"timeline:*",
	"conv_list:*",
}

var indexedMessageOrderPatterns = []string{
	"inbox_order:*",
	"outbox_order:*",
	"conv_order:*",
}

// ──────────────────────────────────────────────────────
// StartCleanupTask — 启动定期清理goroutine
// ──────────────────────────────────────────────────────

// StartCleanupTask 启动一个后台goroutine，按给定间隔运行 CleanupExpiredData。
// 它会立即返回。
// interval 参数允许在测试中覆盖；生产环境中传入 1 * time.Hour。
func StartCleanupTask(rdb *redis.Client, logger *zap.Logger, interval time.Duration) {
	go func() {
		// 启动时立即运行一次，然后定期运行。
		CleanupExpiredData(rdb, logger)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			CleanupExpiredData(rdb, logger)
		}
	}()
}

// ──────────────────────────────────────────────────────
// CleanupExpiredData — 单次扫描 + 修剪
// ──────────────────────────────────────────────────────

// CleanupExpiredData 扫描所有 inbox/outbox/timeline/conv_list 键，
// 并对每个键执行以下修剪操作：
//  1. ZREMRANGEBYSCORE — 删除超过3天的条目
//  2. ZREMRANGEBYRANK  — 限制最大条目数（inbox: 1000, outbox: 500, timeline: 100）
//     conv_list 键仅按时间修剪（不限数量上限）。
func CleanupExpiredData(rdb *redis.Client, logger *zap.Logger) {
	ctx := context.Background()
	// inbox/outbox/timeline/conv_list 的 ZSet score 统一使用 Unix 毫秒时间戳。
	threshold := time.Now().Add(-cleanupAgeDays * 24 * time.Hour).UnixMilli()

	for _, pattern := range cleanupPatterns {
		prefix := strings.SplitN(pattern, ":*", 2)[0] // "inbox"、"outbox"等

		var cursor uint64
		for {
			keys, nextCursor, err := rdb.Scan(ctx, cursor, pattern, scanBatchSize).Result()
			if err != nil {
				logger.Error("SCAN 失败",
					zap.String("pattern", pattern),
					zap.Error(err),
				)
				break
			}

			for _, key := range keys {
				cleanKey(rdb, ctx, logger, key, prefix, threshold)
			}

			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}
	}
	cleanupIndexedMessageData(rdb, logger, threshold)
	cleanupConfirmedPersistEvents(rdb, logger)
}

// cleanupIndexedMessageData keeps the stable order-ZSET/data-HASH layout in
// sync.  Removing only ZSET entries would leak the corresponding HASH values.
func cleanupIndexedMessageData(rdb *redis.Client, logger *zap.Logger, threshold int64) {
	ctx := context.Background()
	for _, pattern := range indexedMessageOrderPatterns {
		var cursor uint64
		for {
			keys, next, err := rdb.Scan(ctx, cursor, pattern, scanBatchSize).Result()
			if err != nil {
				logger.Warn("scan indexed message keys failed", zap.String("pattern", pattern), zap.Error(err))
				break
			}
			for _, orderKey := range keys {
				cleanIndexedOrder(rdb, ctx, logger, orderKey, threshold)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
}

func cleanIndexedOrder(rdb *redis.Client, ctx context.Context, logger *zap.Logger, orderKey string, threshold int64) {
	dataKey, maxCount := indexedDataKeyAndMax(orderKey)
	if dataKey == "" {
		return
	}
	remove := func(ids []string) {
		if len(ids) == 0 {
			return
		}
		pipe := rdb.Pipeline()
		pipe.ZRem(ctx, orderKey, stringSliceToInterface(ids)...)
		pipe.HDel(ctx, dataKey, ids...)
		if _, err := pipe.Exec(ctx); err != nil {
			logger.Warn("trim indexed message data failed", zap.String("key", orderKey), zap.Error(err))
		}
	}
	old, err := rdb.ZRangeByScore(ctx, orderKey, &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(threshold, 10)}).Result()
	if err != nil {
		logger.Warn("read old indexed message ids failed", zap.String("key", orderKey), zap.Error(err))
		return
	}
	remove(old)
	if maxCount <= 0 {
		return
	}
	card, err := rdb.ZCard(ctx, orderKey).Result()
	if err != nil {
		return
	}
	if card <= int64(maxCount) {
		return
	}
	ids, err := rdb.ZRange(ctx, orderKey, 0, card-int64(maxCount)-1).Result()
	if err != nil {
		return
	}
	remove(ids)
}

func indexedDataKeyAndMax(orderKey string) (string, int) {
	switch {
	case strings.HasPrefix(orderKey, "inbox_order:"):
		return strings.Replace(orderKey, "inbox_order:", "inbox_data:", 1), maxInboxEntries
	case strings.HasPrefix(orderKey, "outbox_order:"):
		return strings.Replace(orderKey, "outbox_order:", "outbox_data:", 1), maxOutboxEntries
	case strings.HasPrefix(orderKey, "conv_order:"):
		return strings.Replace(orderKey, "conv_order:", "conv_meta:", 1), 0
	default:
		return "", 0
	}
}

func stringSliceToInterface(values []string) []interface{} {
	out := make([]interface{}, len(values))
	for i, v := range values {
		out[i] = v
	}
	return out
}

// cleanupConfirmedPersistEvents is deliberately marker-driven.  It never
// trims by age alone, so an outage cannot delete an unconfirmed persistence
// event from the Stream.
func cleanupConfirmedPersistEvents(rdb *redis.Client, logger *zap.Logger) {
	ctx := context.Background()
	entries, err := rdb.XRangeN(ctx, "message_persist_stream", "-", "+", 1000).Result()
	if err != nil && err != redis.Nil {
		logger.Warn("scan message persistence stream failed", zap.Error(err))
		return
	}
	for _, entry := range entries {
		confirmed, err := rdb.SIsMember(ctx, "message_persist_confirmed", entry.ID).Result()
		if err != nil || !confirmed {
			continue
		}
		if err := rdb.XAck(ctx, "message_persist_stream", "goim-message-relay", entry.ID).Err(); err != nil {
			logger.Warn("ack stale confirmed stream event failed", zap.String("id", entry.ID), zap.Error(err))
			continue
		}
		if err := rdb.XDel(ctx, "message_persist_stream", entry.ID).Err(); err != nil {
			logger.Warn("delete stale confirmed stream event failed", zap.String("id", entry.ID), zap.Error(err))
			continue
		}
		_ = rdb.SRem(ctx, "message_persist_confirmed", entry.ID).Err()
	}
}

// ──────────────────────────────────────────────────────
// cleanKey — 对单个键执行时间和数量修剪
// ──────────────────────────────────────────────────────

func cleanKey(rdb *redis.Client, ctx context.Context, logger *zap.Logger, key string, prefix string, threshold int64) {
	// 1. 基于时间的修剪：删除 score <= threshold 的条目
	maxScore := strconv.FormatInt(threshold, 10)
	if err := rdb.ZRemRangeByScore(ctx, key, "0", maxScore).Err(); err != nil {
		logger.Error("ZRemRangeByScore 失败",
			zap.String("key", key),
			zap.Error(err),
		)
		return
	}

	// 2. 基于排名的修剪：限制最大条目数（conv_list 不做数量限制）
	maxCount := maxEntriesForPrefix(prefix)
	if maxCount > 0 {
		card, err := rdb.ZCard(ctx, key).Result()
		if err != nil {
			logger.Error("ZCard 失败",
				zap.String("key", key),
				zap.Error(err),
			)
			return
		}
		if card > int64(maxCount) {
			removeCount := card - int64(maxCount)
			if err := rdb.ZRemRangeByRank(ctx, key, 0, removeCount-1).Err(); err != nil {
				logger.Error("ZRemRangeByRank 失败",
					zap.String("key", key),
					zap.Error(err),
				)
			}
		}
	}
}

// maxEntriesForPrefix 返回指定键前缀的最大保留条目数。
// conv_list 返回 0（不做基于排名的修剪）。
func maxEntriesForPrefix(prefix string) int {
	switch prefix {
	case "inbox":
		return maxInboxEntries
	case "outbox":
		return maxOutboxEntries
	case "timeline":
		return maxTimelineEntries
	case "conv_list":
		return 0 // 不做数量限制
	default:
		return 0
	}
}

// ──────────────────────────────────────────────────────
// extractIDFromKey — 将 "prefix:id" 解析为 int64
// ──────────────────────────────────────────────────────

func extractIDFromKey(key string) (int64, error) {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) != 2 || parts[1] == "" {
		return 0, fmt.Errorf("无效的键格式：%s", key)
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("键 %s 中的无效ID：%w", key, err)
	}
	return id, nil
}
