package messaging

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/accept.lua
var acceptScriptSource string

var acceptScript = redis.NewScript(acceptScriptSource)

func hashMessageBody(body string) string {
	digest := sha256.Sum256([]byte(body))
	return hex.EncodeToString(digest[:])
}

func parseRedisUint(value interface{}) (uint64, error) {
	switch typed := value.(type) {
	case string:
		parsed, err := strconv.ParseUint(typed, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse Redis message ID: %w", err)
		}
		return parsed, nil
	case int64:
		if typed < 0 {
			return 0, fmt.Errorf("parse Redis message ID: negative value")
		}
		return uint64(typed), nil
	case uint64:
		return typed, nil
	default:
		return 0, fmt.Errorf("parse Redis message ID: unexpected type %T", value)
	}
}

func parseRedisInt64(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse Redis timestamp: %w", err)
		}
		return parsed, nil
	case int64:
		return typed, nil
	case uint64:
		if typed > uint64(^uint64(0)>>1) {
			return 0, fmt.Errorf("parse Redis timestamp: overflow")
		}
		return int64(typed), nil
	default:
		return 0, fmt.Errorf("parse Redis timestamp: unexpected type %T", value)
	}
}
