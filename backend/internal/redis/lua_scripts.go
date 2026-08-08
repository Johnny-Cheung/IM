package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// LoadLuaScripts 将所有 Lua 脚本预加载到 Redis 中，以优化 EVALSHA 调用。
// go-redis 在首次 Eval 调用时会自动缓存 SHA，因此显式预加载并非严格必要。
// 此函数可在启动时调用以预热缓存，使后续调用使用 EVALSHA 而非 EVAL。
func LoadLuaScripts(rdb *goredis.Client, ctx context.Context) error {
	scripts := []string{
		luaPrivateMsgWrite,
		luaGroupMsgWrite,
		luaPrivateMsgCheck,
		luaGroupMsgCheck,
		luaInboxMarkRead,
		luaRevokeMsg,
		luaMomentLike,
		luaMomentUnlike,
	}
	for _, s := range scripts {
		if err := rdb.ScriptLoad(ctx, s).Err(); err != nil {
			return err
		}
	}
	return nil
}
