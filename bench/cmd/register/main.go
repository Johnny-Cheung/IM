// register 批量注册压测账号。
//
// 用法:
//
//	go run ./cmd/register -count 200000 -prefix bench
//
// 分批注册时用 -start 指定起始编号，与压测工具的 bench_{0..N} 命名保持一致：
//
//	go run ./cmd/register -count 60000 -start 140000
//
// 服务端密码校验用 bcrypt（cost 10，故意慢），所以并发过高会让注册速度受 CPU
// 限制。实测 10 并发约 1 万个/分钟（本机 24 核）；再提高并发收益很小。
//
// 关于 5xx 的幂等校验：并发注册全新账号时服务端可能返回 500——users 行已经
// 提交，但随后建会话的事务因 sessions 索引末端间隙锁互等被 InnoDB 回滚
// （详见 internal/bench.Register 的注释）。此时账号其实已经存在，重试注册只会
// 得到 409。所以收到 5xx 后先用该账号登录一次判定真实状态，再决定是否重试。
//
// 重试带随机抖动：并发全部撞在同一次死锁上时，固定退避会让它们同时醒来再撞。
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Johnny-Cheung/IM/bench/internal/bench"
)

const (
	// maxAttempts 是单次注册的总尝试次数（首次 + 重试）。
	maxAttempts = 4
	// retryBase 是重试退避基数，实际延迟为 base<<attempt 再加随机抖动。
	retryBase = 20 * time.Millisecond
)

func main() {
	var (
		base        = flag.String("base", "http://127.0.0.1:8080", "服务端基地址")
		count       = flag.Int("count", 10, "注册数量")
		start       = flag.Int("start", 0, "起始编号")
		prefix      = flag.String("prefix", "bench", "账号前缀")
		password    = flag.String("password", bench.DefaultPassword, "登录口令")
		concurrency = flag.Int("concurrency", 10, "并发数")
	)
	flag.Parse()

	if *count <= 0 {
		log.Fatalf("register: -count 必须为正数")
	}

	client := bench.NewClient(30 * time.Second)
	var (
		ok        int64
		exists    int64
		recovered int64
		failed    int64
		next      int64 = int64(*start)
		end             = int64(*start + *count)
		started         = time.Now()
	)

	fmt.Printf("注册 %d 个账号 (前缀 %s_，编号 %d..%d)，并发 %d\n",
		*count, *prefix, *start, *start+*count-1, *concurrency)

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			// 抖动要真的互相错开，所以每个 worker 用自己的随机源。
			rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(worker)))
			for {
				idx := int(atomic.AddInt64(&next, 1) - 1)
				if idx >= int(end) {
					return
				}
				outcome, err := registerOne(client, *base, *prefix, idx, *password, rng)
				switch outcome {
				case registered:
					n := atomic.AddInt64(&ok, 1)
					if n%5000 == 0 {
						fmt.Printf("  已注册 %d/%d (%.0f%%)\n", n, *count,
							float64(n)/float64(*count)*100)
					}
				case preexisting:
					atomic.AddInt64(&exists, 1)
				case recoveredFrom500:
					// 服务端 500 但账号其实已建：不重试注册，直接计入。
					n := atomic.AddInt64(&recovered, 1)
					if n <= 10 {
						fmt.Printf("  注册 %s 返回 500，但账号已建（建会话死锁被回滚），已计入成功\n",
							bench.Username(*prefix, idx))
					}
				default:
					if n := atomic.AddInt64(&failed, 1); n <= 10 {
						fmt.Fprintf(os.Stderr, "  注册 %s 失败: %v\n",
							bench.Username(*prefix, idx), err)
					}
				}
			}
		}(i)
	}
	wg.Wait()

	fmt.Printf("完成: 新建 %d, 已存在 %d, 500后证实已建 %d, 失败 %d, 耗时 %s\n",
		ok, exists, recovered, failed, time.Since(started).Round(time.Second))
	if failed > 0 {
		os.Exit(1)
	}
}

// outcome 是一次注册尝试的最终归属。
type outcome int

const (
	registered        outcome = iota // 本次确实新建了账号
	preexisting                      // 账号此前已存在
	recoveredFrom500                 // 注册返回 5xx，但登录探测证实账号已建
	registerFailed
)

// registerOne 注册一个账号，带抖动重试与幂等校验。
func registerOne(
	client *http.Client, base, prefix string, idx int, password string, rng *rand.Rand,
) (outcome, error) {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// 指数退避 + 全抖动：延迟在 [0, base<<attempt) 内随机取，
			// 避免所有 worker 在同一时刻再次齐步重试。
			window := retryBase << (attempt - 1)
			sleepRandom(rng, window)
		}

		err := bench.Register(client, base, prefix, idx, password, fmt.Sprintf("压测%d", idx))
		switch {
		case err == nil:
			return registered, nil
		case errors.Is(err, bench.ErrUsernameTaken):
			return preexisting, nil
		}
		lastErr = err

		// 4xx 是确定性拒绝，重试无意义。
		if bench.IsClientError(err) {
			return registerFailed, err
		}

		// 5xx：可能只是建会话那一步失败了，users 行已经落库。
		// 用登录探一次，能登进去就说明账号其实已存在。
		if bench.IsServerError(err) {
			loginErr := probeLogin(client, base, prefix, idx, password)
			if loginErr == nil {
				return recoveredFrom500, nil
			}
		}
	}
	return registerFailed, lastErr
}

// probeLogin 尝试登录一次，仅用于判定账号是否已经存在。
// 任何错误（含 credential 不符）都只表示"这次探测没能证明账号可用"。
func probeLogin(client *http.Client, base, prefix string, idx int, password string) error {
	_, err := bench.Login(client, base, prefix, idx, password)
	return err
}

// sleepRandom 睡一个 [0, window) 的随机时长。
func sleepRandom(rng *rand.Rand, window time.Duration) {
	if window <= 0 {
		return
	}
	time.Sleep(time.Duration(rng.Int64N(int64(window))))
}