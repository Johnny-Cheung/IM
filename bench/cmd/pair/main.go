// pair 把压测账号两两配成好友，为消息 QPS 压测准备可用会话。
//
// 用法:
//
//	go run ./cmd/pair -pairs 50 -prefix bench
//
// 账号按序号配对：(0,1)、(2,3)、(4,5)…… 共 pairs 对。每对走完整的真实链路：
// 一方发起申请，另一方接受。不接受这一步不行——服务端在 Lua 接受脚本里用
// SISMEMBER 查 Redis 好友集合，非好友直接返回 not_friends（HTTP 403）。
//
// 好友关系同时被投影到 Redis（服务端 AcceptFriendRequest 会调用
// ensureFriendshipProjection）。本工具不接受 probe 之外的旁路，因为绕过
// 好友 API 就等于跳过 Redis 投影，压测时第一条消息就会 403。
//
// 幂等：重复运行不会重复建关系。若双方已是好友，服务端返回 409 或
// 200/accepted，都按成功处理。
//
// 关于 5xx：并发配对时服务端 CreateFriendRequest 会偶发 InnoDB 死锁 1213
// ——同一类根因，只是发生在 friend_requests 表（两条 FOR UPDATE 都走
// idx_friend_requests_to，新插入的行又落在该索引末端 supremum 间隙）。所以
// 收到 5xx 后先用好友列表/申请列表探一次真实状态，再决定是否重试；重试带
// 随机抖动，避免并发一起撞在同一次死锁上又同时醒来再撞。
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
	// maxAttempts 是单对好友的总尝试次数（首次 + 重试）。
	maxAttempts = 5
	// retryBase 是重试退避基数，实际延迟为 base<<attempt 再加随机抖动。
	retryBase = 30 * time.Millisecond
)

func main() {
	var (
		base        = flag.String("base", "http://127.0.0.1:8080", "服务端基地址")
		pairs       = flag.Int("pairs", 50, "好友对数量")
		prefix      = flag.String("prefix", "bench", "账号前缀")
		password    = flag.String("password", bench.DefaultPassword, "登录口令")
		concurrency = flag.Int("concurrency", 8, "并发对数")
		out         = flag.String("out", "/tmp/im-pairs.txt", "输出文件：每行 sender序号<TAB>recipient序号")
	)
	flag.Parse()

	if *pairs <= 0 {
		log.Fatalf("pair: -pairs 必须为正数")
	}

	var (
		ok        int64
		skipped   int64
		recovered int64
		failed    int64
		next      int64
		started   = time.Now()
	)
	fmt.Printf("配对 %d 对好友（账号 %s_0 .. %s_%d），并发 %d\n",
		*pairs, *prefix, *prefix, *pairs*2-1, *concurrency)

	type pairResult struct {
		index int
		a, b  int
	}
	results := make([]pairResult, *pairs)
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(worker)))
			for {
				p := int(atomic.AddInt64(&next, 1) - 1)
				if p >= *pairs {
					return
				}
				a, b := p*2, p*2+1
				outcome, err := makeFriends(*base, *prefix, a, b, *password, rng)
				switch outcome {
				case paired:
					atomic.AddInt64(&ok, 1)
					mu.Lock()
					results[p] = pairResult{index: p, a: a, b: b}
					mu.Unlock()
				case alreadyPaired:
					atomic.AddInt64(&skipped, 1)
					mu.Lock()
					results[p] = pairResult{index: p, a: a, b: b}
					mu.Unlock()
				case recoveredFrom500:
					n := atomic.AddInt64(&recovered, 1)
					if n <= 10 {
						fmt.Printf("  配对 %s_%d <-> %s_%d 返回 500，但关系已建立（死锁回滚），已计入成功\n",
							*prefix, a, *prefix, b)
					}
					mu.Lock()
					results[p] = pairResult{index: p, a: a, b: b}
					mu.Unlock()
				default:
					if n := atomic.AddInt64(&failed, 1); n <= 10 {
						fmt.Fprintf(os.Stderr, "  配对 %s_%d <-> %s_%d 失败: %v\n",
							*prefix, a, *prefix, b, err)
					}
				}
			}
		}(i)
	}
	wg.Wait()

	// 写出发送方 -> 接收方映射，供 qpsload 按序号取值。
	f, err := os.OpenFile(*out, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		log.Fatalf("pair: 创建输出文件: %v", err)
	}
	for _, r := range results {
		// 双方互为好友，方向任选：统一用序号小的作发送方。
		fmt.Fprintf(f, "%d\t%d\n", r.a, r.b)
	}
	if err := f.Close(); err != nil {
		log.Fatalf("pair: 关闭输出文件: %v", err)
	}

	fmt.Printf("完成: 新建 %d, 已存在 %d, 500后证实已建 %d, 失败 %d, 耗时 %s\n",
		ok, skipped, recovered, failed, time.Since(started).Round(time.Second))
	fmt.Printf("配对文件: %s（%d 对）\n", *out, *pairs)
	if failed > 0 {
		os.Exit(1)
	}
}

// outcome 是一次配对尝试的最终归属。
type outcome int

const (
	paired           outcome = iota // 本次真的建立了关系
	alreadyPaired                   // 此前已是好友
	recoveredFrom500                // 服务端 5xx，但探测证明关系已建立
	pairFailed
)

// makeFriends 让 a 向 b 发起好友申请并接受，完成配对，带抖动重试与幂等校验。
func makeFriends(
	base, prefix string, a, b int, password string, rng *rand.Rand,
) (outcome, error) {
	client := bench.NewClient(20 * time.Second)

	cookieA, err := bench.Login(client, base, prefix, a, password)
	if err != nil {
		return pairFailed, fmt.Errorf("登录 %s_%d: %w", prefix, a, err)
	}
	cookieB, err := bench.Login(client, base, prefix, b, password)
	if err != nil {
		return pairFailed, fmt.Errorf("登录 %s_%d: %w", prefix, b, err)
	}
	nameB := bench.Username(prefix, b)

	// 已经是好友就直接收工，保证可重复运行。
	if isFriend, err := bench.AreFriends(client, base, cookieA, nameB); err == nil && isFriend {
		return alreadyPaired, nil
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			sleepRandom(rng, retryBase<<(attempt-1))
		}

		err := attemptPair(client, base, cookieA, cookieB, prefix, a, b, nameB)
		switch {
		case err == nil:
			return paired, nil
		case errors.Is(err, errAlreadyPaired):
			return alreadyPaired, nil
		}
		lastErr = err

		// 4xx 是确定性拒绝，重试无意义。
		if bench.IsClientError(err) {
			return pairFailed, err
		}

		// 5xx：可能只是事务死锁被回滚，关系其实已经建好。探一次真实状态。
		if bench.IsServerError(err) {
			if isFriend, probeErr := bench.AreFriends(client, base, cookieA, nameB); probeErr == nil && isFriend {
				return recoveredFrom500, nil
			}
		}
	}
	return pairFailed, lastErr
}

// errAlreadyPaired 表示这一对已经是好友，无需再建。
var errAlreadyPaired = errors.New("已经是好友")

// attemptPair 执行一次配对：a 发起申请，b 接受。
//
// 两边各用自己的 cookie：申请由 a 发出，接受由 b 执行，与真实用户操作一致。
func attemptPair(
	client *http.Client, base, cookieA, cookieB, prefix string, a, b int, nameB string,
) error {
	requestID, accepted, err := bench.SendFriendRequest(client, base, cookieA, nameB)
	if err != nil {
		if bench.IsClientError(err) {
			// 409 可能是"已有待处理申请"（上一次尝试死锁回滚后留下的），
			// 这时把那条申请找出来接受掉即可，不必新建。
			if id, findErr := bench.PendingRequestID(client, base, cookieA, nameB); findErr == nil && id != 0 {
				if acceptErr := bench.AcceptFriendRequest(client, base, cookieB, id); acceptErr == nil {
					return nil
				}
			}
			return errAlreadyPaired
		}
		return err
	}
	if accepted {
		// 反向申请已存在，服务端直接建立了双向关系。
		return nil
	}
	if requestID == 0 {
		return fmt.Errorf("服务端未返回 request_id")
	}
	if err := bench.AcceptFriendRequest(client, base, cookieB, requestID); err != nil {
		if bench.IsClientError(err) {
			return errAlreadyPaired
		}
		return err
	}
	return nil
}

// sleepRandom 睡一个 [0, window) 的随机时长。
func sleepRandom(rng *rand.Rand, window time.Duration) {
	if window <= 0 {
		return
	}
	time.Sleep(time.Duration(rng.Int64N(int64(window))))
}