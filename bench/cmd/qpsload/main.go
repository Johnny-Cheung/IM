// qpsload 是私聊消息写入 QPS 压测。
//
// 用法:
//
//	go run ./cmd/qpsload -conns 10  -duration 15s -pairs /tmp/im-pairs.txt -prefix bench
//	go run ./cmd/qpsload -conns 100 -duration 15s -pairs /tmp/im-pairs.txt -prefix bench
//
// 每个连接是一个 goroutine，各自持有一条到服务端的 keep-alive HTTP 连接，
// 按"发 -> 等服务端受理 -> 立刻发下一条"的流水线模式持续施压。QPS 由在途
// 请求数除以单条延迟决定，所以连接数越多、单条越快，吞吐越高——直到链路里
// 的某个共享资源（实测是 Redis 单线程 Lua）成为瓶颈。
//
// 为什么走 HTTP 而不是 WS：当前发消息的入口是
// POST /api/conversations/:username/messages，WebSocket 只承载 focus/blur
// 等已读信号，不再接受发消息。同步路径与旧版一致，仍是 Redis Lua 原子接纳。
//
// 重要：返回 201 只表示消息被 Redis 接纳，此时还没有落库——MySQL 落库是
// 异步的（Stream -> RabbitMQ -> consumer）。所以这里的 QPS 是"接纳吞吐"，
// 不是"持久化吞吐"。跑完必须用 /ops/infra 确认异步链路已经排空，
// 否则测出来的是一段积压。
//
// client_message_id 每条唯一：服务端按它去重（同 ID 同内容算 duplicate 不
// 新建），复用 ID 会让吞吐虚高，因为请求走了去重分支而没有真正建消息。
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Johnny-Cheung/IM/bench/internal/bench"
)

type config struct {
	base     string
	conns    int
	duration time.Duration
	pairs    []pair
	prefix   string
	password string
	timeout  time.Duration
}

// pair 是一对好友的序号：idx 向 peer 发消息。
type pair struct{ idx, peer int }

func main() {
	var (
		base     = flag.String("base", "http://127.0.0.1:8080", "服务端基地址")
		conns    = flag.Int("conns", 10, "并发连接数")
		duration = flag.Duration("duration", 15*time.Second, "压测时长")
		pairFile = flag.String("pairs", "/tmp/im-pairs.txt", "好友对文件：sender序号<TAB>recipient序号")
		prefix   = flag.String("prefix", "bench", "账号前缀")
		password = flag.String("password", bench.DefaultPassword, "登录口令")
		timeout  = flag.Duration("timeout", 10*time.Second, "单请求超时")
	)
	flag.Parse()

	if *conns <= 0 {
		log.Fatalf("qpsload: -conns 必须为正数")
	}
	pairs, err := loadPairs(*pairFile)
	if err != nil {
		log.Fatalf("qpsload: %v", err)
	}
	if len(pairs) == 0 {
		log.Fatalf("qpsload: 好友对文件为空，请先运行 bench/cmd/pair")
	}
	// 连接数超过好友对数时循环复用——一个账号可以向同一好友连发多条消息。
	cfg := config{
		base: *base, conns: *conns, duration: *duration, pairs: pairs,
		prefix: *prefix, password: *password, timeout: *timeout,
	}
	if err := run(cfg); err != nil {
		log.Fatalf("qpsload: %v", err)
	}
}

type result struct {
	sent      int64
	accepted  int64 // 201，真正新建
	duplicate int64 // 200，命中去重（不应出现）
	failed    int64
	firstErr  atomic.Value

	mu        sync.Mutex
	latencies []time.Duration
}

func (r *result) addLatency(d time.Duration) {
	r.mu.Lock()
	r.latencies = append(r.latencies, d)
	r.mu.Unlock()
}

func (r *result) recordError(err error) {
	atomic.AddInt64(&r.failed, 1)
	r.firstErr.CompareAndSwap(nil, err.Error())
}

func run(cfg config) error {
	fmt.Printf("qpsload: conns=%d duration=%s pairs=%d prefix=%s\n",
		cfg.conns, cfg.duration, len(cfg.pairs), cfg.prefix)

	// 并发登录所有需要的账号（连接数可能超过好友对数，需去重）。
	needed := map[int]bool{}
	for i := 0; i < cfg.conns; i++ {
		p := cfg.pairs[i%len(cfg.pairs)]
		needed[p.idx] = true
	}
	cookies, err := loginAll(cfg, needed)
	if err != nil {
		return err
	}
	fmt.Printf("  登录完成: %d 个账号\n", len(cookies))

	res := &result{}
	deadline := time.Now().Add(cfg.duration)
	var wg sync.WaitGroup
	wg.Add(cfg.conns)

	start := time.Now()
	for i := 0; i < cfg.conns; i++ {
		go func(conn int) {
			defer wg.Done()
			// 每个连接一个独立 client → 各自一条 keep-alive 连接，
			// 请求之间只付一次 TCP 握手。
			client := bench.NewKeepAliveClient(cfg.timeout, 2)
			p := cfg.pairs[conn%len(cfg.pairs)]
			senderCookie := cookies[p.idx]
			toName := bench.Username(cfg.prefix, p.peer)
			sendLoop(cfg, client, senderCookie, toName, conn, deadline, res)
		}(i)
	}

	// 进度：每秒报一次瞬时 QPS。
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		last := int64(0)
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				accepted := atomic.LoadInt64(&res.accepted)
				fmt.Printf("  [%s] 成功=%d 失败=%d qps=%d\n",
					time.Since(start).Round(time.Second), accepted,
					atomic.LoadInt64(&res.failed), accepted-last)
				last = accepted
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("\n收到中断，等待连接退出…")
		close(done)
		deadline = time.Now()
	}()

	wg.Wait()
	select {
	case <-done:
	default:
		close(done)
	}

	report(cfg, res, time.Since(start))
	if atomic.LoadInt64(&res.failed) > 0 {
		return fmt.Errorf("存在失败请求，详见上方统计")
	}
	if atomic.LoadInt64(&res.duplicate) > 0 {
		return fmt.Errorf("出现 %d 条去重命中（client_message_id 复用），QPS 不可信",
			atomic.LoadInt64(&res.duplicate))
	}
	return nil
}

// sendLoop 在一条 keep-alive 连接上持续发送，直到截止时刻。
func sendLoop(
	cfg config, client *http.Client, cookie, toName string, conn int,
	deadline time.Time, res *result,
) {
	seq := 0
	for time.Now().Before(deadline) {
		seq++
		// client_message_id 必须全局唯一：同一发送者跨连接也不能撞，
		// 否则命中去重会让吞吐虚高。conn 参与前缀保证唯一。
		msgID := fmt.Sprintf("qps-%d-%d-%d", conn, seq, startStamp)
		t0 := time.Now()
		_, created, err := bench.SendMessage(
			client, cfg.base, cookie, toName, msgID, "压测消息",
		)
		lat := time.Since(t0)

		atomic.AddInt64(&res.sent, 1)
		switch {
		case err != nil:
			res.recordError(err)
		case created:
			atomic.AddInt64(&res.accepted, 1)
			res.addLatency(lat)
		default:
			atomic.AddInt64(&res.duplicate, 1)
			res.addLatency(lat)
		}
	}
}

// startStamp 让同一次运行的所有 client_message_id 带上运行标识，
// 避免与上一次运行的同名 ID 撞上去重。
var startStamp = time.Now().UnixNano()

func loginAll(cfg config, needed map[int]bool) (map[int]string, error) {
	out := make(map[int]string, len(needed))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	var firstErr atomic.Value

	for idx := range needed {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			// 登录不再复用连接：预热阶段不占用压测的 keep-alive。
			client := bench.NewClient(cfg.timeout)
			cookie, err := bench.Login(client, cfg.base, cfg.prefix, i, cfg.password)
			if err != nil {
				firstErr.CompareAndSwap(nil, fmt.Sprintf("登录 %s_%d: %v",
					cfg.prefix, i, err))
				return
			}
			mu.Lock()
			out[i] = cookie
			mu.Unlock()
		}(idx)
	}
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		return nil, fmt.Errorf("%v", v)
	}
	return out, nil
}

// loadPairs 读取好友对文件，跳过空行与注释。
func loadPairs(path string) ([]pair, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取好友对文件: %w", err)
	}
	var out []pair
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		var p pair
		if _, err := fmt.Sscanf(fields[0], "%d", &p.idx); err != nil {
			continue
		}
		if _, err := fmt.Sscanf(fields[1], "%d", &p.peer); err != nil {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

func report(cfg config, res *result, elapsed time.Duration) {
	sent := atomic.LoadInt64(&res.sent)
	accepted := atomic.LoadInt64(&res.accepted)
	failed := atomic.LoadInt64(&res.failed)

	fmt.Println("\n=== 结果 ===")
	fmt.Printf("  并发连接:     %d\n", cfg.conns)
	fmt.Printf("  压测时长:     %s\n", cfg.duration)
	fmt.Printf("  总发送:       %d\n", sent)
	fmt.Printf("  成功(201):    %d\n", accepted)
	fmt.Printf("  去重命中(200):%d\n", atomic.LoadInt64(&res.duplicate))
	fmt.Printf("  失败:         %d\n", failed)
	if v := res.firstErr.Load(); v != nil {
		fmt.Printf("  首个错误:     %v\n", v)
	}
	if sent > 0 {
		fmt.Printf("  成功率:       %.2f%%\n", float64(accepted)/float64(sent)*100)
	}
	// 实际用时可能被中断提前，按真实墙钟算 QPS。
	real := elapsed.Seconds()
	if real > 0 {
		fmt.Printf("  QPS (发送):   %.0f msg/s\n", float64(sent)/real)
		fmt.Printf("  QPS (成功):   %.0f msg/s\n", float64(accepted)/real)
	}

	res.mu.Lock()
	lat := append([]time.Duration(nil), res.latencies...)
	res.mu.Unlock()
	if len(lat) > 0 {
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		pct := func(p float64) time.Duration {
			i := int(float64(len(lat)) * p)
			if i >= len(lat) {
				i = len(lat) - 1
			}
			return lat[i]
		}
		var sum time.Duration
		for _, d := range lat {
			sum += d
		}
		fmt.Printf("  延迟 P50:     %.2f ms\n", float64(pct(0.50).Microseconds())/1000)
		fmt.Printf("  延迟 P95:     %.2f ms\n", float64(pct(0.95).Microseconds())/1000)
		fmt.Printf("  延迟 P99:     %.2f ms\n", float64(pct(0.99).Microseconds())/1000)
		fmt.Printf("  延迟 AVG:     %.2f ms\n", float64((sum/time.Duration(len(lat))).Microseconds())/1000)
		fmt.Printf("  延迟 MAX:     %.2f ms\n", float64(lat[len(lat)-1].Microseconds())/1000)
	}
}