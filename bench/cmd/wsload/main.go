// wsload 是 WebSocket 长连接压测客户端。
//
// 为什么不用 k6：k6 的每个 VU 都要独立的 JS 运行时，实测约 213 KiB/连接
// （即便 GOGC=20），6 万连接需要 12 GB 以上内存。Go 的 goroutine 每连接
// 只需几 KiB，同样的规模内存需求低一个数量级，而且能精确控制源 IP 绑定、
// 连接分布和 ping/pong 处理。
//
// 用法：
//
//	go run ./cmd/wsload -count 60000 -cookies /tmp/im-ck60k.txt \
//	  -src-ips 127.0.0.1,127.0.0.2,127.0.0.3 -ramp 120s -hold 60s
//
// （以下命令都在 bench/ 目录内执行）
//
// 连接按序号轮询分配到各个源 IP，每个源 IP 有独立的临时端口空间，因此
// 单机可建立远超 64,512（单 IP 端口上限）的连接。
//
// cookie 有三种来源，优先级从高到低：
//  1. -cookies 指定的预热文件（会被自动过滤掉失效条目）
//  2. -prelogin：在压测前单独并发登录完所有账号，cookie 只留在内存里
//  3. 逐个登录（最慢，登录开销混在压测窗口内，不推荐用于大连接数）
//
// 重要：任何一次"重新登录同一账号"都会作废该账号之前的 cookie——服务端
// CreateSession 会撤销旧会话。所以预热文件在后续跑过登录类测试后就会失效。
// -prelogin 没有这个外部依赖，是避免该问题最稳的方式。
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Johnny-Cheung/IM/bench/internal/bench"
	"github.com/gorilla/websocket"
)

type config struct {
	base       string
	count      int
	cookieFile string
	srcIPs     []string
	ramp       time.Duration
	hold       time.Duration
	password   string
	prefix     string
	accounts   int
	prelogin   bool
	preloginN  int
	rate       int
	retries    int
}

func main() {
	var (
		base     = flag.String("base", "http://127.0.0.1:8080", "服务端基地址")
		count    = flag.Int("count", 1000, "要建立的连接数")
		cookies  = flag.String("cookies", "", "预热的 cookie 文件（序号<TAB>Cookie 头）；为空则用 -prelogin 或逐个登录")
		srcIPs   = flag.String("src-ips", "", "源 IP 列表，逗号分隔；为空则用默认路由")
		ramp     = flag.Duration("ramp", 60*time.Second, "建连错开窗口")
		hold     = flag.Duration("hold", 60*time.Second, "全部连上后统一保持的时长")
		password = flag.String("password", bench.DefaultPassword, "登录口令")
		prefix   = flag.String("prefix", "bench", "账号前缀")
		prelogin = flag.Bool("prelogin", false, "压测前先并发登录所有账号，cookie 只留在内存（无外部文件依赖）")
		rate     = flag.Int("rate", 0, "每秒建连数上限；0 表示按 ramp 窗口均摊")
		retries  = flag.Int("retries", 3, "拨号失败的重试次数")
	)
	flag.Parse()

	src := []string{}
	if strings.TrimSpace(*srcIPs) != "" {
		for _, s := range strings.Split(*srcIPs, ",") {
			if s = strings.TrimSpace(s); s != "" {
				src = append(src, s)
			}
		}
	}

	cfg := config{
		base: *base, count: *count, cookieFile: *cookies,
		srcIPs: src, ramp: *ramp, hold: *hold,
		password: *password, prefix: *prefix, accounts: *count,
		prelogin: *prelogin, preloginN: *count,
		rate: *rate, retries: *retries,
	}

	if err := run(cfg); err != nil {
		log.Fatalf("wsload: %v", err)
	}
}

type result struct {
	ok          int64
	loginFailed int64
	dialFailed  int64
	handshakeNo int64
	brokenPipe  int64
	otherErr    int64

	mu          sync.Mutex
	dialLatency []time.Duration
}

func (r *result) addLatency(d time.Duration) {
	r.mu.Lock()
	r.dialLatency = append(r.dialLatency, d)
	r.mu.Unlock()
}

func run(cfg config) error {
	cookies, err := loadCookies(cfg.cookieFile, cfg.count)
	if err != nil {
		return err
	}
	if cfg.cookieFile != "" && len(cookies) == 0 {
		return fmt.Errorf("cookie 文件 %s 没有可用条目", cfg.cookieFile)
	}

	// cookie 来源：预热文件 > -prelogin > 逐个登录。
	if len(cookies) > 0 {
		// 预热文件里的 cookie 可能已被后续登录作废（CreateSession 会撤销旧会话）。
		cookies, err = filterValidCookies(cfg, cookies)
		if err != nil {
			return err
		}
		fmt.Printf("  有效 cookie: %d 条\n", len(cookies))
		if len(cookies) == 0 {
			return fmt.Errorf("没有任何有效的 cookie，请重新运行 bench/cmd/warmup")
		}
	} else if cfg.prelogin {
		cookies, err = preloginCookies(cfg)
		if err != nil {
			return err
		}
		fmt.Printf("  预登录完成: %d 条 cookie\n", len(cookies))
	}

	res := &result{dialLatency: make([]time.Duration, 0, cfg.count)}
	total := cfg.ramp + cfg.hold
	// 全体共用的绝对截止时刻，保证爬升结束后有一段"全部连接同时在线"的窗口。
	deadline := time.Now().Add(total)

	fmt.Printf("wsload: count=%d srcIPs=%v ramp=%s hold=%s total=%s\n",
		cfg.count, cfg.srcIPs, cfg.ramp, cfg.hold, total)
	if len(cookies) > 0 {
		fmt.Printf("  cookie: %s (%d 条)\n", cfg.cookieFile, len(cookies))
	} else {
		fmt.Printf("  无 cookie 文件，将逐个登录\n")
	}

	// 建连调度：所有 goroutine 同时启动，但拨号通过全局限速器控制速率，
	// 每个连接按序号错开在自己的时间片上拨号。
	//
	// 不能用"计算 stagger 后 sleep"的老办法：那样所有 goroutine 在同一瞬间
	// 创建，最后一批会因调度延迟集中唤醒，在窗口末尾形成 SYN 风暴——实测
	// 20 万连接时最后 5 秒内集中爆发 5,934 个拨号失败，而此前的失败数一直是 0。
	// 限速器把建连速率钉在固定间隔上，建连压力才是平滑的。
	limiter := time.NewTicker(rateInterval(cfg))
	defer limiter.Stop()

	var wg sync.WaitGroup
	wg.Add(cfg.count)
	start := time.Now()

	for i := 0; i < cfg.count; i++ {
		go func(idx int) {
			defer wg.Done()
			// 每个连接等自己的时间片，由全局 ticker 逐个放行。
			if idx > 0 {
				<-limiter.C
			}
			connect(cfg, cookies, idx, deadline, res)
		}(i)
	}

	// 定期打印进度，便于观察爬升与保持。
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				fmt.Printf("  [%s] 已建连=%d 失败: dial=%d handshake=%d 登录=%d\n",
					time.Since(start).Round(time.Second),
					atomic.LoadInt64(&res.ok),
					atomic.LoadInt64(&res.dialFailed),
					atomic.LoadInt64(&res.handshakeNo),
					atomic.LoadInt64(&res.loginFailed))
			}
		}
	}()

	// 支持 Ctrl-C 提前收尾。
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("\n收到中断，等待连接退出…")
	}()

	wg.Wait()
	close(done)

	report(cfg, res, time.Since(start))
	if atomic.LoadInt64(&res.loginFailed)+atomic.LoadInt64(&res.dialFailed)+
		atomic.LoadInt64(&res.handshakeNo) > 0 {
		return fmt.Errorf("存在失败连接，详见上方统计")
	}
	return nil
}

// filterValidCookies 并发探测哪些 cookie 仍有效（GET /api/me 返回 200），
// 按原顺序返回有效项。避开因后续登录而被撤销的会话，否则整批连接会 401。
func filterValidCookies(cfg config, cookies []string) ([]string, error) {
	const probeConcurrency = 200
	valid := make([]bool, len(cookies))
	var wg sync.WaitGroup
	sem := make(chan struct{}, probeConcurrency)
	client := bench.NewClient(20 * time.Second)
	for i := range cookies {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			valid[idx] = bench.VerifyCookie(client, cfg.base, cookies[idx])
		}(i)
	}
	wg.Wait()

	out := make([]string, 0, len(cookies))
	for i, ok := range valid {
		if ok {
			out = append(out, cookies[i])
		}
	}
	return out, nil
}

// preloginCookies 在压测前并发登录 count 个账号，cookie 只保留在内存。
// 与预热文件相比没有外部依赖，也就不会被后续测试作废；与"逐个登录"相比
// 把 bcrypt 开销全部挪到压测窗口之前，建连阶段零 HTTP 请求。
func preloginCookies(cfg config) ([]string, error) {
	const concurrency = 100
	out := make([]string, cfg.preloginN)
	var failed int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	started := time.Now()
	fmt.Printf("  预登录 %d 个账号（并发 %d）…\n", cfg.preloginN, concurrency)

	for i := 0; i < cfg.preloginN; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			if idx%5000 == 0 && idx > 0 {
				fmt.Printf("    已登录 %d/%d (%.0f%%)\n", idx, cfg.preloginN,
					float64(idx)/float64(cfg.preloginN)*100)
			}
			c, err := login(cfg, idx)
			if err != nil {
				atomic.AddInt64(&failed, 1)
				return
			}
			out[idx] = c
		}(i)
	}
	wg.Wait()

	valid := make([]string, 0, cfg.preloginN)
	for _, c := range out {
		if c != "" {
			valid = append(valid, c)
		}
	}
	fmt.Printf("  预登录用时 %s，失败 %d 个\n", time.Since(started).Round(time.Second), failed)
	if len(valid) == 0 {
		return nil, fmt.Errorf("预登录全部失败")
	}
	return valid, nil
}

// dialWebSocket 拨号并握手，遇到超时/连接被拒时退避重试。
// 瞬时资源紧张（本机同时跑客户端与服务端）会让少量拨号在几秒内失败，
// 短暂退避后重试即可成功；429/4xx 这类应用层拒绝不重试。
func dialWebSocket(
	dialer *websocket.Dialer, url string, header http.Header, cfg config,
) (*websocket.Conn, *http.Response, error) {
	var (
		conn *websocket.Conn
		resp *http.Response
		err  error
	)
	backoff := 50 * time.Millisecond
	for attempt := 0; attempt <= cfg.retries; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
		}
		conn, resp, err = dialer.Dial(url, header)
		if err == nil {
			return conn, resp, nil
		}
		// 应用层已给出 HTTP 响应（如 401）属于确定性失败，重试无意义。
		if resp != nil && resp.StatusCode >= 400 {
			return conn, resp, err
		}
	}
	return conn, resp, err
}

// rateInterval 由 ramp 窗口和连接数算出每次放行的间隔，使全部连接恰好
// 在 ramp 内建完。间隔有下限，避免极端参数下 ticker 过于密集。
func rateInterval(cfg config) time.Duration {
	if cfg.rate > 0 {
		return time.Second / time.Duration(cfg.rate)
	}
	interval := cfg.ramp / time.Duration(cfg.count)
	if interval < time.Microsecond {
		interval = time.Microsecond
	}
	return interval
}

// connect 建立一条连接并保持到统一的截止时刻。
func connect(cfg config, cookies []string, idx int, deadline time.Time, res *result) {
	// 源 IP 按序号轮询分配。
	dialer := &websocket.Dialer{HandshakeTimeout: 30 * time.Second}
	if len(cfg.srcIPs) > 0 {
		localIP := net.ParseIP(cfg.srcIPs[idx%len(cfg.srcIPs)])
		dialer.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			d := &net.Dialer{LocalAddr: &net.TCPAddr{IP: localIP}, Timeout: 30 * time.Second}
			return d.DialContext(ctx, network, addr)
		}
	}

	cookieHeader := ""
	if len(cookies) > 0 {
		cookieHeader = cookies[idx%len(cookies)]
	} else {
		c, err := login(cfg, idx)
		if err != nil {
			atomic.AddInt64(&res.loginFailed, 1)
			return
		}
		cookieHeader = c
	}

	wsURL := "ws" + strings.TrimPrefix(cfg.base, "http") + "/api/ws"
	header := http.Header{}
	if cookieHeader != "" {
		header.Set("Cookie", cookieHeader)
	}

	dialStart := time.Now()
	conn, resp, err := dialWebSocket(dialer, wsURL, header, cfg)
	res.addLatency(time.Since(dialStart))
	if err != nil {
		if resp != nil && resp.StatusCode >= 400 {
			atomic.AddInt64(&res.handshakeNo, 1)
		} else {
			atomic.AddInt64(&res.dialFailed, 1)
		}
		return
	}
	defer conn.Close()

	// 读循环：必须持续读，否则收不到服务端的 ping、也感知不到断开。
	// 服务端每 25s 发 ping 并要求 60s 内回 pong，gorilla 的默认 ping handler
	// 会自动回 pong，这里只需保证读循环一直在跑。
	var readerDone sync.WaitGroup
	readerDone.Add(1)
	go func() {
		defer readerDone.Done()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				if isBrokenPipe(err) {
					atomic.AddInt64(&res.brokenPipe, 1)
				}
				return
			}
		}
	}()

	atomic.AddInt64(&res.ok, 1)

	// 保持到统一截止时刻。
	if wait := time.Until(deadline); wait > 0 {
		time.Sleep(wait)
	}
	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(5*time.Second))
	conn.Close()
	readerDone.Wait()
}

func isBrokenPipe(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "broken pipe") || strings.Contains(s, "connection reset")
}

// login 走 HTTP 登录拿 session cookie，委托给共用库。
func login(cfg config, idx int) (string, error) {
	return bench.Login(bench.NewClient(30*time.Second), cfg.base, cfg.prefix, idx, cfg.password)
}

// loadCookies 读取预热文件，返回 cookie 头列表（序号升序）。
func loadCookies(path string, count int) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("读取 cookie 文件: %w", err)
	}
	defer f.Close()

	byIndex := make(map[int]string, count)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			continue
		}
		byIndex[n] = strings.TrimSpace(parts[1])
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	indices := make([]int, 0, len(byIndex))
	for n := range byIndex {
		indices = append(indices, n)
	}
	sort.Ints(indices)
	out := make([]string, 0, len(indices))
	for _, n := range indices {
		out = append(out, byIndex[n])
	}
	return out, nil
}

func report(cfg config, res *result, elapsed time.Duration) {
	fmt.Println("\n=== 结果 ===")
	fmt.Printf("  目标连接数:   %d\n", cfg.count)
	fmt.Printf("  成功建连:     %d\n", atomic.LoadInt64(&res.ok))
	fmt.Printf("  登录失败:     %d\n", atomic.LoadInt64(&res.loginFailed))
	fmt.Printf("  TCP 拨号失败: %d\n", atomic.LoadInt64(&res.dialFailed))
	fmt.Printf("  握手被拒:     %d\n", atomic.LoadInt64(&res.handshakeNo))
	fmt.Printf("  连接中断:     %d\n", atomic.LoadInt64(&res.brokenPipe))
	fmt.Printf("  其他错误:     %d\n", atomic.LoadInt64(&res.otherErr))
	fmt.Printf("  用时:         %s\n", elapsed.Round(time.Second))

	res.mu.Lock()
	lat := append([]time.Duration(nil), res.dialLatency...)
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
		fmt.Printf("  建连延迟:     p50=%s p95=%s p99=%s max=%s\n",
			pct(0.50).Round(time.Microsecond), pct(0.95).Round(time.Microsecond),
			pct(0.99).Round(time.Microsecond), lat[len(lat)-1].Round(time.Microsecond))
	}
}