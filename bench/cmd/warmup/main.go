// warmup 批量预登录并把 session cookie 写入文件。
//
// 用法:
//
//	go run ./cmd/warmup -count 200000 -out /tmp/im-cookies.txt
//
// 压测工具可读这个文件，建连阶段就不必再发登录请求，于是没有 bcrypt 校验的
// CPU 风暴，"最大连接数"才反映服务端维持连接的能力，而不是登录路径的吞吐。
// 实测单独登录一次约 53ms（bcrypt cost 10）；高并发下逐登录会把这段延迟叠加
// 到建连上——早先用 k6 跑到 6 万连接时 ws_connecting 的 p95 劣化到 8.24 秒。
//
// 重要：每次登录都会撤销该账号原有的活动会话。预热之后若再对同一批账号登录
// （包括跑压测工具的 -prelogin、或再次运行本命令），先前文件里的 cookie 就
// 失效了。所以预热完请直接用它跑压测。
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Johnny-Cheung/IM/bench/internal/bench"
)

type entry struct {
	index  int
	cookie string
}

func main() {
	var (
		base        = flag.String("base", "http://127.0.0.1:8080", "服务端基地址")
		count       = flag.Int("count", 40000, "登录数量")
		start       = flag.Int("start", 0, "起始编号")
		prefix      = flag.String("prefix", "bench", "账号前缀")
		password    = flag.String("password", bench.DefaultPassword, "登录口令")
		concurrency = flag.Int("concurrency", 50, "并发数")
		out         = flag.String("out", "/tmp/im-cookies.txt", "输出文件")
	)
	flag.Parse()

	if *count <= 0 {
		log.Fatalf("warmup: -count 必须为正数")
	}

	client := bench.NewClient(30 * time.Second)
	entries := make([]entry, 0, *count)
	var mu sync.Mutex
	var (
		failed  int64
		done    int64
		next    int64 = int64(*start)
		end           = int64(*start + *count)
		started       = time.Now()
	)

	fmt.Printf("预热登录 %d 个账号 (编号 %d..%d)，并发 %d\n",
		*count, *start, *start+*count-1, *concurrency)

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx := int(atomic.AddInt64(&next, 1) - 1)
				if idx >= int(end) {
					return
				}
				cookie, err := bench.Login(client, *base, *prefix, idx, *password)
				if err != nil {
					if n := atomic.AddInt64(&failed, 1); n <= 10 {
						fmt.Fprintf(os.Stderr, "  登录 %s 失败: %v\n",
							bench.Username(*prefix, idx), err)
					}
				} else {
					mu.Lock()
					entries = append(entries, entry{index: idx, cookie: cookie})
					mu.Unlock()
				}
				if n := atomic.AddInt64(&done, 1); n%2000 == 0 {
					fmt.Fprintf(os.Stderr, "  %d/%d (%.0f%%) 失败=%d\n",
						n, *count, float64(n)/float64(*count)*100, atomic.LoadInt64(&failed))
				}
			}
		}()
	}
	wg.Wait()

	// 按序号排序，让读取方可以顺序索引。
	sort.Slice(entries, func(i, j int) bool { return entries[i].index < entries[j].index })

	f, err := os.OpenFile(*out, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		log.Fatalf("warmup: 创建输出文件: %v", err)
	}
	w := bufio.NewWriter(f)
	for _, e := range entries {
		fmt.Fprintf(w, "%d\t%s\n", e.index, e.cookie)
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		log.Fatalf("warmup: 写输出文件: %v", err)
	}
	if err := f.Close(); err != nil {
		log.Fatalf("warmup: 关闭输出文件: %v", err)
	}

	fmt.Printf("完成: 成功 %d, 失败 %d, 耗时 %s\n",
		len(entries), failed, time.Since(started).Round(time.Second))
	fmt.Printf("cookie 文件: %s\n", *out)
	if failed > 0 {
		os.Exit(1)
	}
}