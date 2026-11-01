package main

import (
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Johnny-Cheung/IM/bench/internal/bench"
)

// newServer 起一个假的 auth 服务：register 按脚本返回状态码，login 按账号是否
// "已建"返回 200 或 401。用来确定性地走通 registerOne 的每条分支。
func newServer(t *testing.T, register func(call int64) int, login func() int) *httptest.Server {
	t.Helper()
	var calls int64
	mux := http.NewServeMux()
	mux.HandleFunc("/api/auth/register", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&calls, 1)
		w.WriteHeader(register(n))
	})
	mux.HandleFunc("/api/auth/login", func(w http.ResponseWriter, r *http.Request) {
		code := login()
		if code == http.StatusOK {
			http.SetCookie(w, &http.Cookie{Name: bench.SessionCookieName, Value: "fake"})
		}
		w.WriteHeader(code)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newRNG() *rand.Rand { return rand.New(rand.NewPCG(1, 2)) }

// 注册一次成功 → registered。
func TestRegisterOneSuccess(t *testing.T) {
	srv := newServer(t, func(int64) int { return http.StatusCreated },
		func() int { return http.StatusOK })
	got, err := registerOne(bench.NewClient(5*time.Second), srv.URL, "bench", 1, "pw", newRNG())
	if err != nil || got != registered {
		t.Fatalf("got (%v, %v)，想要 (registered, nil)", got, err)
	}
}

// 账号已存在 → preexisting。
func TestRegisterOneUsernameTaken(t *testing.T) {
	srv := newServer(t, func(int64) int { return http.StatusConflict },
		func() int { return http.StatusUnauthorized })
	got, err := registerOne(bench.NewClient(5*time.Second), srv.URL, "bench", 1, "pw", newRNG())
	if err != nil || got != preexisting {
		t.Fatalf("got (%v, %v)，想要 (preexisting, nil)", got, err)
	}
}

// 4xx 是确定性拒绝：立即返回，不应重试。
func TestRegisterOneClientErrorNoRetry(t *testing.T) {
	var calls int64
	srv := newServer(t,
		func(n int64) int { calls = n; return http.StatusBadRequest },
		func() int { return http.StatusUnauthorized })
	got, err := registerOne(bench.NewClient(5*time.Second), srv.URL, "bench", 1, "pw", newRNG())
	if got != registerFailed || err == nil {
		t.Fatalf("got (%v, %v)，想要 (registerFailed, err)", got, err)
	}
	if calls != 1 {
		t.Fatalf("4xx 重试了 %d 次，应当只请求一次", calls)
	}
}

// 关键路径：注册返回 500（建会话死锁被回滚），但账号其实已建 —— 登录探测成功，
// 应当计入 recoveredFrom500 而不是失败。
func TestRegisterOneRecoversFrom500WhenAccountExists(t *testing.T) {
	var calls int64
	srv := newServer(t,
		func(n int64) int { calls = n; return http.StatusInternalServerError },
		func() int { return http.StatusOK }) // 账号已建
	got, err := registerOne(bench.NewClient(5*time.Second), srv.URL, "bench", 1, "pw", newRNG())
	if err != nil || got != recoveredFrom500 {
		t.Fatalf("got (%v, %v)，想要 (recoveredFrom500, nil)", got, err)
	}
	if calls != 1 {
		t.Fatalf("探测到账号已建后不应再重试注册，实际请求 %d 次", calls)
	}
}

// 500 且登录探测也失败 → 真正失败，但必须重试到 maxAttempts。
func TestRegisterOneRetriesExhausted(t *testing.T) {
	var calls int64
	srv := newServer(t,
		func(n int64) int { calls = n; return http.StatusInternalServerError },
		func() int { return http.StatusUnauthorized })
	got, err := registerOne(bench.NewClient(5*time.Second), srv.URL, "bench", 1, "pw", newRNG())
	if got != registerFailed || err == nil {
		t.Fatalf("got (%v, %v)，想要 (registerFailed, err)", got, err)
	}
	if calls != maxAttempts {
		t.Fatalf("重试次数 %d，想要 %d", calls, maxAttempts)
	}
}

// 抖动必须落在 [0, window) 内，且不同随机源不应总给出同一个值。
func TestSleepRandomBoundedAndVaries(t *testing.T) {
	const window = 50 * time.Millisecond
	first := time.Duration(-1)
	sawDifferent := false
	for i := 0; i < 200; i++ {
		start := time.Now()
		sleepRandom(newRNG(), window)
		d := time.Since(start)
		if d >= window {
			t.Fatalf("抖动 %v 超出窗口 %v", d, window)
		}
		if first < 0 {
			first = d
		} else if d != first {
			sawDifferent = true
		}
	}
	if !sawDifferent {
		t.Fatal("抖动总是相同，说明没有随机化")
	}
}

// window<=0 时不应睡（避免极端参数下无谓等待）。
func TestSleepRandomNoWaitWhenWindowNonPositive(t *testing.T) {
	start := time.Now()
	sleepRandom(newRNG(), 0)
	if d := time.Since(start); d > time.Millisecond {
		t.Fatalf("window=0 时睡了 %v，应当立即返回", d)
	}
}