// Package bench 提供压测脚本共用的 HTTP 与会话处理。
//
// 所有压测工具都通过 session cookie 鉴权，且都要处理同一个陷阱：服务端的
// CreateSession 会撤销该账号原有活动会话，所以任何一次重新登录都会让此前
// 拿到的 cookie 失效。共用的登录逻辑集中在这里，避免各脚本各自踩坑。
package bench

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPError 表示服务端返回了非成功状态码。
//
// 调用方需要区分两类失败：4xx 是确定性拒绝（用户名非法、口令不合规），重试
// 没有意义；5xx 可能只是这一次请求没成，值得重试——注册路径尤其如此，见
// Register 的说明。
type HTTPError struct {
	Op     string // 操作名，用于报错文案
	Status int
}

func (e *HTTPError) Error() string { return fmt.Sprintf("%s返回 %d", e.Op, e.Status) }

// IsServerError 报告 err 是否为 5xx，即值得重试的那一类。
func IsServerError(err error) bool {
	var httpErr *HTTPError
	return errors.As(err, &httpErr) && httpErr.Status >= 500
}

// IsClientError 报告 err 是否为 4xx，即确定性拒绝，不应重试。
func IsClientError(err error) bool {
	var httpErr *HTTPError
	return errors.As(err, &httpErr) && httpErr.Status < 500
}

// DefaultPassword 是所有压测账号共用的口令。
const DefaultPassword = "bench-password-123456"

// SessionCookieName 是服务端下发的会话 cookie 名。
const SessionCookieName = "im_session"

// Username 拼出第 index 个压测账号的用户名。
func Username(prefix string, index int) string {
	return fmt.Sprintf("%s_%d", prefix, index)
}

// Login 登录一个账号并返回可用于 Cookie 头的 "im_session=..." 字符串。
//
// 每次调用都会撤销该账号原有的活动会话，因此重复登录同一账号会让旧 cookie
// 失效。压测前应当只登录一次并缓存结果。
func Login(client *http.Client, base, prefix string, index int, password string) (string, error) {
	body, err := json.Marshal(map[string]string{
		"username": Username(prefix, index),
		"password": password,
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest(http.MethodPost, base+"/api/auth/login", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return "", &HTTPError{Op: "登录", Status: resp.StatusCode}
	}
	for _, c := range resp.Cookies() {
		if c.Name == SessionCookieName {
			return SessionCookieName + "=" + c.Value, nil
		}
	}
	return "", fmt.Errorf("登录未返回 %s", SessionCookieName)
}

// Register 注册一个账号。用户名已存在时返回 ErrUsernameTaken。
//
// 重要：注册成功但返回 500 是可能的。服务端先提交 users 行，再在事务里建会话
// （api.go 的 register → CreateUser 然后 startSession）。如果建会话那一步失败
// ——并发注册全新用户时 InnoDB 会因 sessions 索引末端间隙锁互等而死锁并回滚
// 该事务——users 行已经落库了，接口却报 500。所以收到 5xx 时不能断定"没注册
// 上"，应当在重试前用 Login 探一次，见 cmd/register。
func Register(client *http.Client, base, prefix string, index int, password, nickname string) error {
	body, err := json.Marshal(map[string]string{
		"username": Username(prefix, index),
		"nickname": nickname,
		"password": password,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, base+"/api/auth/register", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode == http.StatusConflict:
		return ErrUsernameTaken
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	default:
		return &HTTPError{Op: "注册", Status: resp.StatusCode}
	}
}

// ErrUsernameTaken 表示注册时用户名已被占用。
var ErrUsernameTaken = fmt.Errorf("username taken")

// NewClient 返回一个禁用 keep-alive 的 HTTP 客户端。
//
// 压测本身要占用大量临时端口，登录/注册的连接如果不能立即释放，会和压测
// 连接争抢同一个端口池。禁用复用让每个请求用完即关。
func NewClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:   true,
			MaxIdleConnsPerHost: 1,
		},
		Timeout: timeout,
	}
}

// VerifyCookie 检查一个 cookie 是否仍然有效（GET /api/me 返回 200）。
func VerifyCookie(client *http.Client, base, cookie string) bool {
	req, err := http.NewRequest(http.MethodGet, base+"/api/me", nil)
	if err != nil {
		return false
	}
	req.Header.Set("Cookie", cookie)
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	return resp.StatusCode == http.StatusOK
}