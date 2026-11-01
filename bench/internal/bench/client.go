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
//
// 这只适用于长连接类压测（建连本身就是被测对象）。QPS 类压测要测的是消息
// 吞吐而不是 TCP 握手，必须用 NewKeepAliveClient，否则会严重低估。
func NewClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:   true,
			MaxIdleConnsPerHost: 1,
		},
		Timeout: timeout,
	}
}

// NewKeepAliveClient 返回一个复用连接的 HTTP 客户端，用于吞吐（QPS）压测。
//
// 每个 goroutine 持有自己的 client，于是各自拥有一条到服务端的 keep-alive
// 连接：请求之间只付一次 TCP 握手，测到的才是消息处理吞吐。MaxIdleConns
// 调到并发量级，避免连接被提前回收。
func NewKeepAliveClient(timeout time.Duration, maxConns int) *http.Client {
	if maxConns < 1 {
		maxConns = 1
	}
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        maxConns,
			MaxIdleConnsPerHost: maxConns,
			MaxConnsPerHost:     maxConns,
			IdleConnTimeout:     5 * time.Minute,
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

// doJSON 发一个带 cookie 的 JSON 请求，返回状态码与响应体。
// 调用方负责解读状态码；只有传输层错误才返回 err。
func doJSON(
	client *http.Client, method, url, cookie string, payload any,
) (int, []byte, error) {
	var reader io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return 0, nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if cookie != "" {
		req.Header.Set("Cookie", cookie)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

// SendFriendRequest 让 cookie 对应的用户向 toUsername 发起好友申请。
//
// 服务端在 201 响应里直接给出 request_id（对端执行接受时要用），不需要再查
// 一次列表。若双方此前已有反向的待处理申请，服务端会直接建立好友关系并返回
// 200 + status=accepted，此时 requestID 为 0 —— 调用方据此判断是否需要接受。
func SendFriendRequest(
	client *http.Client, base, cookie, toUsername string,
) (requestID uint64, accepted bool, err error) {
	status, body, err := doJSON(client, http.MethodPost, base+"/api/friend-requests",
		cookie, map[string]string{"username": toUsername})
	if err != nil {
		return 0, false, err
	}
	switch status {
	case http.StatusOK: // 反向申请已存在，直接成为好友
		return 0, true, nil
	case http.StatusCreated:
		var payload struct {
			Status    string `json:"status"`
			RequestID uint64 `json:"request_id"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return 0, false, fmt.Errorf("解析好友申请响应: %w", err)
		}
		return payload.RequestID, false, nil
	default:
		return 0, false, &HTTPError{Op: "发起好友申请", Status: status}
	}
}

// AcceptFriendRequest 接受一条好友申请。
func AcceptFriendRequest(client *http.Client, base, cookie string, requestID uint64) error {
	url := fmt.Sprintf("%s/api/friend-requests/%d/accept", base, requestID)
	status, _, err := doJSON(client, http.MethodPost, url, cookie, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return &HTTPError{Op: "接受好友申请", Status: status}
	}
	return nil
}

// AreFriends 报告 cookie 对应的用户与 username 是否已是好友。
//
// 并发配对时服务端事务可能死锁回滚（friend_requests 索引末端间隙锁），表现为
// 500；但同一事务里可能已经建好关系。用它做幂等校验，避免把"其实成功了"当成
// 失败、重复发起申请。
func AreFriends(client *http.Client, base, cookie, username string) (bool, error) {
	status, body, err := doJSON(client, http.MethodGet, base+"/api/friends", cookie, nil)
	if err != nil {
		return false, err
	}
	if status != http.StatusOK {
		return false, &HTTPError{Op: "查询好友列表", Status: status}
	}
	var payload struct {
		Friends []struct {
			Username string `json:"username"`
		} `json:"friends"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return false, fmt.Errorf("解析好友列表: %w", err)
	}
	for _, f := range payload.Friends {
		if f.Username == username {
			return true, nil
		}
	}
	return false, nil
}

// PendingRequestID 查找 cookie 用户发给 toUsername 的待处理好友申请编号。
// 没有待处理申请时返回 0。
func PendingRequestID(client *http.Client, base, cookie, toUsername string) (uint64, error) {
	status, body, err := doJSON(client, http.MethodGet, base+"/api/friend-requests", cookie, nil)
	if err != nil {
		return 0, err
	}
	if status != http.StatusOK {
		return 0, &HTTPError{Op: "查询好友申请", Status: status}
	}
	var payload struct {
		Outgoing []struct {
			ID   uint64 `json:"id"`
			User struct {
				Username string `json:"username"`
			} `json:"user"`
		} `json:"outgoing"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, fmt.Errorf("解析好友申请列表: %w", err)
	}
	for _, r := range payload.Outgoing {
		if r.User.Username == toUsername {
			return r.ID, nil
		}
	}
	return 0, nil
}

// SendMessage 发一条私聊消息，返回服务端受理后的会话内序号。
//
// 重要：clientMessageID 必须在整个压测中唯一。服务端按它去重——同 ID 同内容
// 算 duplicate（不新建，返回 200），同 ID 不同内容算 conflict（409）。复用 ID
// 会让吞吐数字虚高，因为大量请求走了去重分支而没有真正建消息。
//
// 返回的 created 表示本次是否真的新建了消息（false 即命中去重）。
func SendMessage(
	client *http.Client, base, cookie, toUsername, clientMessageID, body string,
) (seq uint64, created bool, err error) {
	url := fmt.Sprintf("%s/api/conversations/%s/messages", base, toUsername)
	status, respBody, err := doJSON(client, http.MethodPost, url, cookie, map[string]string{
		"body":              body,
		"client_message_id": clientMessageID,
	})
	if err != nil {
		return 0, false, err
	}
	switch status {
	case http.StatusCreated, http.StatusOK:
		var payload struct {
			Message struct {
				Seq uint64 `json:"seq"`
			} `json:"message"`
		}
		if err := json.Unmarshal(respBody, &payload); err != nil {
			return 0, false, fmt.Errorf("解析发送消息响应: %w", err)
		}
		return payload.Message.Seq, status == http.StatusCreated, nil
	default:
		return 0, false, &HTTPError{Op: "发送消息", Status: status}
	}
}