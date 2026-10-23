package httpapi

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const (
	wsWriteWait  = 10 * time.Second
	wsPongWait   = 60 * time.Second
	wsPingPeriod = 25 * time.Second
	wsSendBuffer = 32

	wsCloseReplaced = 4001
	wsCloseSession  = 4002
)

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     checkWebSocketOrigin,
}

type wsEvent struct {
	Type    string `json:"type"`
	Message any    `json:"message,omitempty"`
}

type wsReadEvent struct {
	Type              string `json:"type"`
	Reader            string `json:"reader"`
	Peer              string `json:"peer"`
	LastReadMessageID uint64 `json:"last_read_message_id"`
}

type wsGroupMessageEvent struct {
	Type    string               `json:"type"`
	Message groupMessageResponse `json:"message"`
}

type wsGroupChangedEvent struct {
	Type    string `json:"type"`
	GroupID uint64 `json:"group_id"`
	Action  string `json:"action"`
}

type wsClient struct {
	userID    uint64
	conn      *websocket.Conn
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

func (c *wsClient) closeWith(code int, reason string) {
	c.closeOnce.Do(func() {
		if code == 0 {
			code = websocket.CloseNormalClosure
		}
		_ = c.conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(code, reason),
			time.Now().Add(time.Second),
		)
		close(c.done)
		_ = c.conn.Close()
	})
}

func (c *wsClient) close() {
	c.closeWith(websocket.CloseNormalClosure, "")
}

func (c *wsClient) enqueue(payload []byte) {
	select {
	case <-c.done:
		return
	default:
	}
	select {
	case c.send <- payload:
	default:
		c.close()
	}
}

func (c *wsClient) writeLoop() {
	ticker := time.NewTicker(wsPingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case payload := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				c.close()
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.close()
				return
			}
		}
	}
}

// Hub manages authenticated WebSocket clients by user ID. Each user has at
// most one active chat connection.
type Hub struct {
	mu      sync.RWMutex
	clients map[uint64]*wsClient
}

func NewHub() *Hub {
	return &Hub{clients: make(map[uint64]*wsClient)}
}

func (h *Hub) add(client *wsClient) {
	h.mu.Lock()
	old := h.clients[client.userID]
	h.clients[client.userID] = client
	h.mu.Unlock()

	if old != nil && old != client {
		old.closeWith(wsCloseReplaced, "replaced")
	}
}

func (h *Hub) remove(client *wsClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[client.userID] == client {
		delete(h.clients, client.userID)
	}
}

// CloseUser closes the active connection for a user with a normal close code.
func (h *Hub) CloseUser(userID uint64) {
	h.CloseUserWith(userID, 0, "")
}

// CloseUserWith closes the active connection for a user with a specific code.
func (h *Hub) CloseUserWith(userID uint64, code int, reason string) {
	h.mu.RLock()
	client := h.clients[userID]
	h.mu.RUnlock()
	if client != nil {
		client.closeWith(code, reason)
	}
}

// Broadcast sends a payload to all active connections for the given users.
func (h *Hub) Broadcast(userIDs []uint64, payload []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	seen := make(map[uint64]struct{}, len(userIDs))
	for _, userID := range userIDs {
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		if client := h.clients[userID]; client != nil {
			client.enqueue(payload)
		}
	}
}

func (h *Hub) activeCount(userID uint64) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.clients[userID] != nil {
		return 1
	}
	return 0
}

func (a *API) websocket(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	a.hub.serve(c, current.ID, currentSessionExpiresAt(c))
}

func (h *Hub) serve(c *gin.Context, userID uint64, expiresAt time.Time) {
	conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("websocket upgrade: %v", err)
		return
	}

	client := &wsClient{
		userID: userID,
		conn:   conn,
		send:   make(chan []byte, wsSendBuffer),
		done:   make(chan struct{}),
	}
	h.add(client)
	defer func() {
		h.remove(client)
		client.close()
	}()

	if !expiresAt.IsZero() {
		remaining := time.Until(expiresAt)
		if remaining <= 0 {
			return
		}
		timer := time.AfterFunc(remaining, func() {
			client.closeWith(wsCloseSession, "session_expired")
		})
		defer timer.Stop()
	}

	go client.writeLoop()

	conn.SetReadLimit(1024)
	_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(wsPongWait))
	})

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func checkWebSocketOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" {
		return false
	}
	if strings.EqualFold(parsed.Host, r.Host) {
		return true
	}
	return isLoopbackHost(parsed.Hostname()) && isLoopbackHost(requestHostname(r.Host))
}

func requestHostname(hostport string) string {
	host, _, err := net.SplitHostPort(hostport)
	if err == nil {
		return host
	}
	return hostport
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// PublishMessage sends a saved message event to both participants.
func (a *API) PublishMessage(senderID, recipientID uint64, message any) {
	payload, err := json.Marshal(wsEvent{
		Type:    "message.created",
		Message: message,
	})
	if err != nil {
		log.Printf("marshal websocket event: %v", err)
		return
	}
	a.hub.Broadcast([]uint64{senderID, recipientID}, payload)
}

// PublishGroupMessage sends a saved group message event to all members.
func (a *API) PublishGroupMessage(memberIDs []uint64, message groupMessageResponse) {
	payload, err := json.Marshal(wsGroupMessageEvent{
		Type:    "group.message.created",
		Message: message,
	})
	if err != nil {
		log.Printf("marshal group message event: %v", err)
		return
	}
	a.hub.Broadcast(memberIDs, payload)
}

// PublishGroupChanged tells affected users to refresh a group.
func (a *API) PublishGroupChanged(memberIDs []uint64, groupID uint64, action string) {
	payload, err := json.Marshal(wsGroupChangedEvent{
		Type:    "group.changed",
		GroupID: groupID,
		Action:  action,
	})
	if err != nil {
		log.Printf("marshal group changed event: %v", err)
		return
	}
	a.hub.Broadcast(memberIDs, payload)
}

// PublishRead sends an updated read position to both participants.
func (a *API) PublishRead(
	readerID, peerID uint64,
	reader, peer string,
	lastReadMessageID uint64,
) {
	payload, err := json.Marshal(wsReadEvent{
		Type:              "messages.read",
		Reader:            reader,
		Peer:              peer,
		LastReadMessageID: lastReadMessageID,
	})
	if err != nil {
		log.Printf("marshal websocket read event: %v", err)
		return
	}
	a.hub.Broadcast([]uint64{readerID, peerID}, payload)
}
