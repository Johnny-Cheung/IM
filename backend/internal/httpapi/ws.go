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

type wsClient struct {
	userID    uint64
	conn      *websocket.Conn
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

func (c *wsClient) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
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

// Hub manages authenticated WebSocket clients by user ID.
type Hub struct {
	mu      sync.RWMutex
	clients map[uint64]map[*wsClient]struct{}
}

func NewHub() *Hub {
	return &Hub{clients: make(map[uint64]map[*wsClient]struct{})}
}

func (h *Hub) add(client *wsClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[client.userID] == nil {
		h.clients[client.userID] = make(map[*wsClient]struct{})
	}
	h.clients[client.userID][client] = struct{}{}
}

func (h *Hub) remove(client *wsClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	clients := h.clients[client.userID]
	delete(clients, client)
	if len(clients) == 0 {
		delete(h.clients, client.userID)
	}
}

// CloseUser closes all active connections for a user.
func (h *Hub) CloseUser(userID uint64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for client := range h.clients[userID] {
		client.close()
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
		for client := range h.clients[userID] {
			client.enqueue(payload)
		}
	}
}

func (h *Hub) activeCount(userID uint64) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients[userID])
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
		timer := time.AfterFunc(remaining, client.close)
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
