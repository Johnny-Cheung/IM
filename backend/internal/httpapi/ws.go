package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/auth"
	"github.com/Johnny-Cheung/IM/backend/internal/messaging"
	"github.com/Johnny-Cheung/IM/backend/internal/store"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const (
	wsWriteWait  = 10 * time.Second
	wsPongWait   = 60 * time.Second
	wsPingPeriod = 25 * time.Second
	wsSendBuffer = 32

	wsFrameRatePerSecond = 5.0
	wsFrameBurst         = 10.0
	wsFrameTimeout       = 5 * time.Second

	wsCloseReplaced = 4001
	wsCloseSession  = 4002
)

const (
	wsFocusKindDirect = "direct"
	wsFocusKindGroup  = "group"
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

type wsFocusAckEvent struct {
	Type              string `json:"type"`
	Kind              string `json:"kind,omitempty"`
	Username          string `json:"username,omitempty"`
	GroupID           uint64 `json:"group_id,omitempty"`
	LastReadMessageID uint64 `json:"last_read_message_id"`
	Error             string `json:"error,omitempty"`
}

type wsGroupReadEvent struct {
	Type    string `json:"type"`
	GroupID uint64 `json:"group_id"`
}

type wsInboundFrame struct {
	Type     string `json:"type"`
	Kind     string `json:"kind"`
	Username string `json:"username"`
	GroupID  uint64 `json:"group_id"`
}

type wsFocus struct {
	kind         string
	peerID       uint64
	peerUsername string
	groupID      uint64
}

type wsRateLimiter struct {
	tokens float64
	last   time.Time
}

func (l *wsRateLimiter) allow(now time.Time) bool {
	if l.last.IsZero() {
		l.tokens = wsFrameBurst
	} else {
		l.tokens = math.Min(
			wsFrameBurst,
			l.tokens+now.Sub(l.last).Seconds()*wsFrameRatePerSecond,
		)
	}
	l.last = now
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}

type wsClient struct {
	userID    uint64
	username  string
	conn      *websocket.Conn
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once

	focusMu sync.RWMutex
	focus   wsFocus
	limiter wsRateLimiter
}

func (c *wsClient) setFocus(focus wsFocus) {
	c.focusMu.Lock()
	c.focus = focus
	c.focusMu.Unlock()
}

func (c *wsClient) clearFocus() {
	c.setFocus(wsFocus{})
}

func (c *wsClient) currentFocus() wsFocus {
	c.focusMu.RLock()
	defer c.focusMu.RUnlock()
	return c.focus
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

func (h *Hub) isDirectFocus(userID, peerID uint64) bool {
	h.mu.RLock()
	client := h.clients[userID]
	h.mu.RUnlock()
	if client == nil {
		return false
	}
	focus := client.currentFocus()
	return focus.kind == wsFocusKindDirect && focus.peerID == peerID
}

func (h *Hub) focusedGroupUsers(groupID uint64) []uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	users := make([]uint64, 0, len(h.clients))
	for userID, client := range h.clients {
		focus := client.currentFocus()
		if focus.kind == wsFocusKindGroup && focus.groupID == groupID {
			users = append(users, userID)
		}
	}
	return users
}

func (a *API) websocket(c *gin.Context) {
	current, ok := currentUser(c)
	if !ok {
		writeError(c, http.StatusUnauthorized, "unauthenticated", "请先登录")
		return
	}
	a.serveWebSocket(c, current, currentSessionExpiresAt(c))
}

func (a *API) serveWebSocket(c *gin.Context, current store.User, expiresAt time.Time) {
	conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("websocket upgrade: %v", err)
		return
	}

	client := &wsClient{
		userID:   current.ID,
		username: current.Username,
		conn:     conn,
		send:     make(chan []byte, wsSendBuffer),
		done:     make(chan struct{}),
	}
	a.hub.add(client)
	defer func() {
		a.hub.remove(client)
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
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType != websocket.TextMessage {
			continue
		}
		if !client.limiter.allow(time.Now()) {
			continue
		}
		a.handleClientFrame(client, payload)
	}
}

func (a *API) handleClientFrame(client *wsClient, payload []byte) {
	var frame wsInboundFrame
	if err := json.Unmarshal(payload, &frame); err != nil {
		return
	}
	switch frame.Type {
	case "focus":
		if frame.Kind != wsFocusKindDirect && frame.Kind != wsFocusKindGroup {
			return
		}
		client.clearFocus()
		switch frame.Kind {
		case wsFocusKindDirect:
			a.handleDirectFocus(client, strings.TrimSpace(frame.Username))
		case wsFocusKindGroup:
			a.handleGroupFocus(client, frame.GroupID)
		}
	case "blur":
		client.clearFocus()
	}
}

func (a *API) handleDirectFocus(client *wsClient, rawUsername string) {
	usernameKey := auth.UsernameKey(rawUsername)
	if usernameKey == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), wsFrameTimeout)
	defer cancel()

	target, err := a.store.GetUserByUsernameKey(ctx, usernameKey)
	if errors.Is(err, store.ErrUserNotFound) {
		a.sendFocusError(client, wsFocusKindDirect, rawUsername, 0, "user_not_found")
		return
	}
	if err != nil {
		log.Printf("websocket focus user %q: %v", rawUsername, err)
		a.sendFocusError(client, wsFocusKindDirect, rawUsername, 0, "message_service_unavailable")
		return
	}
	if target.ID == client.userID {
		a.sendFocusError(client, wsFocusKindDirect, target.Username, 0, "not_friend")
		return
	}
	if a.directReads == nil {
		a.sendFocusError(client, wsFocusKindDirect, target.Username, 0, "message_service_unavailable")
		return
	}
	relationship, err := a.store.GetRelationship(ctx, client.userID, target.ID)
	if err != nil {
		log.Printf("websocket focus relationship user=%d peer=%d: %v",
			client.userID, target.ID, err)
		a.sendFocusError(client, wsFocusKindDirect, target.Username, 0, "message_service_unavailable")
		return
	}
	if relationship != store.RelationshipFriend {
		// Focus is the only direct read path in v1.7, so a conversation that
		// outlived the friendship must still be markable as read.
		exists, err := a.store.HasDirectConversation(ctx, client.userID, target.ID)
		if err != nil {
			log.Printf("websocket focus conversation user=%d peer=%d: %v",
				client.userID, target.ID, err)
			a.sendFocusError(client, wsFocusKindDirect, target.Username, 0, "message_service_unavailable")
			return
		}
		if !exists {
			a.sendFocusError(client, wsFocusKindDirect, target.Username, 0, "not_friend")
			return
		}
	}

	result, err := a.directReads.AdvanceRead(
		ctx, client.userID, target.ID, 0, time.Now().UTC(),
	)
	if err != nil {
		log.Printf("websocket focus direct read user=%d peer=%d: %v",
			client.userID, target.ID, err)
		a.sendFocusError(client, wsFocusKindDirect, target.Username, 0, "message_service_unavailable")
		return
	}

	client.setFocus(wsFocus{
		kind:         wsFocusKindDirect,
		peerID:       target.ID,
		peerUsername: target.Username,
	})
	if result.Advanced {
		a.PublishRead(
			client.userID,
			target.ID,
			client.username,
			target.Username,
			result.LastReadMessageID,
		)
	}
	a.sendFocusAck(client, wsFocusAckEvent{
		Kind:              wsFocusKindDirect,
		Username:          target.Username,
		LastReadMessageID: result.LastReadMessageID,
	})
}

func (a *API) handleGroupFocus(client *wsClient, groupID uint64) {
	if groupID == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), wsFrameTimeout)
	defer cancel()

	if a.groupReads == nil {
		a.sendFocusError(client, wsFocusKindGroup, "", groupID, "message_service_unavailable")
		return
	}
	result, err := a.groupReads.AdvanceRead(
		ctx, groupID, client.userID, 0, time.Now().UTC(),
	)
	if err != nil {
		// One answer for not-a-member, dissolved and nonexistent groups: the
		// distinction would let anyone probe group IDs.
		if !errors.Is(err, messaging.ErrGroupNotMember) &&
			!errors.Is(err, messaging.ErrGroupDissolved) {
			log.Printf("websocket focus group=%d user=%d: %v",
				groupID, client.userID, err)
		}
		a.sendFocusError(client, wsFocusKindGroup, "", groupID, "not_group_member")
		return
	}

	client.setFocus(wsFocus{kind: wsFocusKindGroup, groupID: groupID})
	logGroupReadStreamError(result, groupID)
	if result.Advanced {
		if memberIDs, err := a.store.ListGroupMemberIDs(ctx, groupID); err != nil {
			log.Printf("websocket focus group members group=%d: %v", groupID, err)
		} else {
			a.publishGroupRead(memberIDs, groupID, map[uint64]struct{}{
				client.userID: {},
			})
		}
	}
	a.sendFocusAck(client, wsFocusAckEvent{
		Kind:              wsFocusKindGroup,
		GroupID:           groupID,
		LastReadMessageID: result.LastReadMessageID,
	})
}

func (a *API) sendFocusAck(client *wsClient, event wsFocusAckEvent) {
	event.Type = "focus.ack"
	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("marshal websocket focus ack: %v", err)
		return
	}
	client.enqueue(payload)
}

func (a *API) sendFocusError(client *wsClient, kind, username string, groupID uint64, code string) {
	a.sendFocusAck(client, wsFocusAckEvent{
		Kind:     kind,
		Username: username,
		GroupID:  groupID,
		Error:    code,
	})
}

func (a *API) publishGroupRead(
	memberIDs []uint64,
	groupID uint64,
	excluded map[uint64]struct{},
) {
	if len(memberIDs) == 0 {
		return
	}
	payload, err := json.Marshal(wsGroupReadEvent{
		Type:    "group.read",
		GroupID: groupID,
	})
	if err != nil {
		log.Printf("marshal websocket group read event: %v", err)
		return
	}
	targets := make([]uint64, 0, len(memberIDs))
	for _, userID := range memberIDs {
		if userID == 0 {
			continue
		}
		if _, skip := excluded[userID]; skip {
			continue
		}
		targets = append(targets, userID)
	}
	a.hub.Broadcast(targets, payload)
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
