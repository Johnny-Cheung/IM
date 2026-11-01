package httpapi

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Johnny-Cheung/IM/backend/internal/store"
)

const (
	// sessionCacheTTL is a safety net for revocations that bypass this process.
	// Normal logout, replacement and password changes invalidate the entry
	// immediately through the per-user mutation sequence.
	sessionCacheTTL = 5 * time.Minute
	userCacheTTL    = time.Hour

	cacheSweepInterval = time.Minute
)

type sessionCacheKey [32]byte

func sessionCacheKeyFor(tokenHash []byte) (sessionCacheKey, bool) {
	var key sessionCacheKey
	if len(tokenHash) != len(key) {
		return key, false
	}
	copy(key[:], tokenHash)
	return key, true
}

type sessionCacheEntry struct {
	session   store.SessionUser
	mutation  uint64
	expiresAt time.Time
}

// sessionCache keeps authenticated sessions in process. Mutation is a per-user
// sequence: an auth change (login/replacement, logout, password change, nickname
// update) bumps it, so every cached session for that user becomes stale without
// scanning the cache. A cache fill records the sequence observed before the
// database read and refuses to store the result if the user mutated meanwhile.
type sessionCache struct {
	mu        sync.RWMutex
	entries   map[sessionCacheKey]sessionCacheEntry
	mutations map[uint64]uint64
	sequence  atomic.Uint64
	ttl       time.Duration
	lastSweep time.Time
}

func newSessionCache() *sessionCache {
	return &sessionCache{
		entries:   make(map[sessionCacheKey]sessionCacheEntry),
		mutations: make(map[uint64]uint64),
		ttl:       sessionCacheTTL,
	}
}

func (c *sessionCache) queryStart() uint64 {
	return c.sequence.Load()
}

// markMutation invalidates all cached sessions for userID and returns the new
// sequence. Callers that just created a session can store it with this value.
func (c *sessionCache) markMutation(userID uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	sequence := c.sequence.Add(1)
	c.mutations[userID] = sequence
	return sequence
}

func (c *sessionCache) get(key sessionCacheKey, now time.Time) (store.SessionUser, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	if !ok {
		c.mu.RUnlock()
		return store.SessionUser{}, false
	}
	mutation := c.mutations[entry.session.User.ID]
	c.mu.RUnlock()

	if entry.mutation != mutation || !entry.expiresAt.After(now) {
		return store.SessionUser{}, false
	}
	return entry.session, true
}

func (c *sessionCache) put(
	key sessionCacheKey,
	session store.SessionUser,
	queryStart uint64,
	now time.Time,
) {
	if session.RevokedAt != nil || !session.ExpiresAt.After(now) {
		return
	}
	expiresAt := now.Add(c.ttl)
	if session.ExpiresAt.Before(expiresAt) {
		expiresAt = session.ExpiresAt
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	mutation := c.mutations[session.User.ID]
	if mutation > queryStart {
		// An auth mutation raced the database read; this result may predate it.
		return
	}
	c.entries[key] = sessionCacheEntry{
		session:   session,
		mutation:  mutation,
		expiresAt: expiresAt,
	}
	c.sweepLocked(now)
}

func (c *sessionCache) putFresh(
	key sessionCacheKey,
	session store.SessionUser,
	mutation uint64,
	now time.Time,
) {
	if session.RevokedAt != nil || !session.ExpiresAt.After(now) {
		return
	}
	expiresAt := now.Add(c.ttl)
	if session.ExpiresAt.Before(expiresAt) {
		expiresAt = session.ExpiresAt
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mutations[session.User.ID] != mutation {
		return
	}
	c.entries[key] = sessionCacheEntry{
		session:   session,
		mutation:  mutation,
		expiresAt: expiresAt,
	}
	c.sweepLocked(now)
}

func (c *sessionCache) delete(key sessionCacheKey) {
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

func (c *sessionCache) sweepLocked(now time.Time) {
	if now.Sub(c.lastSweep) < cacheSweepInterval {
		return
	}
	c.lastSweep = now
	for key, entry := range c.entries {
		if !entry.expiresAt.After(now) ||
			entry.mutation != c.mutations[entry.session.User.ID] {
			delete(c.entries, key)
		}
	}
}

type userCacheEntry struct {
	user      store.User
	mutation  uint64
	expiresAt time.Time
}

// userCache mirrors the immutable username -> public profile lookup. Nickname
// changes bump the per-key mutation so a stale fill cannot overwrite them.
type userCache struct {
	mu        sync.RWMutex
	entries   map[string]userCacheEntry
	mutations map[string]uint64
	sequence  atomic.Uint64
	ttl       time.Duration
	lastSweep time.Time
}

func newUserCache() *userCache {
	return &userCache{
		entries:   make(map[string]userCacheEntry),
		mutations: make(map[string]uint64),
		ttl:       userCacheTTL,
	}
}

func (c *userCache) queryStart() uint64 {
	return c.sequence.Load()
}

func (c *userCache) markMutation(usernameKey string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	sequence := c.sequence.Add(1)
	c.mutations[usernameKey] = sequence
	return sequence
}

func (c *userCache) get(usernameKey string, now time.Time) (store.User, bool) {
	c.mu.RLock()
	entry, ok := c.entries[usernameKey]
	if !ok {
		c.mu.RUnlock()
		return store.User{}, false
	}
	mutation := c.mutations[usernameKey]
	c.mu.RUnlock()

	if entry.mutation != mutation || !entry.expiresAt.After(now) {
		return store.User{}, false
	}
	return entry.user, true
}

func (c *userCache) put(
	usernameKey string,
	user store.User,
	queryStart uint64,
	now time.Time,
) {
	if usernameKey == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mutations[usernameKey] > queryStart {
		return
	}
	c.putLocked(usernameKey, user, c.mutations[usernameKey], now)
}

func (c *userCache) putFresh(
	usernameKey string,
	user store.User,
	mutation uint64,
	now time.Time,
) {
	if usernameKey == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mutations[usernameKey] != mutation {
		return
	}
	c.putLocked(usernameKey, user, mutation, now)
}

func (c *userCache) putLocked(
	usernameKey string,
	user store.User,
	mutation uint64,
	now time.Time,
) {
	c.entries[usernameKey] = userCacheEntry{
		user: store.User{
			ID:       user.ID,
			Username: user.Username,
			Nickname: user.Nickname,
		},
		mutation:  mutation,
		expiresAt: now.Add(c.ttl),
	}
	c.sweepLocked(now)
}

func (c *userCache) sweepLocked(now time.Time) {
	if now.Sub(c.lastSweep) < cacheSweepInterval {
		return
	}
	c.lastSweep = now
	for key, entry := range c.entries {
		if !entry.expiresAt.After(now) ||
			entry.mutation != c.mutations[key] {
			delete(c.entries, key)
		}
	}
}
