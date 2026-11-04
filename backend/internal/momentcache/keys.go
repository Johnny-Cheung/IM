package momentcache

import (
	"fmt"
	"strconv"
	"strings"
)

const defaultPrefix = "im:v12:"

// Keyspace builds every derived Moments cache key below one Redis namespace.
// The cache is disposable: all keys can be removed without losing Moments data.
type Keyspace struct {
	prefix string
}

func NewKeyspace(prefix string) Keyspace {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = defaultPrefix
	}
	if !strings.HasSuffix(prefix, ":") {
		prefix += ":"
	}
	return Keyspace{prefix: prefix}
}

func (k Keyspace) Prefix() string {
	return k.prefixOrDefault()
}

func (k Keyspace) prefixOrDefault() string {
	if k.prefix == "" {
		return defaultPrefix
	}
	return k.prefix
}

// feedKey materializes the viewer's recent visible feed snapshot. Scores and
// members are moment IDs so the existing before_id cursor keeps its meaning.
func (k Keyspace) feedKey(viewerID uint64) string {
	return fmt.Sprintf("%smoments:feed:%d", k.prefixOrDefault(), viewerID)
}

func (k Keyspace) feedMetaKey(viewerID uint64) string {
	return fmt.Sprintf("%smoments:feed:meta:%d", k.prefixOrDefault(), viewerID)
}

// feedGenKey is bumped whenever the viewer's friendship set changes, so a
// warm that started before the change cannot install a stale snapshot.
func (k Keyspace) feedGenKey(viewerID uint64) string {
	return fmt.Sprintf("%smoments:feed:gen:%d", k.prefixOrDefault(), viewerID)
}

func (k Keyspace) feedWarmLockKey(viewerID uint64) string {
	return fmt.Sprintf("%smoments:feed:warm:%d", k.prefixOrDefault(), viewerID)
}

func (k Keyspace) feedTempKey(viewerID uint64, token string) string {
	return fmt.Sprintf("%smoments:feed:tmp:%d:%s", k.prefixOrDefault(), viewerID, token)
}

// authorKey is the pull source used for high-fanout authors. It contains only
// friends-visible moments; private moments never enter it.
func (k Keyspace) authorKey(authorID uint64) string {
	return fmt.Sprintf("%smoments:author:%d", k.prefixOrDefault(), authorID)
}

func (k Keyspace) authorMetaKey(authorID uint64) string {
	return fmt.Sprintf("%smoments:author:meta:%d", k.prefixOrDefault(), authorID)
}

func (k Keyspace) authorWarmLockKey(authorID uint64) string {
	return fmt.Sprintf("%smoments:author:warm:%d", k.prefixOrDefault(), authorID)
}

func (k Keyspace) authorTempKey(authorID uint64, token string) string {
	return fmt.Sprintf("%smoments:author:tmp:%d:%s", k.prefixOrDefault(), authorID, token)
}

// pullAuthorsKey is sticky: once an author crosses the fanout threshold they
// stay on the pull path, avoiding a history gap if their friend count falls.
func (k Keyspace) pullAuthorsKey() string {
	return k.prefixOrDefault() + "moments:pull-authors"
}

func (k Keyspace) likeMembersKey(momentID uint64) string {
	return fmt.Sprintf("%smoments:likes:%d:members", k.prefixOrDefault(), momentID)
}

// likeCountKey is both the cached count and the ready marker. A count of zero
// is still written, so its presence means the cached member set is complete.
func (k Keyspace) likeCountKey(momentID uint64) string {
	return fmt.Sprintf("%smoments:likes:%d:count", k.prefixOrDefault(), momentID)
}

func (k Keyspace) likeEpochKey(momentID uint64) string {
	return fmt.Sprintf("%smoments:likes:%d:epoch", k.prefixOrDefault(), momentID)
}

func formatID(id uint64) string {
	return strconv.FormatUint(id, 10)
}
