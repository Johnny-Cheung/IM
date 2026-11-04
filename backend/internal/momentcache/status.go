package momentcache

import "sync/atomic"

// Status is the last in-process snapshot exposed by /ops/infra. It never
// performs a Redis or MySQL request.
type Status struct {
	Enabled        bool   `json:"enabled"`
	Degraded       bool   `json:"degraded"`
	FeedHits       uint64 `json:"feed_hits"`
	FeedFallbacks  uint64 `json:"feed_fallbacks"`
	FeedWarms      uint64 `json:"feed_warms"`
	WarmErrors     uint64 `json:"warm_errors"`
	PushRecipients uint64 `json:"push_recipients"`
	PullSources    uint64 `json:"pull_sources"`
	LikeHits       uint64 `json:"like_hits"`
	LikeMisses     uint64 `json:"like_misses"`
	CacheErrors    uint64 `json:"cache_errors"`
}

type counters struct {
	feedHits       atomic.Uint64
	feedFallbacks  atomic.Uint64
	feedWarms      atomic.Uint64
	warmErrors     atomic.Uint64
	pushRecipients atomic.Uint64
	pullSources    atomic.Uint64
	likeHits       atomic.Uint64
	likeMisses     atomic.Uint64
	cacheErrors    atomic.Uint64
}

func (c *counters) snapshot(enabled, degraded bool) Status {
	return Status{
		Enabled:        enabled,
		Degraded:       degraded,
		FeedHits:       c.feedHits.Load(),
		FeedFallbacks:  c.feedFallbacks.Load(),
		FeedWarms:      c.feedWarms.Load(),
		WarmErrors:     c.warmErrors.Load(),
		PushRecipients: c.pushRecipients.Load(),
		PullSources:    c.pullSources.Load(),
		LikeHits:       c.likeHits.Load(),
		LikeMisses:     c.likeMisses.Load(),
		CacheErrors:    c.cacheErrors.Load(),
	}
}
