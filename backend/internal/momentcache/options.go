package momentcache

import (
	"time"
)

const (
	defaultPullThreshold    = 500
	defaultFeedWarmLimit    = 500
	defaultMaxPullSources   = 100
	defaultFeedTTL          = 7 * 24 * time.Hour
	defaultLikeTTL          = 7 * 24 * time.Hour
	defaultOperationTimeout = 300 * time.Millisecond
)

// Options tunes the disposable Moments cache. The zero value is valid and
// enables the default feed/like limits.
type Options struct {
	Enabled          bool
	PullThreshold    int
	FeedWarmLimit    int
	MaxPullSources   int
	FeedTTL          time.Duration
	LikeTTL          time.Duration
	OperationTimeout time.Duration
	Now              func() time.Time
}

func (o Options) withDefaults() Options {
	if o.PullThreshold <= 0 {
		o.PullThreshold = defaultPullThreshold
	}
	if o.FeedWarmLimit <= 0 {
		o.FeedWarmLimit = defaultFeedWarmLimit
	}
	if o.MaxPullSources <= 0 {
		o.MaxPullSources = defaultMaxPullSources
	}
	if o.FeedTTL <= 0 {
		o.FeedTTL = defaultFeedTTL
	}
	if o.LikeTTL <= 0 {
		o.LikeTTL = defaultLikeTTL
	}
	if o.OperationTimeout <= 0 {
		o.OperationTimeout = defaultOperationTimeout
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}
