package messaging

import "sync/atomic"

// SeqGate is the process-local readiness switch for sequence-based writes and
// read states. It is closed while conversation counters are being calibrated
// and while seq:ready is missing. A nil gate is always ready.
type SeqGate struct {
	closed atomic.Bool
}

func NewSeqGate() *SeqGate {
	return &SeqGate{}
}

// Open marks the gate ready.
func (g *SeqGate) Open() {
	if g != nil {
		g.closed.Store(false)
	}
}

// Close marks the gate unavailable.
func (g *SeqGate) Close() {
	if g != nil {
		g.closed.Store(true)
	}
}

// Ready returns ErrSeqNotReady while the gate is closed.
func (g *SeqGate) Ready() error {
	if g != nil && g.closed.Load() {
		return ErrSeqNotReady
	}
	return nil
}

// IsReady reports whether the gate is open.
func (g *SeqGate) IsReady() bool {
	return g.Ready() == nil
}
