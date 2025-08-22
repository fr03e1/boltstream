package readiness

import "sync/atomic"

type Gate struct {
	v atomic.Bool
}

func New(initial bool) *Gate {
	g := &Gate{}
	g.v.Store(initial)
	return g
}

func (g *Gate) Set(ok bool) {
	g.v.Store(ok)
}

func (g *Gate) Ready() bool {
	return g.v.Load()
}
