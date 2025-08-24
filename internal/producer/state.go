package producer

import (
	"sync"
	"time"
)

type Status struct {
	Running       bool   `json:"running"`
	RunID         string `json:"run_id"`
	RPSTarget     int    `json:"rps_target"`
	RPSActual     int    `json:"rps_actual"`
	ProducedTotal int64  `json:"produced_total"`
	UptimeSec     int64  `json:"uptime_sec"`
}

type State struct {
	mu        sync.RWMutex
	startedAt time.Time
}

func NewState() *State { return &State{startedAt: time.Now()} }

func (s *State) Snapshot() Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Status{UptimeSec: int64(time.Since(s.startedAt).Seconds())}
}
