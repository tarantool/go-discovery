package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/tarantool/go-discovery"
)

// Periodic allows updating the configuration by timer, after each time period.
type Periodic struct {
	ticker   *time.Ticker
	done     chan struct{}
	stopOnce sync.Once
}

// NewPeriodic creates a Periodic with a given tick time period.
func NewPeriodic(period time.Duration) *Periodic {
	ticker := time.NewTicker(period)
	return &Periodic{
		ticker:   ticker,
		done:     make(chan struct{}),
		stopOnce: sync.Once{},
	}
}

// Wait makes Periodic satisfy the Scheduler interface.
func (s *Periodic) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		return discovery.ErrSchedulerStopped
	default:
		select {
		case <-s.done:
			return discovery.ErrSchedulerStopped
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ticker.C:
			return nil
		}
	}
}

// Stop makes Periodic satisfy the Scheduler interface.
func (s *Periodic) Stop() {
	s.stopOnce.Do(func() {
		close(s.done)
		s.ticker.Stop()
	})
}
