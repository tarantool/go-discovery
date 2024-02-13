package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/scheduler"
)

func TestPeriodic_Wait(t *testing.T) {
	cases := []struct {
		name      string
		scheduler *scheduler.Periodic
		period    time.Duration
	}{
		{
			name:      "Second",
			scheduler: scheduler.NewPeriodic(time.Second),
			period:    time.Second,
		},
		{
			name:      "3 seconds",
			scheduler: scheduler.NewPeriodic(3 * time.Second),
			period:    3 * time.Second,
		},
		{
			name:      "6 seconds",
			scheduler: scheduler.NewPeriodic(6 * time.Second),
			period:    6 * time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			// Wait for the next tick to track the actual wait time.
			err := tc.scheduler.Wait(ctx)
			assert.NoError(t, err)

			timeStart := time.Now()
			err = tc.scheduler.Wait(ctx)
			timeEnd := time.Now()

			assert.NoError(t, err)
			assert.InEpsilon(t, tc.period, timeEnd.Sub(timeStart), 0.05,
				"period of wait is incorrect")
		})
	}
}

func TestPeriodic_ConcurrentStop(t *testing.T) {
	for i := 0; i < 100000; i++ {
		go func() {
			var err error
			wg := sync.WaitGroup{}

			scheduler := scheduler.NewPeriodic(10 * time.Second)
			ctx := context.Background()

			wg.Add(2)

			go func() {
				err = scheduler.Wait(ctx)
				wg.Done()
			}()

			go func() {
				scheduler.Stop()
				wg.Done()
			}()

			wg.Wait()
			assert.Equal(t, discovery.ErrSchedulerStopped, err)
		}()
	}
}

func TestPeriodic_ConcurrentContextCancel(t *testing.T) {
	for i := 0; i < 100000; i++ {
		go func() {
			var err error
			wg := sync.WaitGroup{}

			scheduler := scheduler.NewPeriodic(10 * time.Second)
			ctx, cancel := context.WithCancel(context.Background())

			wg.Add(2)

			go func() {
				err = scheduler.Wait(ctx)
				wg.Done()
			}()

			go func() {
				cancel()
				wg.Done()
			}()

			wg.Wait()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "context canceled")
		}()
	}
}

func TestPeriodic_WaitAfterStop(t *testing.T) {
	scheduler := scheduler.NewPeriodic(10 * time.Second)
	ctx := context.Background()

	scheduler.Stop()

	err := scheduler.Wait(ctx)
	assert.Equal(t, discovery.ErrSchedulerStopped, err)
}

func TestPeriodic_WaitStopCancelCtx(t *testing.T) {
	scheduler := scheduler.NewPeriodic(10 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	cancel()
	scheduler.Stop()

	err := scheduler.Wait(ctx)
	assert.Equal(t, discovery.ErrSchedulerStopped, err)
}

// Test makes sure that there is no panic.
func TestPeriodic_ConcurrentStops(_ *testing.T) {
	for i := 0; i < 100000; i++ {
		go func() {
			wg := sync.WaitGroup{}

			scheduler := scheduler.NewPeriodic(10 * time.Second)

			wg.Add(2)

			go func() {
				scheduler.Stop()
				wg.Done()
			}()

			go func() {
				scheduler.Stop()
				wg.Done()
			}()

			wg.Wait()
		}()
	}
}
