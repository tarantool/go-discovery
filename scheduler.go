package discovery

import (
	"context"
	"fmt"
)

// Scheduler is an interface that allows to schedule update events. An
// implementation of the interface should generate events, a client should
// wait on the `Wait()` method for a next event.
type Scheduler interface {
	// Wait blocks until a next event. It returns `nil` if an event occurs or
	// an `error` if the context expired or the Scheduler implementation has
	// been stopped.
	//
	// If the Scheduler was stopped by user calling the Stop() method, the
	// ErrSchedulerStopped error will be returned.
	Wait(ctx context.Context) error
	// Stop stops an event generation.
	Stop()
}

// ErrSchedulerStopped is an error that tells that user stopped the scheduler.
// All Wait() calls will return this error after the Stop() call.
var ErrSchedulerStopped = fmt.Errorf("scheduler was stopped")
