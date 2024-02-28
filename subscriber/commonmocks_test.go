package subscriber_test

import (
	"sync"
	"sync/atomic"

	"github.com/tarantool/go-discovery"
)

type mockObserver struct {
	eventCnt     atomic.Int32
	recentEvents atomic.Pointer[[]discovery.Event]

	errCnt    atomic.Int32
	recentErr error

	errWg sync.WaitGroup
}

func newMockObserver() *mockObserver {
	obs := &mockObserver{}
	obs.recentEvents.Store(&[]discovery.Event{})
	obs.errWg.Add(1)
	return obs
}

func (o *mockObserver) Observe(events []discovery.Event, err error) {
	if err != nil {
		o.errCnt.Add(1)
		o.recentErr = err
		o.errWg.Done()
	} else {
		o.eventCnt.Add(1)
		o.recentEvents.Store(&events)
	}
}
