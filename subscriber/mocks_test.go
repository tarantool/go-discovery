package subscriber_test

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/subscriber"
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

type mockSubscriber struct {
	subCnt   atomic.Int32
	unsubCnt atomic.Int32
	state    chan discovery.Observer

	eventsReturn []discovery.Event
}

func newMockSubscriber() *mockSubscriber {
	state := make(chan discovery.Observer, 1)
	state <- nil

	return &mockSubscriber{
		state: state,
	}
}

func (s *mockSubscriber) Subscribe(ctx context.Context,
	observer discovery.Observer) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	state := <-s.state
	if state != nil {
		s.state <- state
		return subscriber.ErrSubscriptionExists
	}
	state = observer
	s.state <- state

	s.subCnt.Add(1)

	if s.eventsReturn != nil {
		observer.Observe(s.eventsReturn, nil)
	}
	return nil
}

func (s *mockSubscriber) Unsubscribe(observer discovery.Observer) {
	state := <-s.state
	if state == observer && observer != nil {
		s.unsubCnt.Add(1)
		observer.Observe(nil, discovery.ErrUnsubscribe)

		state = nil
	}
	s.state <- state
}
