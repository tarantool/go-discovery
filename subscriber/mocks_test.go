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

	needCountEvents bool
	eventWg         sync.WaitGroup
	errWg           sync.WaitGroup
}

func newMockObserver() *mockObserver {
	obs := &mockObserver{}
	obs.recentEvents.Store(&[]discovery.Event{})
	obs.eventWg.Add(1)
	obs.errWg.Add(1)
	return obs
}

func (o *mockObserver) Observe(events []discovery.Event, err error) {
	if len(events) > 0 || err == nil {
		o.eventCnt.Add(1)
		o.recentEvents.Store(&events)
		if o.needCountEvents {
			for range events {
				o.eventWg.Done()
			}
		}
	}
	if err != nil {
		o.errCnt.Add(1)
		o.recentErr = err
		o.errWg.Done()
	}
}

type mockSubscriber struct {
	subCnt   atomic.Int32
	unsubCnt atomic.Int32
	state    chan discovery.Observer
	stop     chan struct{}

	eventWait    chan struct{}
	eventsReturn []discovery.Event
}

func newMockSubscriber() *mockSubscriber {
	state := make(chan discovery.Observer, 1)
	state <- nil

	return &mockSubscriber{
		state:     state,
		eventWait: make(chan struct{}, 1),
		stop:      make(chan struct{}, 1),
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

	s.eventWait <- struct{}{}
	if s.eventsReturn != nil {
		observer.Observe(s.eventsReturn, nil)
	}

	go func() {
		for {
			select {
			case <-s.stop:
				return
			case <-ctx.Done():
				return
			case s.eventWait <- struct{}{}:
				select {
				case <-s.stop:
					return
				default:
				}
				if s.eventsReturn != nil {
					observer.Observe(s.eventsReturn, nil)
				}
			}
		}
	}()
	return nil
}

func (s *mockSubscriber) Unsubscribe(observer discovery.Observer) {
	state := <-s.state
	if state == observer && observer != nil {
		s.unsubCnt.Add(1)
		observer.Observe(nil, discovery.ErrUnsubscribe)

		state = nil
		s.stop <- struct{}{}
	}
	s.state <- state
	<-s.eventWait
}
