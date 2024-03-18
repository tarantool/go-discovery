package subscriber

import (
	"context"
	"fmt"
	"sync"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/observer"
	"github.com/tarantool/go-discovery/pool"
)

type connectableObserver struct {
	observer discovery.Observer
	pool     *pool.Pool
}

func (o *connectableObserver) Observe(events []discovery.Event, err error) {
	o.pool.Observe(events, err)
	if err != nil {
		o.observer.Observe(events, err)
	}
}

type connectableBalancer struct {
	pool.Balancer

	mu        sync.Mutex
	observer  discovery.Observer
	instances map[string]discovery.Instance
}

func newConnectableBalancer(observer discovery.Observer) *connectableBalancer {
	return &connectableBalancer{
		observer:  observer,
		instances: make(map[string]discovery.Instance),
	}
}

func (b *connectableBalancer) Add(instance discovery.Instance) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.instances[instance.Name] = instance
	b.observer.Observe([]discovery.Event{
		{
			Type: discovery.EventTypeAdd,
			New:  instance,
		},
	}, nil)
	return nil
}

func (b *connectableBalancer) Remove(instanceName string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	instance, ok := b.instances[instanceName]
	if !ok {
		return
	}

	delete(b.instances, instanceName)
	b.observer.Observe([]discovery.Event{
		{
			Type: discovery.EventTypeRemove,
			Old:  instance,
		},
	}, nil)
}

type connectableState struct {
	observer      *connectableObserver
	innerObserver discovery.Observer
}

// Connectable filters the stream of events from the inner Subscriber
// and isolates from it only nodes available for connection.
// Additional events are generated when a node is disconnected.
type Connectable struct {
	state      chan connectableState
	subscriber discovery.Subscriber
	factory    discovery.DialerFactory
}

// ErrMissingFactory is an error that tells that the provided
// DialerFactory is nil.
var ErrMissingFactory = fmt.Errorf("a dialer factory is missing")

// NewConnectable creates a Connectable with given inner subscriber.
func NewConnectable(factory discovery.DialerFactory,
	subscriber discovery.Subscriber) *Connectable {

	state := make(chan connectableState, 1)
	state <- connectableState{}

	return &Connectable{
		state:      state,
		subscriber: subscriber,
		factory:    factory,
	}
}

// Subscribe subscribes for updates on the availability of nodes.
//
// Note that only one subscribed observer is allowed at a time.
// To unsubscribe, use the Unsubscribe method.
func (s *Connectable) Subscribe(ctx context.Context,
	innerObserver discovery.Observer) error {
	if s.factory == nil {
		return ErrMissingFactory
	}
	if s.subscriber == nil {
		return ErrMissingSubscriber
	}
	if innerObserver == nil {
		return discovery.ErrMissingObserver
	}

	state := <-s.state
	if state.observer != nil {
		s.state <- state
		return ErrSubscriptionExists
	}
	accumulator := observer.NewAccumulator(innerObserver)
	helperPool, err := pool.NewPool(s.factory, newConnectableBalancer(accumulator))
	if err != nil {
		s.state <- connectableState{}
		return err
	}

	state = connectableState{
		observer: &connectableObserver{
			observer: accumulator,
			pool:     helperPool,
		},
		innerObserver: innerObserver,
	}
	s.state <- state

	err = s.subscriber.Subscribe(ctx, state.observer)
	if err != nil {
		<-s.state
		s.state <- connectableState{}
	}
	return err
}

// Unsubscribe unsubscribes the observer from the updates.
func (s *Connectable) Unsubscribe(observer discovery.Observer) {
	state := <-s.state
	if state.observer != nil && state.innerObserver == observer {
		s.state <- state

		s.subscriber.Unsubscribe(state.observer)

		<-s.state
		state = connectableState{}
	}
	s.state <- state
}
