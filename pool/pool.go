package pool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tarantool/go-discovery"

	"github.com/tarantool/go-tarantool/v2"
	ttpool "github.com/tarantool/go-tarantool/v2/pool"
)

var (
	// ErrMissingDialerFactory is returned if a nil dialer factory is passed to
	// the Pool constructor.
	ErrMissingDialerFactory = fmt.Errorf("dialer factory argument is missing")
	// ErrMissingBalancer is returned if a nil balancer is passed to
	// the Pool constructor.
	ErrMissingBalancer = fmt.Errorf("dialer factory argument is missing")
	// ErrUnsubscribed is returned if the pool does not have an active
	// subscription to a Subscriber.
	ErrUnsubscribed = fmt.Errorf("pool is not subscribed yet")
	// ErrNoConnectedInstances is returned if there are no instances for the
	// mode currently in the pool.
	ErrNoConnectedInstances = fmt.Errorf("there are no connected instances")
)

// instanceState is a helper structure to hold a state of an active instance.
type instanceState struct {
	// Instance is an instance configuration.
	Instance discovery.Instance
	// AddCancel cancels adding instance to a ttpool.ConnectionPool by call.
	AddCancel context.CancelFunc
	// AddDone will be closed after the ttpool.ConnectionPool.Add() call.
	AddDone chan struct{}
	// Added indicates the result of the ttpool.ConnectionPool.Add().
	// It's true on success.
	Added bool
}

// Pool is a connection pool. It implements discovery.Observer interface to
// be able to subscribe to instance configurations updates.
type Pool struct {
	// balancer is a balancer used by the pool.
	balancer Balancer
	// factory is a DialerFactory used to create connection settings to
	// instances.
	factory DialerFactory
	// pool is ttpool.ConnectionPool object that used to handle connections
	// and execute requests.
	pool atomic.Pointer[ttpool.ConnectionPool]
	// states is a thread-safe of map[string]*instanceState. We need the
	// thread-safe to avoid data races between Pool.Observer() and
	// connectionHandler.
	states sync.Map
}

// NewPool creates a Pool object. It is required to subscribe the created pool
// to any discovery.Subscriber to start receiving new instances configurations
// and create actual connections to instances.
func NewPool(factory DialerFactory, balancer Balancer) (*Pool, error) {
	if factory == nil {
		return nil, ErrMissingDialerFactory
	}
	if balancer == nil {
		return nil, ErrMissingBalancer
	}
	return &Pool{
		balancer: balancer,
		factory:  factory,
	}, nil
}

// Observe observes instances configurations update events. The
// subscribtion is supported to only a single discovery.Subscriber at the
// moment.
//
// A concurrent subscribtion to >1 discovery.Subscriber will lead to
// an invalid events processing and undefined behavior:
//
//	subscriber.Subscribe(pool)
//	otherSubscriber.Subscribe(pool)
//
// At the same time it supports re-subscribtion, so the code:
//
//	subscriber.Subscribe(pool)
//	subscriber.Unsubscribe(pool)
//	otherSubscriber.Subscribe(pool)
//	otherSubscriber.Unsubscribe(pool)
//
// is valid.
func (p *Pool) Observe(events []discovery.Event, err error) {
	if err != nil {
		p.unsubscribe(err)
		return
	}

	pool, subscribed := p.trySubscribe()
	if pool == nil {
		// It should not happen. The error is already logged.
		return
	}
	if subscribed {
		p.pool.Store(pool)
	}

	for _, event := range events {
		switch event.Type {
		case discovery.EventTypeAdd:
			// The remove is not required, but it will not be nice to get
			// problems due to incorrect implementation of the subscriber.
			p.removeInstance(event.New)
			p.addInstance(event.New)
		case discovery.EventTypeUpdate:
			p.removeInstance(event.Old)
			// The remove is not required, but it will not be nice to get
			// problems due to incorrect implementation of the subscriber.
			p.removeInstance(event.New)
			p.addInstance(event.New)
		case discovery.EventTypeRemove:
			p.removeInstance(event.Old)
		default:
			// It should not happen in fact.
			log.Printf("unexpected event type: %d", event.Type)
		}
	}
}

// Do executes the request on instances with the specified mode. You could
// use ModeAdapter type to adapt it to tarantool.Doer interface.
func (p *Pool) Do(request tarantool.Request, mode discovery.Mode) *tarantool.Future {
	for {
		pool := p.pool.Load()
		if pool == nil {
			future := tarantool.NewFuture(request)
			future.SetError(ErrUnsubscribed)
			return future
		}

		next, exist := p.balancer.Next(mode)
		if !exist {
			future := tarantool.NewFuture(request)
			future.SetError(ErrNoConnectedInstances)
			return future
		}

		// If the pool is already closed and we recreated the pool and refilled
		// the balancer somewhere between p.pool.Load(), then we will retry
		// with the case:
		// err == ttpool.ErrNoHealthyInstance. So all should be fine here.
		future := pool.DoInstance(request, next)

		var err error
		select {
		case _, ok := <-future.WaitChan():
			if !ok {
				// The future is done, we could check an error here.
				_, err = future.GetResponse()
			}
		default:
		}

		if errors.Is(err, ttpool.ErrNoHealthyInstance) {
			// It may happen in two cases:
			// 1. The tarantool pool has not yet added an instance to the
			// active ones.
			// 2. The tarantool pool has already removed an instance, but we
			// have not.
			// In both cases we have a lag, but it should not be too long, so
			// we can just to retry.
			continue
		}
		return future
	}
}

// trySubscribe tries to subscribe and return pool, true if subscribed.
// It may return pool, false if it is already subscribed.
// It may return nil, false if it is unable to subscribe (should not happen).
func (p *Pool) trySubscribe() (*ttpool.ConnectionPool, bool) {
	pool := p.pool.Load()
	if pool != nil {
		return pool, false
	}

	var err error
	pool, err = ttpool.ConnectWithOpts(context.Background(), nil, ttpool.Opts{
		CheckTimeout:      time.Second,
		ConnectionHandler: newConnectionHandler(p),
	})
	if err != nil {
		// It should not happen in fact, but it looks better than panic.
		log.Printf("unexpected error on the pool initialization: %s", err)
		return nil, false
	}
	return pool, true
}

// unsubscribe registers an unsubscribe error and clears the pool state.
func (p *Pool) unsubscribe(err error) {
	pool := p.pool.Swap(nil)
	if pool == nil {
		// Unsubscribed.
		return
	}

	// It waits for cancellation and should unregister all endpoints with
	// a handler.
	pool.Close()

	// Ensure that all done.
	p.states.Range(func(_, value any) bool {
		state := value.(*instanceState)
		state.AddCancel()
		<-state.AddDone
		return true
	})

	// Just recreate a map.
	p.states = sync.Map{}
	log.Printf("pool unregistered, reason: %s", err)
}

// addInstance starts to process an instance.
func (p *Pool) addInstance(instance discovery.Instance) {
	ctx, cancel := context.WithCancel(context.Background())
	dialer, opts, err := p.factory.NewDialer(instance)
	if err != nil {
		cancel()
		log.Printf("failed to create an instance dialer %q: %s",
			instance.Name, err)
		return
	}

	state := &instanceState{
		Instance:  instance,
		AddCancel: cancel,
		AddDone:   make(chan struct{}),
		Added:     false,
	}
	p.states.Store(instance.Name, state)

	// The goroutine could be concurrent with unsubscribe, so it's better to
	// save the pool pointer here to avoid data races.
	pool := p.pool.Load()
	go func() {
		err := pool.Add(ctx, ttpool.Instance{
			Name:   instance.Name,
			Dialer: dialer,
			Opts:   opts,
		})
		state.Added = err == nil
		close(state.AddDone)
	}()
}

// removeInstance stops the instance processing.
func (p *Pool) removeInstance(instance discovery.Instance) {
	if value, exist := p.states.LoadAndDelete(instance.Name); exist {
		state := value.(*instanceState)
		state.AddCancel()
		<-state.AddDone
		if state.Added {
			err := p.pool.Load().Remove(instance.Name)
			log.Printf("an error on an instance removing %q: %s",
				instance.Name, err)
		}
	}
}

// connectionHandler is a concrete implementation of
// ttpool.ConnectionHandler.
type connectionHandler struct {
	pool *Pool
}

// newConnectionHandler creates a new connectionHandler object.
func newConnectionHandler(pool *Pool) *connectionHandler {
	return &connectionHandler{
		pool: pool,
	}
}

// Discovered is called when ttpool.ConnectionPool discoveres an instance.
// The handler call adds the instance to the pool balancer.
func (h *connectionHandler) Discovered(name string, _ *tarantool.Connection,
	role ttpool.Role) error {
	if value, exist := h.pool.states.Load(name); exist {
		state := value.(*instanceState)
		state.Instance.Mode = roleToMode(role)

		if err := h.pool.balancer.Add(state.Instance); err != nil {
			return fmt.Errorf("instance %q not added due to a balancer decision: %w",
				name, err)
		}
		return nil
	}

	// It's expected and not a problem. We remove an instance from the map
	// first and only then stop adding and remove it from
	// ttpool.ConnectionPool.
	return nil
}

// Diactivated is called when ttpool.ConnectionPool disconnects an instance.
// The handler call removes the instance from the pool balancer.
func (h *connectionHandler) Deactivated(name string, _ *tarantool.Connection,
	_ ttpool.Role) error {
	h.pool.balancer.Remove(name)
	return nil
}

// roleToMode maps ttpool.Role to discovery.Mode.
func roleToMode(role ttpool.Role) discovery.Mode {
	switch role {
	case ttpool.UnknownRole:
		return discovery.ModeAny
	case ttpool.MasterRole:
		return discovery.ModeRO
	case ttpool.ReplicaRole:
		return discovery.ModeRW
	default:
		// It should not happen, but panic is not a good idea.
		log.Printf("Unknown role: %d", role)
		return discovery.ModeAny
	}
}
