package subscriber

import (
	"context"
	"fmt"
	"reflect"

	"github.com/tarantool/go-discovery"
)

type scheduleState struct {
	observer discovery.Observer
	cancel   context.CancelFunc
	done     chan struct{}
}

// Schedule combines Scheduler and Discoverer.
// It receives a list of nodes from the Discoverer by the given schedule
// and sends the events to the subscriber.
//
// Note that only one observer at a time could be subscribed.
type Schedule struct {
	state      chan scheduleState
	scheduler  discovery.Scheduler
	discoverer discovery.Discoverer
}

var (
	// ErrMissingScheduler is an error that tells that the provided
	// scheduler is nil.
	ErrMissingScheduler = fmt.Errorf("scheduler is missing")
	// ErrMissingDiscoverer is an error that tells that the provided
	// discoverer is nil.
	ErrMissingDiscoverer = fmt.Errorf("discoverer is missing")
)

// NewSchedule creates a Schedule with given scheduler and discoverer.
func NewSchedule(scheduler discovery.Scheduler,
	discoverer discovery.Discoverer) *Schedule {
	state := make(chan scheduleState, 1)
	state <- scheduleState{}

	return &Schedule{
		state:      state,
		scheduler:  scheduler,
		discoverer: discoverer,
	}
}

func compareInstances(old, new []discovery.Instance) []discovery.Event {
	var events []discovery.Event
	instMap := make(map[string]discovery.Instance)

	for _, inst := range old {
		instMap[inst.Name] = inst
	}

	for _, inst := range new {
		if oldInst, ok := instMap[inst.Name]; ok {
			if !reflect.DeepEqual(oldInst, inst) {
				events = append(events, discovery.Event{
					Type: discovery.EventTypeUpdate,
					Old:  oldInst,
					New:  inst,
				})
			}
			delete(instMap, inst.Name)
		} else {
			events = append(events, discovery.Event{
				Type: discovery.EventTypeAdd,
				New:  inst,
			})
		}
	}

	for _, inst := range instMap {
		events = append(events, discovery.Event{
			Type: discovery.EventTypeRemove,
			Old:  inst,
		})
	}

	return events
}

func (s *Schedule) observeError(ctx context.Context,
	observer discovery.Observer, err error) {
	select {
	case <-ctx.Done():
		observer.Observe(nil, discovery.ErrUnsubscribe)
	default:
		observer.Observe(nil, err)
	}
}

// Subscribe subscribes the observer to the updates of the nodes.
//
// Note that only one subscribed observer is allowed at a time.
// To unsubscribe, use the Unsubscribe method.
func (s *Schedule) Subscribe(ctx context.Context,
	observer discovery.Observer) error {
	if s.scheduler == nil {
		return ErrMissingScheduler
	}
	if s.discoverer == nil {
		return ErrMissingDiscoverer
	}
	if observer == nil {
		return discovery.ErrMissingObserver
	}

	state := <-s.state
	if state.observer != nil {
		s.state <- state
		return ErrSubscriptionExists
	}
	state.observer = observer

	subCtx, cancel := context.WithCancel(context.Background())
	state.cancel = cancel
	state.done = make(chan struct{})

	s.state <- state

	currentState, err := s.discoverer.Discovery(ctx)
	if err != nil {
		state.cancel()
		close(state.done)

		<-s.state
		s.state <- scheduleState{}

		return err
	}

	events := compareInstances(nil, currentState)
	observer.Observe(events, nil)

	go func() {
		defer close(state.done)

		for {
			err = s.scheduler.Wait(subCtx)
			if err != nil {
				s.observeError(subCtx, observer, err)
				return
			}

			insts, err := s.discoverer.Discovery(subCtx)
			if err != nil {
				s.observeError(subCtx, observer, err)
				return
			}

			events = compareInstances(currentState, insts)

			if len(events) > 0 {
				observer.Observe(events, nil)
				currentState = insts
			}
		}
	}()
	return nil
}

// Unsubscribe unsubscribes the observer from the updates of the nodes.
func (s *Schedule) Unsubscribe(observer discovery.Observer) {
	state := <-s.state
	if state.observer == observer && observer != nil {
		s.state <- state

		state.cancel()
		<-state.done

		<-s.state
		state = scheduleState{}
	}
	s.state <- state
}
