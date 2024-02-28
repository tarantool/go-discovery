package subscriber

import (
	"context"
	"fmt"

	"github.com/tarantool/go-discovery"
)

type filterObserver struct {
	observer     discovery.Observer
	filters      []discovery.Filter
	firstObserve bool
}

func runFilters(inst discovery.Instance, filters []discovery.Filter) bool {
	for _, filter := range filters {
		if !filter.Filter(inst) {
			return false
		}
	}
	return true
}

func (o *filterObserver) Observe(events []discovery.Event, err error) {
	if err != nil {
		o.observer.Observe(nil, err)
		return
	}

	var result []discovery.Event
	for _, event := range events {
		switch event.Type {
		case discovery.EventTypeAdd:
			if runFilters(event.New, o.filters) {
				result = append(result, event)
			}
		case discovery.EventTypeUpdate:
			oldFiltered := runFilters(event.Old, o.filters)
			newFiltered := runFilters(event.New, o.filters)

			if oldFiltered && newFiltered {
				result = append(result, event)
			} else if oldFiltered {
				result = append(result, discovery.Event{
					Type: discovery.EventTypeRemove,
					Old:  event.Old,
				})
			} else if newFiltered {
				result = append(result, discovery.Event{
					Type: discovery.EventTypeAdd,
					New:  event.New,
				})
			}
		case discovery.EventTypeRemove:
			if runFilters(event.Old, o.filters) {
				result = append(result, event)
			}
		}
	}

	if len(result) != 0 || o.firstObserve {
		o.firstObserve = false
		o.observer.Observe(result, nil)
	}
}

type filterState struct {
	observer *filterObserver
}

// Filter allows filtering the stream of events from the inner Subscriber.
// Only nodes with suitable configurations are sent to the observer.
//
// Note that only one observer at a time could be subscribed.
type Filter struct {
	state      chan filterState
	subscriber discovery.Subscriber
	filters    []discovery.Filter
}

var (
	// ErrMissingSubscriber is an error that tells that the provided inner
	// subscriber is nil.
	ErrMissingSubscriber = fmt.Errorf("inner subscriber is missing")
)

// NewFilter creates a Filter with given inner subscriber and filters.
func NewFilter(subscriber discovery.Subscriber,
	filters ...discovery.Filter) *Filter {
	state := make(chan filterState, 1)
	state <- filterState{}

	return &Filter{
		state:      state,
		subscriber: subscriber,
		filters:    filters,
	}
}

// Subscribe subscribes the observer to the filtered updates.
//
// Note that only one subscribed observer is allowed at a time.
// To unsubscribe, use the Unsubscribe method.
func (s *Filter) Subscribe(ctx context.Context, observer discovery.Observer) error {
	if s.subscriber == nil {
		return ErrMissingSubscriber
	}
	if observer == nil {
		return discovery.ErrMissingObserver
	}

	state := <-s.state
	if state.observer != nil {
		s.state <- state
		return ErrSubscriptionExists
	}
	state.observer = &filterObserver{
		observer:     observer,
		filters:      s.filters,
		firstObserve: true,
	}

	s.state <- state

	err := s.subscriber.Subscribe(ctx, state.observer)
	if err != nil {
		<-s.state
		s.state <- filterState{}

		return err
	}
	return nil
}

// Unsubscribe unsubscribes the observer from the updates.
func (s *Filter) Unsubscribe(observer discovery.Observer) {
	state := <-s.state
	if state.observer != nil && state.observer.observer == observer {
		s.state <- state

		s.subscriber.Unsubscribe(state.observer)

		<-s.state
		state = filterState{}
	}
	s.state <- state
}
