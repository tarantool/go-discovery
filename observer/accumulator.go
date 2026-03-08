// Package observer implements an observer for Tarantool 3.0.
package observer

import (
	"reflect"

	"github.com/tarantool/go-discovery"
)

type accumulatorEvent struct {
	events []discovery.Event
	err    error
}

// Accumulator accumulates events and sends them by batches.
// It helps not to delay a subscription with a slow observer.
// While one batch is being sent, the other is accumulating.
type Accumulator struct {
	observer discovery.Observer
	eventsCh chan []accumulatorEvent
	done     <-chan struct{}
}

// NewAccumulator creates an Observer with given inner observer.
func NewAccumulator(observer discovery.Observer) *Accumulator {
	return &Accumulator{
		observer: observer,
	}
}

func getInstanceName(event discovery.Event) string {
	switch event.Type {
	case discovery.EventTypeAdd:
		return event.New.Name
	case discovery.EventTypeRemove:
		return event.Old.Name
	case discovery.EventTypeUpdate:
		// We believe that we have a guarantee that
		// event.Old.Name == event.New.Name.
		return event.Old.Name
	default:
		return ""
	}
}

func reduceAdd(old, newEv discovery.Event) (discovery.Event, bool) {
	switch newEv.Type {
	case discovery.EventTypeRemove:
		return discovery.Event{}, true
	case discovery.EventTypeUpdate:
		return discovery.Event{
			Type: discovery.EventTypeAdd,
			New:  newEv.New,
		}, false
	default:
		return old, false
	}
}

func reduceRemove(old, newEv discovery.Event) (discovery.Event, bool) {
	switch newEv.Type {
	case discovery.EventTypeAdd:
		if reflect.DeepEqual(old.Old, newEv.New) {
			return discovery.Event{}, true
		}
		return discovery.Event{
			Type: discovery.EventTypeUpdate,
			Old:  old.Old,
			New:  newEv.New,
		}, false
	default:
		return old, false
	}
}

func reduceUpdate(old, newEv discovery.Event) (discovery.Event, bool) {
	switch newEv.Type {
	case discovery.EventTypeRemove:
		return discovery.Event{
			Type: discovery.EventTypeRemove,
			Old:  old.Old,
		}, false
	case discovery.EventTypeUpdate:
		if reflect.DeepEqual(old.Old, newEv.New) {
			return discovery.Event{}, true
		}
		return discovery.Event{
			Type: discovery.EventTypeUpdate,
			Old:  old.Old,
			New:  newEv.New,
		}, false
	default:
		return old, false
	}
}

func reduceEvent(old, newEv discovery.Event) (discovery.Event, bool) {
	switch old.Type {
	case discovery.EventTypeAdd:
		return reduceAdd(old, newEv)
	case discovery.EventTypeRemove:
		return reduceRemove(old, newEv)
	case discovery.EventTypeUpdate:
		return reduceUpdate(old, newEv)
	}
	return old, false
}

func reduceEvents(events []discovery.Event) []discovery.Event {
	reduced := make(map[string]discovery.Event)

	for _, event := range events {
		name := getInstanceName(event)
		if oldEvent, ok := reduced[name]; ok {
			if event, isReduced := reduceEvent(oldEvent, event); isReduced {
				delete(reduced, name)
			} else {
				reduced[name] = event
			}
		} else {
			reduced[name] = event
		}
	}

	result := make([]discovery.Event, 0, len(reduced))
	for _, event := range reduced {
		result = append(result, event)
	}
	return result
}

func startObserving(eventsCh <-chan []accumulatorEvent,
	observer discovery.Observer) <-chan struct{} {

	done := make(chan struct{})
	notify := true
	go func() {
		defer close(done)
		for accumulatorEvents := range eventsCh {
			var (
				events []discovery.Event
				err    error
			)

			for _, accumulatorEvent := range accumulatorEvents {
				events = append(events, accumulatorEvent.events...)
				if err == nil {
					err = accumulatorEvent.err
				}
			}

			events = reduceEvents(events)

			if len(events) != 0 || err != nil || notify {
				notify = false
				observer.Observe(events, err)
			}
		}
	}()
	return done
}

// Observe adds events to the event storage.
// In case of an error, Accumulator will send all stored events with the error.
func (o *Accumulator) Observe(events []discovery.Event, err error) {
	if o.eventsCh == nil {
		o.eventsCh = make(chan []accumulatorEvent, 1)
		o.done = startObserving(o.eventsCh, o.observer)
	}

	var storedEvents []accumulatorEvent
	select {
	case storedEvents = <-o.eventsCh:
	default:
		storedEvents = make([]accumulatorEvent, 0)
	}
	o.eventsCh <- append(storedEvents, accumulatorEvent{
		events: events,
		err:    err,
	})

	if err != nil {
		close(o.eventsCh)
		<-o.done
		o.eventsCh, o.done = nil, nil
	}
}
