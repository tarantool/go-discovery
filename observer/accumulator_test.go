package observer_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/observer"
)

var _ discovery.Observer = observer.NewAccumulator(nil)

type mockObserver struct {
	eventCnt     atomic.Int32
	recentEvents atomic.Pointer[[]discovery.Event]

	errCnt    atomic.Int32
	recentErr error

	eventWg sync.WaitGroup
	errWg   sync.WaitGroup
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
		o.eventWg.Done()
	}
	if err != nil {
		o.errCnt.Add(1)
		o.recentErr = err
		o.errWg.Done()
	}
}

func TestAccumulator_Observe(t *testing.T) {
	events1 := []discovery.Event{
		{
			Type: discovery.EventTypeAdd,
			New: discovery.Instance{
				Name: "some name",
			},
		},
		{
			Type: discovery.EventTypeRemove,
			Old: discovery.Instance{
				Name: "some other name",
			},
		},
	}
	events2 := []discovery.Event{
		{
			Type: discovery.EventTypeUpdate,
			Old: discovery.Instance{
				Name: "Old",
			},
			New: discovery.Instance{
				Name: "New",
			},
		},
	}

	obs := newMockObserver()

	accumulator := observer.NewAccumulator(obs)
	accumulator.Observe(events1, nil)

	obs.eventWg.Wait()

	assert.Equal(t, int32(1), obs.eventCnt.Load())
	for _, event := range *obs.recentEvents.Load() {
		assert.Contains(t, events1, event)
	}
	assert.Equal(t, int32(0), obs.errCnt.Load())

	obs.eventWg.Add(1)
	accumulator.Observe(events2, nil)

	obs.eventWg.Wait()

	assert.Equal(t, int32(2), obs.eventCnt.Load())
	assert.Equal(t, events2, *obs.recentEvents.Load())
	assert.Equal(t, int32(0), obs.errCnt.Load())

	accumulator.Observe(nil, fmt.Errorf("error"))

	obs.errWg.Wait()

	assert.Equal(t, int32(1), obs.errCnt.Load())
	assert.Errorf(t, obs.recentErr, "error")
}

func TestAccumulator_reuse(t *testing.T) {
	event := []discovery.Event{
		{
			Type: discovery.EventTypeAdd,
			New: discovery.Instance{
				Name: "some name",
			},
		},
	}
	obsError := fmt.Errorf("some error")

	obs := newMockObserver()
	accumulator := observer.NewAccumulator(obs)

	accumulator.Observe(event, nil)

	obs.eventWg.Wait()
	obs.eventWg.Add(1)

	assert.Equal(t, int32(1), obs.eventCnt.Load())
	assert.Equal(t, event, *obs.recentEvents.Load())
	assert.Equal(t, int32(0), obs.errCnt.Load())

	accumulator.Observe(event, obsError)

	obs.errWg.Wait()
	obs.errWg.Add(1)
	obs.eventWg.Add(1)

	assert.Equal(t, int32(2), obs.eventCnt.Load())
	assert.Equal(t, event, *obs.recentEvents.Load())
	assert.Equal(t, int32(1), obs.errCnt.Load())
	assert.Equal(t, obsError, obs.recentErr)

	accumulator.Observe(event, nil)

	obs.eventWg.Wait()
	obs.eventWg.Add(1)

	assert.Equal(t, int32(3), obs.eventCnt.Load())
	assert.Equal(t, event, *obs.recentEvents.Load())
	assert.Equal(t, int32(1), obs.errCnt.Load())

	accumulator.Observe(nil, obsError)

	obs.errWg.Wait()
	obs.errWg.Add(1)

	assert.Equal(t, int32(3), obs.eventCnt.Load())
	assert.Equal(t, event, *obs.recentEvents.Load())
	assert.Equal(t, int32(2), obs.errCnt.Load())
	assert.Equal(t, obsError, obs.recentErr)
}

func TestAccumulator_reduce(t *testing.T) {
	cases := []struct {
		name   string
		events []discovery.Event
		result []discovery.Event
	}{
		{
			name: "add update",
			events: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Flying Dutchman",
					},
				},
			},
			result: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Flying Dutchman",
					},
				},
			},
		},
		{
			name: "add remove",
			events: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Cpt. Barbossa",
					},
				},
				{
					Type: discovery.EventTypeRemove,
					Old: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
			},
			result: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Cpt. Barbossa",
					},
				},
			},
		},
		{
			name: "update update",
			events: []discovery.Event{
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Flying Dutchman",
					},
				},
			},
			result: []discovery.Event{
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Flying Dutchman",
					},
				},
			},
		},
		{
			name: "update update cancellation",
			events: []discovery.Event{
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Cpt. Barbossa",
					},
				},
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
					New: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
				},
			},
			result: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Cpt. Barbossa",
					},
				},
			},
		},
		{
			name: "update remove",
			events: []discovery.Event{
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
				{
					Type: discovery.EventTypeRemove,
					Old: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
			},
			result: []discovery.Event{
				{
					Type: discovery.EventTypeRemove,
					Old: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
				},
			},
		},
		{
			name: "remove add",
			events: []discovery.Event{
				{
					Type: discovery.EventTypeRemove,
					Old: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
				},
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Cpt. Barbossa",
					},
				},
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
				},
			},
			result: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Cpt. Barbossa",
					},
				},
			},
		},
		{
			name: "remove add updated",
			events: []discovery.Event{
				{
					Type: discovery.EventTypeRemove,
					Old: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
				},
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
			},
			result: []discovery.Event{
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
			},
		},
		{
			name: "add update remove",
			events: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
				},
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Cpt. Barbossa",
					},
				},
				{
					Type: discovery.EventTypeRemove,
					Old: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
			},
			result: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Cpt. Barbossa",
					},
				},
			},
		},
		{
			name: "reduce different",
			events: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
					New: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
				},
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name:  "Cpt. Barbossa",
						Group: "Black Pearl",
					},
				},
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name:  "Cpt. Barbossa",
						Group: "Black Pearl",
					},
					New: discovery.Instance{
						Name: "Cpt. Barbossa",
					},
				},
				{
					Type: discovery.EventTypeRemove,
					Old: discovery.Instance{
						Name: "Cpt. Barbossa",
					},
				},
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name: "Cpt. Jack Sparrow",
					},
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Cpt. Barbossa",
					},
				},
			},
			result: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name:  "Cpt. Jack Sparrow",
						Group: "Black Pearl",
					},
				},
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Cpt. Barbossa",
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := newMockObserver()

			accumulator := observer.NewAccumulator(obs)
			accumulator.Observe(tc.events, nil)

			obs.eventWg.Wait()

			assert.Equal(t, int32(1), obs.eventCnt.Load())
			for _, event := range *obs.recentEvents.Load() {
				assert.Contains(t, tc.result, event)
			}
			assert.Equal(t, int32(0), obs.errCnt.Load())
		})
	}
}

func TestAccumulator_Observe_notify(t *testing.T) {
	obs := newMockObserver()

	accumulator := observer.NewAccumulator(obs)
	accumulator.Observe(nil, nil)
	obs.eventWg.Wait()

	assert.Equal(t, int32(1), obs.eventCnt.Load())
	assert.Equal(t, []discovery.Event{}, *obs.recentEvents.Load())
	assert.Equal(t, int32(0), obs.errCnt.Load())
}
