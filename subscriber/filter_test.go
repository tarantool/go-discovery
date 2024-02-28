package subscriber_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/subscriber"
)

var _ discovery.Subscriber = subscriber.NewFilter(nil, nil)

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

func TestNewFilter_NilSubscriber(t *testing.T) {
	filter := subscriber.NewFilter(nil, nil)
	assert.NotNil(t, filter)

	obs := newMockObserver()
	err := filter.Subscribe(context.Background(), obs)
	assert.Equal(t, subscriber.ErrMissingSubscriber, err)
}

func TestFilter_Subscribe_NilObserver(t *testing.T) {
	sub := newMockSubscriber()

	filter := subscriber.NewFilter(sub, nil)
	assert.NotNil(t, filter)

	err := filter.Subscribe(context.Background(), nil)
	assert.Equal(t, discovery.ErrMissingObserver, err)
	assert.Equal(t, int32(0), sub.subCnt.Load())
	assert.Equal(t, int32(0), sub.unsubCnt.Load())
}

func TestFilter_Subscribe_Concurrent(t *testing.T) {
	for i := 0; i < 100000; i++ {
		go func() {
			sub := newMockSubscriber()

			filter := subscriber.NewFilter(sub, nil)
			assert.NotNil(t, filter)

			obs := newMockObserver()
			defer filter.Unsubscribe(obs)

			wg := sync.WaitGroup{}
			wg.Add(2)
			var err1, err2 error

			go func() {
				err1 = filter.Subscribe(context.Background(), obs)
				wg.Done()
			}()

			go func() {
				err2 = filter.Subscribe(context.Background(), obs)
				wg.Done()
			}()
			wg.Wait()

			assert.True(t, (err1 == nil) != (err2 == nil),
				"Only one of subscriptions should succeed")
			assert.True(t, errors.Is(err1, subscriber.ErrSubscriptionExists) !=
				errors.Is(err2, subscriber.ErrSubscriptionExists),
				"Only one of subscriptions should fail with specific error")
			assert.Equal(t, int32(1), sub.subCnt.Load())
		}()
	}
}

func TestFilter_Subscribe(t *testing.T) {
	cases := []struct {
		name       string
		filters    []discovery.Filter
		origEvents []discovery.Event
		resEvents  []discovery.Event
		noEvents   bool
	}{
		{
			name:       "nothing",
			filters:    nil,
			origEvents: nil,
			resEvents:  []discovery.Event{},
			noEvents:   true,
		},
		{
			name: "add",
			filters: []discovery.Filter{
				discovery.FilterFunc(func(inst discovery.Instance) bool {
					return inst.Name == "Chucky" || inst.Name == "Jason"
				}),
				discovery.FilterFunc(func(inst discovery.Instance) bool {
					return inst.Group == "Friday" || inst.Group == "Doll"
				}),
			},
			origEvents: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name:  "Jason",
						Group: "Friday",
					},
				},
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name:  "Chucky",
						Group: "Night",
					},
				},
			},
			resEvents: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name:  "Jason",
						Group: "Friday",
					},
				},
			},
		},
		{
			name: "add from update",
			filters: []discovery.Filter{
				discovery.FilterFunc(func(inst discovery.Instance) bool {
					return inst.Name == "Chucky" || inst.Name == "Jason"
				}),
				discovery.FilterFunc(func(inst discovery.Instance) bool {
					return inst.Group == "Friday" || inst.Group == "Doll"
				}),
			},
			origEvents: []discovery.Event{
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name:  "Freddy",
						Group: "Night",
					},
					New: discovery.Instance{
						Name:  "Jason",
						Group: "Friday",
					},
				},
			},
			resEvents: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name:  "Jason",
						Group: "Friday",
					},
				},
			},
		},
		{
			name: "update",
			filters: []discovery.Filter{
				discovery.FilterFunc(func(inst discovery.Instance) bool {
					return inst.Name == "Chucky" || inst.Name == "Jason"
				}),
				discovery.FilterFunc(func(inst discovery.Instance) bool {
					return inst.Group == "Friday" || inst.Group == "Doll"
				}),
			},
			origEvents: []discovery.Event{
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name:  "Jason",
						Group: "Friday",
					},
					New: discovery.Instance{
						Name:  "Chucky",
						Group: "Doll",
					},
				},
			},
			resEvents: []discovery.Event{
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name:  "Jason",
						Group: "Friday",
					},
					New: discovery.Instance{
						Name:  "Chucky",
						Group: "Doll",
					},
				},
			},
		},
		{
			name: "remove",
			filters: []discovery.Filter{
				discovery.FilterFunc(func(inst discovery.Instance) bool {
					return inst.Name == "Chucky" || inst.Name == "Jason"
				}),
				discovery.FilterFunc(func(inst discovery.Instance) bool {
					return inst.Group == "Friday" || inst.Group == "Doll"
				}),
			},
			origEvents: []discovery.Event{
				{
					Type: discovery.EventTypeRemove,
					Old: discovery.Instance{
						Name:  "Jason",
						Group: "Friday",
					},
				},
				{
					Type: discovery.EventTypeRemove,
					Old: discovery.Instance{
						Name:  "Freddy",
						Group: "Night",
					},
				},
			},
			resEvents: []discovery.Event{
				{
					Type: discovery.EventTypeRemove,
					Old: discovery.Instance{
						Name:  "Jason",
						Group: "Friday",
					},
				},
			},
		},
		{
			name: "remove from update",
			filters: []discovery.Filter{
				discovery.FilterFunc(func(inst discovery.Instance) bool {
					return inst.Name == "Chucky" || inst.Name == "Jason"
				}),
				discovery.FilterFunc(func(inst discovery.Instance) bool {
					return inst.Group == "Friday" || inst.Group == "Doll"
				}),
			},
			origEvents: []discovery.Event{
				{
					Type: discovery.EventTypeUpdate,
					Old: discovery.Instance{
						Name:  "Jason",
						Group: "Friday",
					},
					New: discovery.Instance{
						Name:  "Freddy",
						Group: "Night",
					},
				},
			},
			resEvents: []discovery.Event{
				{
					Type: discovery.EventTypeRemove,
					Old: discovery.Instance{
						Name:  "Jason",
						Group: "Friday",
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := newMockSubscriber()
			sub.eventsReturn = tc.origEvents

			filter := subscriber.NewFilter(sub, tc.filters...)
			assert.NotNil(t, filter)

			obs := newMockObserver()
			err := filter.Subscribe(context.Background(), obs)
			assert.NoError(t, err)

			if tc.noEvents {
				assert.Equal(t, int32(0), obs.eventCnt.Load())
			} else {
				assert.Equal(t, int32(1), obs.eventCnt.Load())
			}
			assert.Equal(t, tc.resEvents, *obs.recentEvents.Load())

			filter.Unsubscribe(obs)

			assert.Equal(t, int32(1), obs.errCnt.Load())
			assert.Equal(t, discovery.ErrUnsubscribe, obs.recentErr)
		})
	}
}

func TestFilter_Subscribe_InitialContextCancel(t *testing.T) {
	sub := newMockSubscriber()

	filter := subscriber.NewFilter(sub, nil)
	assert.NotNil(t, filter)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	obs := newMockObserver()
	err := filter.Subscribe(ctx, obs)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")

	assert.Equal(t, int32(0), sub.subCnt.Load())
	assert.Equal(t, int32(0), sub.unsubCnt.Load())
	assert.Equal(t, int32(0), obs.eventCnt.Load())
	assert.Equal(t, int32(0), obs.errCnt.Load())
}

func TestFilter_Unsubscribe_Concurrent(t *testing.T) {
	for i := 0; i < 100000; i++ {
		go func() {
			sub := newMockSubscriber()

			filter := subscriber.NewFilter(sub, nil)
			assert.NotNil(t, filter)

			obs := newMockObserver()
			err := filter.Subscribe(context.Background(), obs)
			assert.NoError(t, err)

			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				filter.Unsubscribe(obs)
				wg.Done()
			}()

			go func() {
				filter.Unsubscribe(obs)
				wg.Done()
			}()
			wg.Wait()

			assert.Equal(t, int32(1), obs.errCnt.Load())
			assert.Equal(t, discovery.ErrUnsubscribe, obs.recentErr)
			assert.Equal(t, int32(0), obs.eventCnt.Load())
			assert.Equal(t, int32(1), sub.unsubCnt.Load())
		}()
	}
}

// Test to make sure there is no panic.
func TestFilter_Unsubscribe_NilObserver(t *testing.T) {
	sub := newMockSubscriber()

	filter := subscriber.NewFilter(sub, nil)
	assert.NotNil(t, filter)

	filter.Unsubscribe(nil)
}

func TestFilter_ReuseSchedule(t *testing.T) {
	sub := newMockSubscriber()

	filter := subscriber.NewFilter(sub, nil)
	assert.NotNil(t, filter)

	obs := newMockObserver()
	err := filter.Subscribe(context.Background(), obs)
	assert.NoError(t, err)

	filter.Unsubscribe(obs)

	obs.errWg.Add(1)

	err = filter.Subscribe(context.Background(), obs)
	assert.NoError(t, err)

	filter.Unsubscribe(obs)
}
