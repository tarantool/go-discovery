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

var _ discovery.Subscriber = subscriber.NewSchedule(nil, nil)

type mockScheduler struct {
	cntWait atomic.Int32
	cntStop atomic.Int32

	stepWg sync.WaitGroup
	doStep chan struct{}
}

func (s *mockScheduler) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.doStep:
		s.cntWait.Add(1)
		s.stepWg.Done()
		return nil
	}
}

func (s *mockScheduler) Stop() {
	s.cntStop.Add(1)
}

type mockDiscoverer struct {
	cnt       atomic.Int32
	instances atomic.Pointer[[]discovery.Instance]
}

func (d *mockDiscoverer) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	d.cnt.Add(1)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		ptr := d.instances.Load()
		var insts []discovery.Instance
		if ptr != nil {
			insts = *ptr
		}
		return insts, nil
	}
}

func TestNewSchedule_NilScheduler(t *testing.T) {
	schedule := subscriber.NewSchedule(nil, nil)
	assert.NotNil(t, schedule)

	obs := newMockObserver()
	err := schedule.Subscribe(context.Background(), obs)
	assert.Equal(t, subscriber.ErrMissingScheduler, err)
}

func TestNewSchedule_NilDiscoverer(t *testing.T) {
	schedule := subscriber.NewSchedule(&mockScheduler{}, nil)
	assert.NotNil(t, schedule)

	obs := newMockObserver()
	err := schedule.Subscribe(context.Background(), obs)
	assert.Equal(t, subscriber.ErrMissingDiscoverer, err)
}

func TestSchedule_Subscribe_NilObserver(t *testing.T) {
	sched := &mockScheduler{}
	disc := &mockDiscoverer{}

	schedule := subscriber.NewSchedule(sched, disc)
	assert.NotNil(t, schedule)

	err := schedule.Subscribe(context.Background(), nil)
	assert.Equal(t, discovery.ErrMissingObserver, err)
	assert.Equal(t, int32(0), sched.cntWait.Load())
	assert.Equal(t, int32(0), sched.cntStop.Load())
	assert.Equal(t, int32(0), disc.cnt.Load())
}

func TestSchedule_Subscribe_Concurrent(t *testing.T) {
	sched := &mockScheduler{}
	disc := &mockDiscoverer{}

	for i := 0; i < 5000; i++ {
		go func() {
			schedule := subscriber.NewSchedule(sched, disc)
			assert.NotNil(t, schedule)

			obs := newMockObserver()
			defer schedule.Unsubscribe(obs)

			wg := sync.WaitGroup{}
			wg.Add(2)
			var err1, err2 error

			go func() {
				err1 = schedule.Subscribe(context.Background(), obs)
				wg.Done()
			}()

			go func() {
				err2 = schedule.Subscribe(context.Background(), obs)
				wg.Done()
			}()
			wg.Wait()

			assert.True(t, (err1 == nil) != (err2 == nil),
				"Only one of subscriptions should succeed")
			assert.True(t, errors.Is(err1, subscriber.ErrSubscriptionExists) !=
				errors.Is(err2, subscriber.ErrSubscriptionExists),
				"Only one of subscriptions should fail with specific error")
		}()
	}
}

func TestSchedule_Subscribe(t *testing.T) {
	sched := &mockScheduler{doStep: make(chan struct{}, 1)}
	sched.stepWg.Add(1)
	disc := &mockDiscoverer{}

	schedule := subscriber.NewSchedule(sched, disc)
	assert.NotNil(t, schedule)

	obs := newMockObserver()
	err := schedule.Subscribe(context.Background(), obs)
	assert.NoError(t, err)

	sched.doStep <- struct{}{}
	sched.stepWg.Wait()

	schedule.Unsubscribe(obs)

	assert.Equal(t, int32(1), obs.eventCnt.Load())

	assert.Equal(t, int32(1), sched.cntWait.Load())
	assert.Equal(t, int32(0), sched.cntStop.Load())

	// One call for getting the starting instances and one after scheduler.
	assert.Equal(t, int32(2), disc.cnt.Load())
}

func TestSchedule_Subscribe_Events(t *testing.T) {
	cases := []struct {
		name       string
		noEvent    bool
		event      discovery.Event
		startInst  []discovery.Instance
		updateInst []discovery.Instance
	}{
		{
			name: "Add",
			event: discovery.Event{
				Type: discovery.EventTypeAdd,
				New: discovery.Instance{
					Name: "Karl",
				},
			},
			startInst: []discovery.Instance{
				{
					Name: "Benjamin",
				},
			},
			updateInst: []discovery.Instance{
				{
					Name: "Karl",
				},
				{
					Name: "Benjamin",
				},
			},
		},
		{
			name: "Update",
			event: discovery.Event{
				Type: discovery.EventTypeUpdate,
				Old: discovery.Instance{
					Name: "Karl",
				},
				New: discovery.Instance{
					Name:  "Karl",
					Group: "four",
				},
			},
			startInst: []discovery.Instance{
				{
					Name: "Karl",
				},
			},
			updateInst: []discovery.Instance{
				{
					Name:  "Karl",
					Group: "four",
				},
			},
		},
		{
			name: "Remove",
			event: discovery.Event{
				Type: discovery.EventTypeRemove,
				Old: discovery.Instance{
					Name: "Karl",
				},
			},
			startInst: []discovery.Instance{
				{
					Name: "Karl",
				},
			},
			updateInst: []discovery.Instance{},
		},
		{
			name:    "Nothing",
			noEvent: true,
			startInst: []discovery.Instance{
				{
					Name:  "Dwarf",
					Group: "Dig",
				},
			},
			updateInst: []discovery.Instance{
				{
					Name:  "Dwarf",
					Group: "Dig",
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sched := &mockScheduler{doStep: make(chan struct{}, 1)}
			sched.stepWg.Add(1)
			disc := &mockDiscoverer{instances: atomic.Pointer[[]discovery.Instance]{}}
			disc.instances.Store(&tc.startInst)

			schedule := subscriber.NewSchedule(sched, disc)
			assert.NotNil(t, schedule)

			obs := newMockObserver()
			err := schedule.Subscribe(context.Background(), obs)
			assert.NoError(t, err)

			sched.doStep <- struct{}{}
			sched.stepWg.Wait()

			disc.instances.Store(&tc.updateInst)
			sched.stepWg.Add(1)

			sched.doStep <- struct{}{}
			sched.stepWg.Wait()

			schedule.Unsubscribe(obs)

			if tc.noEvent {
				// Only adding instances on start.
				assert.Equal(t, int32(1), obs.eventCnt.Load())
			} else {
				assert.Equal(t, int32(2), obs.eventCnt.Load())
				assert.Equal(t, []discovery.Event{tc.event}, *obs.recentEvents.Load())
			}
		})
	}
}

func TestSchedule_Subscribe_MultipleEvents(t *testing.T) {
	startInst := []discovery.Instance{
		{
			Name: "deleted",
		},
		{
			Name: "updated",
		},
		{
			Name:  "nothing",
			Group: "none",
		},
	}
	endInst := []discovery.Instance{
		{
			Name: "added",
		},
		{
			Name:  "updated",
			Group: "update",
		},
		{
			Name:  "nothing",
			Group: "none",
		},
	}

	sched := &mockScheduler{doStep: make(chan struct{}, 1)}
	sched.stepWg.Add(1)
	disc := &mockDiscoverer{instances: atomic.Pointer[[]discovery.Instance]{}}
	disc.instances.Store(&startInst)

	schedule := subscriber.NewSchedule(sched, disc)
	assert.NotNil(t, schedule)

	obs := newMockObserver()
	err := schedule.Subscribe(context.Background(), obs)
	assert.NoError(t, err)

	sched.doStep <- struct{}{}
	sched.stepWg.Wait()

	disc.instances.Store(&endInst)
	sched.stepWg.Add(1)

	sched.doStep <- struct{}{}
	sched.stepWg.Wait()

	schedule.Unsubscribe(obs)

	assert.Equal(t, int32(2), obs.eventCnt.Load())
	events := *obs.recentEvents.Load()
	assert.Equal(t, 3, len(events))
	assert.Equal(t, discovery.Event{
		Type: discovery.EventTypeAdd,
		New:  endInst[0],
	}, events[0])
	assert.Equal(t, discovery.Event{
		Type: discovery.EventTypeUpdate,
		Old:  startInst[1],
		New:  endInst[1],
	}, events[1])
	assert.Equal(t, discovery.Event{
		Type: discovery.EventTypeRemove,
		Old:  startInst[0],
	}, events[2])
}

func TestSchedule_Subscribe_InitialContextCancel(t *testing.T) {
	sched := &mockScheduler{}
	disc := &mockDiscoverer{}

	schedule := subscriber.NewSchedule(sched, disc)
	assert.NotNil(t, schedule)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	obs := newMockObserver()
	err := schedule.Subscribe(ctx, obs)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")

	assert.Equal(t, int32(0), sched.cntWait.Load())
	assert.Equal(t, int32(0), sched.cntStop.Load())
	assert.Equal(t, int32(1), disc.cnt.Load())
	assert.Equal(t, int32(0), obs.eventCnt.Load())
	assert.Equal(t, int32(0), obs.errCnt.Load())
}

func TestSchedule_Subscribe_ConcurrentInitialContextCancel(t *testing.T) {
	for i := 0; i < 500000; i++ {
		go func() {
			sched := &mockScheduler{}
			disc := &mockDiscoverer{}

			schedule := subscriber.NewSchedule(sched, disc)
			assert.NotNil(t, schedule)

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			obs := newMockObserver()

			go func() {
				_ = schedule.Subscribe(ctx, obs)
			}()
			go func() {
				schedule.Unsubscribe(obs)
			}()
		}()
	}
}

func TestSchedule_Unsubscribe_Concurrent(t *testing.T) {
	for i := 0; i < 100000; i++ {
		go func() {
			sched := &mockScheduler{doStep: make(chan struct{}, 1)}
			sched.stepWg.Add(1)
			disc := &mockDiscoverer{}

			wg := sync.WaitGroup{}
			wg.Add(2)

			schedule := subscriber.NewSchedule(sched, disc)
			assert.NotNil(t, schedule)

			obs := newMockObserver()
			err := schedule.Subscribe(context.Background(), obs)
			assert.NoError(t, err)

			sched.doStep <- struct{}{}
			sched.stepWg.Wait()

			go func() {
				schedule.Unsubscribe(obs)
				wg.Done()
			}()

			go func() {
				schedule.Unsubscribe(obs)
				wg.Done()
			}()
			wg.Wait()

			sched.stepWg.Add(1)
			sched.doStep <- struct{}{}
			sched.stepWg.Wait()

			assert.Equal(t, int32(1), obs.errCnt.Load())
			assert.Equal(t, discovery.ErrUnsubscribe, obs.recentErr)
			assert.Equal(t, int32(0), obs.eventCnt.Load())

			assert.Equal(t, int32(1), sched.cntWait.Load())
			assert.Equal(t, int32(0), sched.cntStop.Load())

			// One call for getting the starting instances and one after scheduler.
			assert.Equal(t, int32(2), disc.cnt.Load())
		}()
	}
}

// Test to make sure there is no panic.
func TestSchedule_Unsubscribe_NilObserver(t *testing.T) {
	sched := &mockScheduler{}
	disc := &mockDiscoverer{}

	schedule := subscriber.NewSchedule(sched, disc)
	assert.NotNil(t, schedule)

	schedule.Unsubscribe(nil)
}

func TestSchedule_ReuseSchedule(t *testing.T) {
	sched := &mockScheduler{doStep: make(chan struct{}, 1)}
	sched.stepWg.Add(1)
	disc := &mockDiscoverer{}

	schedule := subscriber.NewSchedule(sched, disc)
	assert.NotNil(t, schedule)

	obs := newMockObserver()
	err := schedule.Subscribe(context.Background(), obs)
	assert.NoError(t, err)

	schedule.Unsubscribe(obs)

	obs.errWg.Wait()
	obs.errWg.Add(1)

	err = schedule.Subscribe(context.Background(), obs)
	assert.NoError(t, err)

	schedule.Unsubscribe(obs)
}
