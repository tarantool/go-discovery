package pool_test

import (
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/pool"
)

func init() {
	log.SetOutput(io.Discard)
}

type nextNoneMockBalancer struct {
	pool.Balancer
	Mode discovery.Mode
}

func (b *nextNoneMockBalancer) Next(mode discovery.Mode) (string, bool) {
	b.Mode = mode
	return "", false
}

type errMockDialerFactory struct {
	Instances []discovery.Instance
}

func (f *errMockDialerFactory) NewDialer(
	instance discovery.Instance) (tarantool.Dialer, tarantool.Opts, error) {
	f.Instances = append(f.Instances, instance)
	return nil, tarantool.Opts{}, fmt.Errorf("any error")
}

type unreachableMockDialerFactory struct {
	Instances []discovery.Instance
}

func (f *unreachableMockDialerFactory) NewDialer(
	instance discovery.Instance) (tarantool.Dialer, tarantool.Opts, error) {
	f.Instances = append(f.Instances, instance)

	return tarantool.NetDialer{
		Address: "non_exist:3363",
	}, tarantool.Opts{}, nil
}

// Unfortunately, we cannot test too much using unit tests without any
// real connectable Tarantool instance.

func TestNewPool_missed_factory(t *testing.T) {
	var dummyBalancer struct {
		pool.Balancer
	}

	testPool, err := pool.NewPool(nil, dummyBalancer)
	assert.Nil(t, testPool)
	assert.Equal(t, pool.ErrMissingDialerFactory, err)
}

func TestNewPool_missed_balancer(t *testing.T) {
	var dummyFactory struct {
		pool.DialerFactory
	}

	testPool, err := pool.NewPool(dummyFactory, nil)
	assert.Nil(t, testPool)
	assert.Equal(t, pool.ErrMissingBalancer, err)
}

func TestPoolDo_unsubscribed(t *testing.T) {
	var dummyBalancer struct {
		pool.Balancer
	}
	var dummyFactory struct {
		pool.DialerFactory
	}
	var dummyRequest struct {
		tarantool.Request
	}

	testPool, err := pool.NewPool(dummyFactory, dummyBalancer)
	require.NotNil(t, testPool)
	require.NoError(t, err)

	modes := []discovery.Mode{
		discovery.ModeAny,
		discovery.ModeRO,
		discovery.ModeRW,
	}
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			future := testPool.Do(dummyRequest, mode)
			_, err := future.GetResponse()

			assert.Equal(t, pool.ErrUnsubscribed, err)
		})
	}
}

func TestPoolDo_no_instances(t *testing.T) {
	var dummyFactory struct {
		pool.DialerFactory
	}
	var dummyRequest struct {
		tarantool.Request
	}

	balancer := &nextNoneMockBalancer{}
	testPool, err := pool.NewPool(dummyFactory, balancer)
	require.NotNil(t, testPool)
	require.NoError(t, err)

	testPool.Observe(nil, nil)
	defer testPool.Observe(nil, discovery.ErrUnsubscribe)

	modes := []discovery.Mode{
		discovery.ModeAny,
		discovery.ModeRO,
		discovery.ModeRW,
	}
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			future := testPool.Do(dummyRequest, mode)
			_, err := future.GetResponse()

			assert.Equal(t, pool.ErrNoConnectedInstances, err)
			assert.Equal(t, mode, balancer.Mode)
		})
	}
}

func TestPoolDo_no_instances_resubscribe(t *testing.T) {
	var dummyFactory struct {
		pool.DialerFactory
	}
	var dummyRequest struct {
		tarantool.Request
	}

	balancer := &nextNoneMockBalancer{}
	testPool, err := pool.NewPool(dummyFactory, balancer)
	require.NotNil(t, testPool)
	require.NoError(t, err)

	modes := []discovery.Mode{
		discovery.ModeAny,
		discovery.ModeRO,
		discovery.ModeRW,
	}
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			for i := 0; i < 5; i++ {
				testPool.Observe(nil, nil)
				future := testPool.Do(dummyRequest, mode)
				_, err := future.GetResponse()

				assert.Equal(t, pool.ErrNoConnectedInstances, err)
				testPool.Observe(nil, discovery.ErrUnsubscribe)

				future = testPool.Do(dummyRequest, mode)
				_, err = future.GetResponse()
				assert.Equal(t, pool.ErrUnsubscribed, err)
			}
		})
	}
}

func TestPoolDo_no_instances_if_dialer_factory_returns_err(t *testing.T) {
	var dummyRequest struct {
		tarantool.Request
	}

	balancer := &nextNoneMockBalancer{}
	factory := &errMockDialerFactory{}
	testPool, err := pool.NewPool(factory, balancer)
	require.NotNil(t, testPool)
	require.NoError(t, err)

	instances := []discovery.Instance{
		discovery.Instance{
			Name: "instance1",
		},
		discovery.Instance{
			Name: "instance2",
		},
	}
	events := []discovery.Event{}
	for _, instance := range instances {
		events = append(events, discovery.Event{
			Type: discovery.EventTypeAdd,
			New:  instance,
		})
	}

	testPool.Observe(events, nil)
	defer testPool.Observe(nil, discovery.ErrUnsubscribe)

	assert.ElementsMatch(t, instances, factory.Instances)

	modes := []discovery.Mode{
		discovery.ModeAny,
		discovery.ModeRO,
		discovery.ModeRW,
	}
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			for i := 0; i < 5; i++ {
				future := testPool.Do(dummyRequest, mode)
				_, err := future.GetResponse()

				assert.Equal(t, pool.ErrNoConnectedInstances, err)
			}
		})
	}
}

func TestPoolDo_no_instances_if_dialer_factory_returns_unreachable(t *testing.T) {
	var dummyRequest struct {
		tarantool.Request
	}

	balancer := &nextNoneMockBalancer{}
	factory := &unreachableMockDialerFactory{}
	testPool, err := pool.NewPool(factory, balancer)
	require.NotNil(t, testPool)
	require.NoError(t, err)

	instances := []discovery.Instance{
		discovery.Instance{
			Name: "instance1",
		},
		discovery.Instance{
			Name: "instance2",
		},
	}
	events := []discovery.Event{}
	for _, instance := range instances {
		events = append(events, discovery.Event{
			Type: discovery.EventTypeAdd,
			New:  instance,
		})
	}

	testPool.Observe(events, nil)
	defer testPool.Observe(nil, discovery.ErrUnsubscribe)

	assert.ElementsMatch(t, instances, factory.Instances)

	modes := []discovery.Mode{
		discovery.ModeAny,
		discovery.ModeRO,
		discovery.ModeRW,
	}
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			for i := 0; i < 5; i++ {
				future := testPool.Do(dummyRequest, mode)
				_, err := future.GetResponse()

				assert.Equal(t, pool.ErrNoConnectedInstances, err)
			}
		})
	}
}
