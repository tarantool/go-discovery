package discoverer_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/discoverer"
)

var _ discovery.Discoverer = discoverer.NewCache(nil)

func TestCache_Discovery_returnsErrorOnNilBaseDiscoverer(t *testing.T) {
	cache := discoverer.NewCache(nil)

	_, err := cache.Discovery(context.Background())
	assert.Equal(t, discoverer.ErrMissingDiscoverer, err)
}

func TestCache_Discovery_returnsBaseResultOnFirstCall(t *testing.T) {
	baseInstance := discovery.Instance{
		Group:      "group1",
		Replicaset: "rs1",
		Name:       "instance1",
	}
	instances := []discovery.Instance{baseInstance}
	mock := &mockDiscoverer{instances: instances}
	cache := discoverer.NewCache(mock)

	result, err := cache.Discovery(context.Background())
	require.NoError(t, err)
	assert.Equal(t, instances, result)
}

func TestCache_Discovery_returnsCachedResultOnSecondCall(t *testing.T) {
	baseInstance := discovery.Instance{
		Group:      "group1",
		Replicaset: "rs1",
		Name:       "instance1",
	}
	instances := []discovery.Instance{baseInstance}
	mock := &mockDiscoverer{instances: instances}
	cache := discoverer.NewCache(mock)

	result1, err1 := cache.Discovery(context.Background())
	assert.Equal(t, instances, result1)
	require.NoError(t, err1)

	newInstance := discovery.Instance{
		Group:      "group2",
		Replicaset: "rs2",
		Name:       "instance2",
	}
	mock.instances = []discovery.Instance{newInstance}

	result2, err2 := cache.Discovery(context.Background())
	require.NoError(t, err2)
	assert.Equal(t, instances, result2)
}

func TestCache_Discovery_callsBaseDiscovererOnlyOnce(t *testing.T) {
	mock := &mockDiscoverer{
		instances: []discovery.Instance{
			{
				Group:      "group1",
				Replicaset: "rs1",
				Name:       "instance1",
			},
		},
	}
	cache := discoverer.NewCache(mock)

	result1, err1 := cache.Discovery(context.Background())
	require.NoError(t, err1)
	assert.Equal(t, 1, mock.calls)

	result2, err2 := cache.Discovery(context.Background())
	require.NoError(t, err2)
	assert.Equal(t, 1, mock.calls)

	assert.Equal(t, result1, result2)
}

func TestCache_Discovery_cachesBaseError(t *testing.T) {
	instances := []discovery.Instance{
		{Group: "group1", Replicaset: "rs1", Name: "instance1"},
		{Group: "group2", Replicaset: "rs2", Name: "instance2"},
	}
	mock := &mockDiscoverer{
		instances: instances,
		err:       errors.New("foo"),
	}
	cache := discoverer.NewCache(mock)

	result1, err1 := cache.Discovery(context.Background())
	require.Error(t, err1)
	assert.Equal(t, instances, result1)
	assert.Equal(t, 1, mock.calls)

	result2, err2 := cache.Discovery(context.Background())
	require.Error(t, err2)
	assert.Equal(t, instances, result2)
	assert.Equal(t, 1, mock.calls)

	assert.Equal(t, err1, err2)
}

func TestCache_Discovery_handlesNilBaseInstances(t *testing.T) {
	mock := &mockDiscoverer{instances: nil}
	cache := discoverer.NewCache(mock)

	result, err := cache.Discovery(context.Background())
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestCache_Discovery_respectsContextCancellationOnFirstCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mock := &mockDiscoverer{instances: []discovery.Instance{{}}}
	cache := discoverer.NewCache(mock)

	_, err := cache.Discovery(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}

func TestCache_Discovery_cachesCorrectNumberOfInstances(t *testing.T) {
	expected := []discovery.Instance{
		{Group: "group1", Replicaset: "rs1", Name: "instance1"},
		{Group: "group2", Replicaset: "rs2", Name: "instance2"},
	}
	mock := &mockDiscoverer{instances: expected}
	cache := discoverer.NewCache(mock)

	result, err := cache.Discovery(context.Background())
	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestCache_Discovery_passesContext(t *testing.T) {
	mock := &mockDiscoverer{
		instances: []discovery.Instance{
			{
				Group:      "group1",
				Replicaset: "rs1",
				Name:       "instance1",
			},
		},
	}
	cache := discoverer.NewCache(mock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := cache.Discovery(ctx)
	require.NoError(t, err)

	assert.True(t, ctx == mock.lastCtx)
}
