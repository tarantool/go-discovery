package discovery_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/dial"
	"github.com/tarantool/go-discovery/discoverer"
	"github.com/tarantool/go-discovery/filter"
	"github.com/tarantool/go-discovery/scheduler"
	"github.com/tarantool/go-discovery/subscriber"
)

func TestEtcdWatchScheduler_Etcd_Wait(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		success bool
	}{
		{
			name:    "Full key",
			key:     "key",
			success: true,
		},
		{
			name:    "Prefix",
			key:     "key_prefix",
			success: true,
		},
		{
			name:    "Wrong key",
			key:     "no_prefix_key",
			success: false,
		},
	}

	cluster := integration.NewLazyCluster()
	defer cluster.Terminate()

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: cluster.EndpointsV3(),
	})

	require.NoError(t, err)
	require.NotNil(t, etcd)
	defer etcd.Close()

	scheduler := scheduler.NewEtcdWatch(etcd, "key")

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			wg := sync.WaitGroup{}

			wg.Add(1)
			go func() {
				err = scheduler.Wait(ctx)
				if tc.success {
					assert.NoError(t, err)
				} else {
					assert.Error(t, discovery.ErrSchedulerStopped, err)
				}
				wg.Done()
			}()

			_, err := etcd.Put(ctx, tc.key, "value")
			require.NoError(t, err)

			if !tc.success {
				scheduler.Stop()
			}
			wg.Wait()
		})
	}
}

func TestEtcdWatchScheduler_Etcd_Stop(t *testing.T) {
	cluster := integration.NewLazyCluster()
	defer cluster.Terminate()

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: cluster.EndpointsV3(),
	})
	require.NoError(t, err)
	require.NotNil(t, etcd)
	defer etcd.Close()

	scheduler := scheduler.NewEtcdWatch(etcd, "key")

	ctx := context.Background()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err = scheduler.Wait(ctx)
		assert.Equal(t, discovery.ErrSchedulerStopped, err)
		wg.Done()
	}()

	scheduler.Stop()
	wg.Wait()
}

func TestEtcdWatchScheduler_Etcd_CloseStop(t *testing.T) {
	cluster := integration.NewLazyCluster()
	defer cluster.Terminate()

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: cluster.EndpointsV3(),
	})
	require.NoError(t, err)
	require.NotNil(t, etcd)

	scheduler := scheduler.NewEtcdWatch(etcd, "key")

	ctx := context.Background()

	etcd.Close()

	err = scheduler.Wait(ctx)
	assert.Equal(t, discovery.ErrSchedulerStopped, err)
}

func TestEtcdWatchScheduler_and_EtcdDiscoverer(t *testing.T) {
	cluster := integration.NewLazyCluster()
	defer cluster.Terminate()

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: cluster.EndpointsV3(),
	})
	require.NoError(t, err)
	require.NotNil(t, etcd)
	defer etcd.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	var (
		instances []discovery.Instance
	)

	scheduler := scheduler.NewEtcdWatch(etcd, "/prefix/")
	defer scheduler.Stop()
	disc := discoverer.NewEtcd(etcd, "/prefix/")
	go func() {
		defer wg.Done()

		err := scheduler.Wait(context.Background())
		require.NoError(t, err)

		instances, err = disc.Discovery(context.Background())
		require.NoError(t, err)
	}()

	_, err = etcd.Put(context.Background(), "/prefix/config/foo", `
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo: {}
  zoo:
    replicasets:
      any:
        instances:
          foo: {}
`)
	require.NoError(t, err)

	wg.Wait()

	assert.ElementsMatch(t, []discovery.Instance{
		discovery.Instance{
			Group:      "foo",
			Replicaset: "bar",
			Name:       "zoo",
			Mode:       discovery.ModeRW,
		},
		discovery.Instance{
			Group:      "zoo",
			Replicaset: "any",
			Name:       "foo",
			Mode:       discovery.ModeRW,
		},
	}, instances)
}

func TestDiscoverer_Etcd_and_Filter(t *testing.T) {
	cluster := integration.NewLazyCluster()
	defer cluster.Terminate()

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: cluster.EndpointsV3(),
	})
	require.NoError(t, err)
	require.NotNil(t, etcd)
	defer etcd.Close()

	_, err = etcd.Put(context.Background(), "/prefix/config/foo", `
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo: {}
  zoo:
    replicasets:
      any:
        instances:
          foo: {}
`)
	require.NoError(t, err)

	disc := discoverer.NewFilter(discoverer.NewEtcd(etcd, "/prefix"),
		filter.NameOneOf{Names: []string{"foo"}})

	instances, err := disc.Discovery(context.Background())
	require.NoError(t, err)

	assert.ElementsMatch(t, []discovery.Instance{
		discovery.Instance{
			Group:      "zoo",
			Replicaset: "any",
			Name:       "foo",
			Mode:       discovery.ModeRW,
		},
	}, instances)
}

func TestDiscoverer_Etcd_Connectable(t *testing.T) {
	defer stopTarantool(startTarantool(t))

	cluster := integration.NewLazyCluster()
	defer cluster.Terminate()

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: cluster.EndpointsV3(),
	})
	require.NoError(t, err)
	require.NotNil(t, etcd)
	defer etcd.Close()

	_, err = etcd.Put(context.Background(), "/prefix/config/foo", `
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            iproto:
              advertise:
                client: 127.0.0.1:3013
  zoo:
    replicasets:
      any:
        instances:
          foo: {}
`)
	require.NoError(t, err)

	factory := dial.NewNetDialerFactory(ttUsername, ttPassword, opts)

	disc := discoverer.NewConnectable(factory,
		discoverer.NewEtcd(etcd, "/prefix"))

	inst, err := disc.Discovery(context.Background())
	assert.NoError(t, err)

	assert.NotNil(t, inst)
	assert.Equal(t, 1, len(inst))
	assert.Equal(t, discovery.Instance{
		Group:      "foo",
		Replicaset: "bar",
		Name:       "zoo",
		Mode:       discovery.ModeRW,
		URI:        []string{"127.0.0.1:3013"},
	}, inst[0])
}

type mockObserver struct {
	eventCnt     atomic.Int32
	recentEvents atomic.Pointer[[]discovery.Event]

	errCnt    atomic.Int32
	recentErr error

	wgEvent sync.WaitGroup
}

func newMockObserver() *mockObserver {
	obs := &mockObserver{}
	obs.recentEvents.Store(&[]discovery.Event{})
	obs.wgEvent = sync.WaitGroup{}
	return obs
}

func (o *mockObserver) Observe(events []discovery.Event, err error) {
	if len(events) > 0 || err == nil {
		o.eventCnt.Add(1)
		o.recentEvents.Store(&events)
		o.wgEvent.Done()
	}
	if err != nil {
		o.errCnt.Add(1)
		o.recentErr = err
	}
}

func TestSubscriber_Etcd_Connectable(t *testing.T) {
	defer stopTarantool(startTarantool(t))

	cluster := integration.NewLazyCluster()
	defer cluster.Terminate()

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: cluster.EndpointsV3(),
	})
	require.NoError(t, err)
	require.NotNil(t, etcd)
	defer etcd.Close()

	connectable := subscriber.NewConnectable(
		dial.NewNetDialerFactory(ttUsername, ttPassword, opts),
		subscriber.NewSchedule(scheduler.NewEtcdWatch(etcd, "foo"),
			discoverer.NewEtcd(etcd, "foo")))

	obs := newMockObserver()
	obs.wgEvent.Add(1)

	err = connectable.Subscribe(context.Background(), obs)
	require.NoError(t, err)
	defer connectable.Unsubscribe(obs)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = etcd.Put(ctx, "foo/config/key", `
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            iproto:
              listen:
              - uri: :0
              - uri: 127.0.0.1:3013
  zoo:
    replicasets:
      any:
        instances:
          foo: {}
`)
	cancel()
	require.NoError(t, err)

	obs.wgEvent.Wait()

	assert.Equal(t, int32(1), obs.eventCnt.Load())
	assert.Equal(t, []discovery.Event{
		{
			Type: discovery.EventTypeAdd,
			New: discovery.Instance{
				Group:      "foo",
				Replicaset: "bar",
				Name:       "zoo",
				Mode:       discovery.ModeRW,
				URI:        []string{":0", "127.0.0.1:3013"},
			},
		},
	}, *obs.recentEvents.Load())

	obs.wgEvent.Add(1)
	connectable.Unsubscribe(obs)

	assert.Equal(t, int32(2), obs.eventCnt.Load())
	assert.Equal(t, []discovery.Event{
		{
			Type: discovery.EventTypeRemove,
			Old: discovery.Instance{
				Group:      "foo",
				Replicaset: "bar",
				Name:       "zoo",
				Mode:       discovery.ModeRW,
				URI:        []string{":0", "127.0.0.1:3013"},
			},
		},
	}, *obs.recentEvents.Load())

	assert.Equal(t, int32(1), obs.errCnt.Load())
	assert.Equal(t, discovery.ErrUnsubscribe, obs.recentErr)
}
