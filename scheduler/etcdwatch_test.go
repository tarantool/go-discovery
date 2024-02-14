package scheduler_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/scheduler"
)

type mockWatcher struct {
	clientv3.Watcher
	response chan clientv3.WatchResponse
}

func newMockWatcher() *mockWatcher {
	return &mockWatcher{response: make(chan clientv3.WatchResponse, 1)}
}

func (w *mockWatcher) Watch(_ context.Context, _ string,
	_ ...clientv3.OpOption) clientv3.WatchChan {
	return w.response
}

func TestEtcdWatch_Wait(t *testing.T) {
	watcher := newMockWatcher()
	watcher.response <- clientv3.WatchResponse{
		Events: make([]*clientv3.Event, 1),
	}

	scheduler := scheduler.NewEtcdWatch(watcher, "")
	ctx := context.Background()

	err := scheduler.Wait(ctx)
	assert.NoError(t, err)
}

func TestEtcdWatch_ConcurrentStop(t *testing.T) {
	for i := 0; i < 100000; i++ {
		go func() {
			var err error
			wg := sync.WaitGroup{}

			scheduler := scheduler.NewEtcdWatch(&mockWatcher{}, "")
			ctx := context.Background()

			wg.Add(2)

			go func() {
				err = scheduler.Wait(ctx)
				wg.Done()
			}()

			go func() {
				scheduler.Stop()
				wg.Done()
			}()

			wg.Wait()
			assert.Equal(t, discovery.ErrSchedulerStopped, err)
		}()
	}
}

func TestEtcdWatch_ConcurrentContextCancel(t *testing.T) {
	for i := 0; i < 100000; i++ {
		go func() {
			var err error
			wg := sync.WaitGroup{}

			scheduler := scheduler.NewEtcdWatch(&mockWatcher{}, "")
			ctx, cancel := context.WithCancel(context.Background())

			wg.Add(2)

			go func() {
				err = scheduler.Wait(ctx)
				wg.Done()
			}()

			go func() {
				cancel()
				wg.Done()
			}()

			wg.Wait()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "context canceled")
		}()
	}
}

func TestEtcdWatch_WaitAfterStop(t *testing.T) {
	scheduler := scheduler.NewEtcdWatch(&mockWatcher{}, "")
	ctx := context.Background()

	scheduler.Stop()

	err := scheduler.Wait(ctx)
	assert.Equal(t, discovery.ErrSchedulerStopped, err)
}

func TestEtcdWatch_WaitStopCancelCtx(t *testing.T) {
	scheduler := scheduler.NewEtcdWatch(&mockWatcher{}, "")
	ctx, cancel := context.WithCancel(context.Background())

	cancel()
	scheduler.Stop()

	err := scheduler.Wait(ctx)
	assert.Equal(t, discovery.ErrSchedulerStopped, err)
}

func TestEtcdWatch_ConcurrentWatcherClose_Stop(t *testing.T) {
	for i := 0; i < 100000; i++ {
		go func() {
			var err error

			watcher := newMockWatcher()
			// Event field size is 0 means that the watcher is closed.
			watcher.response <- clientv3.WatchResponse{}

			scheduler := scheduler.NewEtcdWatch(watcher, "")
			ctx := context.Background()

			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				err = scheduler.Wait(ctx)
				wg.Done()
			}()

			go func() {
				scheduler.Stop()
				wg.Done()
			}()

			wg.Wait()
			assert.Equal(t, discovery.ErrSchedulerStopped, err)
		}()
	}
}
