package scheduler

import (
	"context"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tarantool/go-discovery"
)

// EtcdWatch generates an update event when the configuration
// is updated in etcd. Configuration is searched by the prefix.
//
// Note that the package user configures the connection to etcd himself.
type EtcdWatch struct {
	watcher   clientv3.Watcher
	prefix    string
	watchChan clientv3.WatchChan
	cancel    context.CancelFunc
	done      chan struct{}
	stopOnce  sync.Once
}

// NewEtcdWatch creates an EtcdWatch scheduler with given watcher for
// the given prefix.
//
// watcher is a watcher for etcd events from the etcd package.
// For example, clientv3.Client implements a clientv3.Watcher interface.
func NewEtcdWatch(watcher clientv3.Watcher,
	prefix string) *EtcdWatch {
	ctx, cancel := context.WithCancel(context.Background())
	return &EtcdWatch{
		watcher:   watcher,
		prefix:    prefix,
		watchChan: watcher.Watch(ctx, prefix, clientv3.WithPrefix()),
		cancel:    cancel,
		done:      make(chan struct{}),
		stopOnce:  sync.Once{},
	}
}

// Wait allows waiting until the configuration in etcd is updated.
func (s *EtcdWatch) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		return discovery.ErrSchedulerStopped
	default:
		select {
		case <-s.done:
			return discovery.ErrSchedulerStopped
		case <-ctx.Done():
			s.cancel()
			return ctx.Err()
		case resp := <-s.watchChan:
			err := resp.Err()

			if err != nil || (err == nil && len(resp.Events) == 0) {
				// "WatchResponse" from this closed channel has zero events
				// and nil "Err()".
				s.Stop()
				return discovery.ErrSchedulerStopped
			}
			return nil
		}
	}
}

// Stop stops the watcher and event generation.
func (s *EtcdWatch) Stop() {
	s.stopOnce.Do(func() {
		close(s.done)
		s.cancel()
	})
}
