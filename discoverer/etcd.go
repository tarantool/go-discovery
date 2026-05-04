package discoverer

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tarantool/go-storage"
	etcdstorage "github.com/tarantool/go-storage/driver/etcd"
)

// EtcdClient is the interface required to create an etcd-backed storage.
type EtcdClient interface {
	// Txn creates a new transaction.
	Txn(ctx context.Context) clientv3.Txn
	// Watch watches for changes on a key.
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
}

// NewEtcd creates a new discoverer that retrieves instance configurations
// from etcd. The prefix must have the same value as config.etcd.prefix.
//
// If etcd is nil, the returned discoverer fails with [ErrMissingStorage] on
// any [Storage.Discovery] call.
func NewEtcd(etcd EtcdClient, prefix string) *Storage {
	if etcd == nil {
		return &Storage{prefix: prefix}
	}
	return NewStorage(storage.NewStorage(etcdstorage.New(etcd)), prefix)
}
