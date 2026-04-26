package discoverer

import (
	"context"
	"fmt"

	"github.com/tarantool/go-discovery"
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

// Etcd discovers a list of instance configurations from etcd.
type Etcd struct {
	// prefix is the configuration prefix for etcd.
	prefix string
	// st is a storage instance backed by etcd.
	st storage.Storage
}

// ErrMissingEtcd is an error that tells that the provided etcd object is nil.
var ErrMissingEtcd = fmt.Errorf("etcd object is missing")

// NewEtcd creates a new etcd discoverer to retrieve a list of instance
// configurations from etcd.
//
// The prefix must have the same value as config.etcd.prefix.
func NewEtcd(etcd EtcdClient, prefix string) *Etcd {
	var st storage.Storage
	if etcd != nil {
		st = storage.NewStorage(etcdstorage.New(etcd))
	}
	return &Etcd{
		prefix: prefix,
		st:     st,
	}
}

// Discovery retrieves a list of instance configurations from etcd.
func (d *Etcd) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	if d.st == nil {
		return nil, ErrMissingEtcd
	}

	return discoverWithRetry(ctx, d.st, d.prefix, "etcd")
}
