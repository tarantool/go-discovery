package discoverer

import (
	"context"
	"errors"
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
	// etcd obtains information from etcd.
	etcd EtcdClient
	// prefix is the configuration prefix for etcd.
	prefix string
}

// ErrMissingEtcd is an error that tells that the provided etcd object is nil.
var ErrMissingEtcd = fmt.Errorf("etcd object is missing")

// NewEtcd creates a new etcd discoverer to retrieve a list of instance
// configurations from etcd.
//
// The prefix must have the same value as config.etcd.prefix.
func NewEtcd(etcd EtcdClient, prefix string) *Etcd {
	return &Etcd{
		etcd:   etcd,
		prefix: prefix,
	}
}

// Discovery retrieves a list of instance configurations from etcd.
func (d *Etcd) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	if d.etcd == nil {
		return nil, ErrMissingEtcd
	}

	st := storage.NewStorage(etcdstorage.New(d.etcd))

	for {
		timeout, err := checkTimeout(ctx)
		if err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		subCtx, cancel := context.WithTimeout(ctx, timeout)
		instances, err := buildInstances(subCtx, st, d.prefix)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// Too small timeout? We could retry until the main ctx is not
				// expired.
				continue
			}

			return nil, fmt.Errorf("failed to get data from etcd: %w", err)
		}

		return instances, nil
	}
}
