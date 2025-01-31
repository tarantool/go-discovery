package discoverer

import (
	"context"
	"errors"
	"fmt"

	"github.com/tarantool/tt/lib/cluster"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tarantool/go-discovery"
)

// Etcd discovers a list of instance configurations from etcd.
type Etcd struct {
	// etcd obtains information from etcd.
	etcd clientv3.KV
	// The configuration prefix for etcd.
	prefix string
}

// ErrMissingEtcd is an error that tells that the provided etcd object is nil.
var ErrMissingEtcd = fmt.Errorf("etcd object is missing")

// NewEtcd creates a new etcd discoverer to retrieve a list of instance
// configurations from etcd.
//
// The prefix must have the same value as config.etcd.prefix.
func NewEtcd(etcd clientv3.KV, prefix string) *Etcd {
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

		dataCollector := cluster.NewEtcdAllCollector(d.etcd, d.prefix, timeout)
		collector := cluster.NewYamlCollectorDecorator(dataCollector)
		config, err := collector.Collect()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// Too small timeout? We could retry until the main ctx is not
				// expired.
				continue
			}

			var noConfig cluster.CollectEmptyError
			if errors.As(err, &noConfig) {
				return nil, nil
			}

			return nil, fmt.Errorf("failed to get data from etcd: %w", err)
		}

		return parseConfig(config)
	}
}
