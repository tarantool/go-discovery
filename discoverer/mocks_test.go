package discoverer_test

import (
	"context"

	"github.com/tarantool/go-discovery"
)

type mockDiscoverer struct {
	instances []discovery.Instance
}

func (d *mockDiscoverer) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return d.instances, nil
	}
}
