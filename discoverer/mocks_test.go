package discoverer_test

import (
	"context"

	"github.com/tarantool/go-discovery"
)

type mockDiscoverer struct {
	instances []discovery.Instance
	err       error
	calls     int
	lastCtx   context.Context
}

func (d *mockDiscoverer) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	d.calls++

	d.lastCtx = ctx

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return d.instances, d.err
	}
}
