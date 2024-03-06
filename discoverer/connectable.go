package discoverer

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/tarantool/go-tarantool/v2"
	"golang.org/x/exp/slices"

	"github.com/tarantool/go-discovery"
)

// Connectable gets a list of nodes from the inner discoverer and returns
// only those that are available for connection.
type Connectable struct {
	factory    discovery.DialerFactory
	discoverer discovery.Discoverer
}

// ErrMissingFactory is an error that tells that the provided
// DialerFactory is nil.
var ErrMissingFactory = fmt.Errorf("a dialer factory is missing")

// NewConnectable creates a Connectable with a given dialer factory and
// an inner discoverer.
func NewConnectable(factory discovery.DialerFactory,
	discoverer discovery.Discoverer) *Connectable {
	return &Connectable{
		factory:    factory,
		discoverer: discoverer,
	}
}

func checkRunning(ctx context.Context, factory discovery.DialerFactory,
	instance discovery.Instance) (discovery.Instance, bool) {
	dialer, opts, err := factory.NewDialer(instance)
	if err != nil {
		return instance, false
	}

	conn, err := tarantool.Connect(ctx, dialer, opts)
	if err != nil {
		return instance, false
	}
	defer conn.Close()

	var data []struct {
		Ro     bool   `msgpack:"is_ro"`
		Status string `msgpack:"status"`
	}

	err = conn.Do(tarantool.NewWatchOnceRequest("box.status").
		Context(ctx)).GetTyped(&data)
	if err != nil || len(data) == 0 {
		return instance, false
	}

	if data[0].Status != "running" {
		return instance, false
	}

	if data[0].Ro {
		instance.Mode = discovery.ModeRO
	} else {
		instance.Mode = discovery.ModeRW
	}

	return instance, true
}

func getConnectable(ctx context.Context, factory discovery.DialerFactory,
	instances []discovery.Instance) ([]discovery.Instance, error) {
	connectable := make([]discovery.Instance, len(instances))

	wg := sync.WaitGroup{}
	wg.Add(len(instances))

	for i, instance := range instances {
		go func(i int, instance discovery.Instance) {
			defer wg.Done()

			if instance, ok := checkRunning(ctx, factory, instance); ok {
				connectable[i] = instance
			}
		}(i, instance)
	}

	wg.Wait()
	connectable = slices.DeleteFunc(connectable, func(instance discovery.Instance) bool {
		return reflect.ValueOf(instance).IsZero()
	})

	select {
	case <-ctx.Done():
		return connectable, ctx.Err()
	default:
		return connectable, nil
	}
}

// Discovery calls d.discoverer.Discovery() and returns a list of nodes that are
// available for connection.
func (d *Connectable) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	if d.factory == nil {
		return nil, ErrMissingFactory
	}
	if d.discoverer == nil {
		return nil, ErrMissingDiscoverer
	}

	instances, err := d.discoverer.Discovery(ctx)
	if err != nil {
		return nil, err
	}

	return getConnectable(ctx, d.factory, instances)
}
