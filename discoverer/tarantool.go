package discoverer

import (
	"context"
	"errors"
	"fmt"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/tt/lib/cluster"
)

// Tarantool discovers a list of instance configurations from TcS.
type Tarantool struct {
	conn   tarantool.Doer
	prefix string
}

// ErrMissingTarantool for case of wrong initialization.
var ErrMissingTarantool = errors.New("no Tarantool connector was applied")

// NewTarantool create decorator with Discovery method.
func NewTarantool(conn tarantool.Doer, prefix string) *Tarantool {
	return &Tarantool{
		conn:   conn,
		prefix: prefix,
	}
}

// Discovery retrieves a list of instance configurations from the Storage.
func (t *Tarantool) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	if t.conn == nil {
		return nil, ErrMissingTarantool
	}

	for {
		timeout, err := checkTimeout(ctx)
		if err != nil {
			return nil, err
		}

		dataCollector := cluster.NewTarantoolAllCollector(t.conn, t.prefix, timeout)
		collector := cluster.NewYamlCollectorDecorator(dataCollector)
		config, err := collector.Collect()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// Too small timeout? We could retry until the main ctx is not expired.
				continue
			}
			var noConfig cluster.CollectEmptyError
			if errors.As(err, &noConfig) {
				return nil, nil
			}

			return nil, fmt.Errorf("failed to get data from tarantool: %w", err)
		}

		return parseConfig(config)
	}
}
