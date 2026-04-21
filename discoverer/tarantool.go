package discoverer

import (
	"context"
	"errors"
	"fmt"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-storage"
	tcsstorage "github.com/tarantool/go-storage/driver/tcs"
)

// TarantoolClient is the interface required to create a TCS-backed storage.
type TarantoolClient interface {
	tarantool.Doer
	NewWatcher(key string, callback tarantool.WatchCallback) (tarantool.Watcher, error)
}

// Tarantool discovers a list of instance configurations from TcS.
type Tarantool struct {
	// conn obtains information from TcS.
	conn TarantoolClient
	// prefix is the configuration prefix for TcS.
	prefix string
}

// ErrMissingTarantool for case of wrong initialization.
var ErrMissingTarantool = errors.New("no Tarantool connector was applied")

// NewTarantool create decorator with Discovery method.
func NewTarantool(conn TarantoolClient, prefix string) *Tarantool {
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

	st := storage.NewStorage(tcsstorage.New(t.conn))

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
		instances, err := buildInstances(subCtx, st, t.prefix)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// Too small timeout? We could retry until the main ctx is not
				// expired.
				continue
			}

			return nil, fmt.Errorf("failed to get data from tarantool: %w", err)
		}

		return instances, nil
	}
}
