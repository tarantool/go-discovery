package discoverer

import (
	"context"
	"errors"

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
	// prefix is the configuration prefix for TcS.
	prefix string
	// st is a storage instance backed by TcS.
	st storage.Storage
}

// ErrMissingTarantool for case of wrong initialization.
var ErrMissingTarantool = errors.New("no Tarantool connector was applied")

// NewTarantool create decorator with Discovery method.
func NewTarantool(conn TarantoolClient, prefix string) *Tarantool {
	var st storage.Storage
	if conn != nil {
		st = storage.NewStorage(tcsstorage.New(conn))
	}
	return &Tarantool{
		prefix: prefix,
		st:     st,
	}
}

// Discovery retrieves a list of instance configurations from the Storage.
func (t *Tarantool) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	if t.st == nil {
		return nil, ErrMissingTarantool
	}

	return discoverWithRetry(ctx, t.st, t.prefix, "tarantool")
}
