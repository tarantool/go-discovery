package discoverer

import (
	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-storage"
	tcsstorage "github.com/tarantool/go-storage/driver/tcs"
)

// TarantoolClient is the interface required to create a TCS-backed storage.
type TarantoolClient interface {
	tarantool.Doer
	NewWatcher(key string, callback tarantool.WatchCallback) (tarantool.Watcher, error)
}

// NewTarantool creates a new discoverer that retrieves instance
// configurations from a Tarantool Centralized Storage.
//
// If conn is nil, the returned discoverer fails with [ErrMissingStorage] on
// any [Storage.Discovery] call.
func NewTarantool(conn TarantoolClient, prefix string) *Storage {
	if conn == nil {
		return &Storage{prefix: prefix}
	}
	return NewStorageDiscoverer(storage.NewStorage(tcsstorage.New(conn)), prefix)
}
