package discoverer

import (
	"context"
	"errors"

	"github.com/tarantool/go-discovery/v2"
	"github.com/tarantool/go-storage"
)

// ErrMissingStorage is returned from Discovery when the discoverer was
// constructed without a backing storage (for example, when a nil client
// was passed to [NewEtcd] or [NewTarantool]).
var ErrMissingStorage = errors.New("storage is missing")

// Storage discovers a list of instance configurations from a storage.
type Storage struct {
	// st is a storage instance.
	st storage.Storage
	// prefix is a configuration prefix.
	prefix string
}

// NewStorage creates a new storage discoverer to retrieve a list
// of instance configurations from a storage.
func NewStorage(st storage.Storage, prefix string) *Storage {
	return &Storage{
		st:     st,
		prefix: prefix,
	}
}

// Discovery retrieves instance configurations from the storage.
func (d *Storage) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	if d.st == nil {
		return nil, ErrMissingStorage
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return buildInstances(ctx, d.st, d.prefix)
}
