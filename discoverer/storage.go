package discoverer

import (
	"context"
	"errors"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-storage"
)

var (
	// ErrTypedStorageNil is returned when typed storage is nil.
	ErrTypedStorageNil = errors.New("typed storage cannot be nil")
)

// Storage discovers a list of instance configurations from a storage.
type Storage struct {
	// st is a storage instance.
	st storage.Storage
	// prefix is a configuration prefix.
	prefix string
}

// NewStorageDiscoverer creates a new storage discoverer to retrieve a list
// of instance configurations from a storage.
func NewStorageDiscoverer(st storage.Storage, prefix string) *Storage {
	return &Storage{
		st:     st,
		prefix: prefix,
	}
}

// Discovery retrieves instance configurations from the storage.
func (d *Storage) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	if d.st == nil {
		return nil, ErrTypedStorageNil
	}

	instances, err := buildInstances(ctx, d.st, d.prefix)
	if err != nil {
		return nil, err
	}

	return instances, nil
}
