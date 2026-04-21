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
	// ErrRangeDataFailed is returned when range operation on storage fails.
	ErrRangeDataFailed = errors.New("failed to range data from storage")
	// ErrValidateDataFailed is returned when data validation fails.
	ErrValidateDataFailed = errors.New("failed to validate data")
	// ErrParseConfigFailed is returned when configuration parsing fails.
	ErrParseConfigFailed = errors.New("failed to parse configuration")
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
		return nil, errors.Join(ErrParseConfigFailed, err)
	}

	return instances, nil
}
