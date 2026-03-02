package discoverer

import (
	"context"
	"errors"
	"strings"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/integrity"
	"github.com/tarantool/tt/lib/cluster"
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
	// typed is a typed storage for cluster configurations.
	typed *integrity.Typed[cluster.ClusterConfig]
}

// NewStorageDiscoverer creates a new storage discoverer to retrieve a list
// of instance configurations from a storage.
func NewStorageDiscoverer(s storage.Storage, prefix string) *Storage {
	typed := integrity.NewTypedBuilder[cluster.ClusterConfig](s).
		WithPrefix(strings.TrimRight(prefix, "/")).
		Build()

	return &Storage{
		typed: typed,
	}
}

// Discovery retrieves instance configurations from the storage.
func (d *Storage) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	if d.typed == nil {
		return nil, ErrTypedStorageNil
	}

	results, err := d.typed.Range(ctx, "config/")
	if err != nil {
		return nil, errors.Join(ErrRangeDataFailed, err)
	}

	var allInstances []discovery.Instance
	for _, result := range results {
		if result.Error != nil {
			return nil, errors.Join(ErrValidateDataFailed, result.Error)
		}

		cc, ok := result.Value.Get()
		if !ok {
			continue
		}

		instances, err := convertClusterConfig(cc)
		if err != nil {
			return nil, errors.Join(ErrParseConfigFailed, err)
		}
		allInstances = append(allInstances, instances...)
	}

	return allInstances, nil
}
