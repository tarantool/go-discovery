package discoverer

import (
	"context"
	"fmt"
	"strings"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/integrity"
	"github.com/tarantool/tt/lib/cluster"
)

// Storage discovers a list of instance configurations from a storage.
type (
	Storage struct {
		// typed is a typed storage for cluster configurations.
		typed *integrity.Typed[cluster.ClusterConfig]
	}
)

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
		return nil, fmt.Errorf("typed storage cannot be nil")
	}

	results, err := d.typed.Range(ctx, "config/")
	if err != nil {
		return nil, fmt.Errorf("failed to range data from storage: %w", err)
	}

	var allInstances []discovery.Instance
	for _, result := range results {
		if result.Error != nil {
			return nil, fmt.Errorf("failed to validate data for %s: %w", result.Name, result.Error)
		}

		cc, ok := result.Value.Get()
		if !ok {
			continue
		}

		instances, err := convertClusterConfig(cc)
		if err != nil {
			return nil, fmt.Errorf("failed to parse configuration for %s: %w", result.Name, err)
		}
		allInstances = append(allInstances, instances...)
	}

	return allInstances, nil
}
