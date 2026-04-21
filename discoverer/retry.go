package discoverer

import (
	"context"
	"errors"
	"fmt"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-storage"
)

// discoverWithRetry calls buildInstances in a loop with timeout-based retries.
func discoverWithRetry(
	ctx context.Context,
	st storage.Storage,
	prefix string,
	name string,
) ([]discovery.Instance, error) {
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
		instances, err := buildInstances(subCtx, st, prefix)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			return nil, fmt.Errorf("failed to get data from %s: %w", name, err)
		}
		return instances, nil
	}
}
