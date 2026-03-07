package discoverer

import (
	"context"

	"github.com/tarantool/go-discovery"
)

// Cache wraps a Discoverer and caches its result so that subsequent calls to
// Discovery return the cached result immediately. This implementation is not
// thread-safe.
type Cache struct {
	base    discovery.Discoverer
	cached  bool
	results []discovery.Instance
	err     error
}

// NewCache creates a new Cache that wraps the provided discoverer.
func NewCache(d discovery.Discoverer) *Cache {
	return &Cache{
		base: d,
	}
}

// Discovery returns a list of instance configurations for this environment or
// its cached result if available.
//
// Note: This method is not thread-safe. If called concurrently multiple
// times, it may call the underlying discoverer more than once.
func (c *Cache) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	if c.base == nil {
		return nil, ErrMissingDiscoverer
	}

	if !c.cached {
		c.results, c.err = c.base.Discovery(ctx)
		c.cached = true
	}

	return c.results, c.err
}
