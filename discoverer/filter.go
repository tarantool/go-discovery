package discoverer

import (
	"context"
	"fmt"

	"github.com/tarantool/go-discovery"
)

// Filter filters the list of discovered nodes by some set of filters.
// It serves as a wrapper to another Discoverer.
type Filter struct {
	// discoverer is an inner Discoverer.
	discoverer discovery.Discoverer
	filters    []discovery.Filter
}

// ErrMissingDiscoverer is an error that tells that the provided inner
// discoverer is nil.
var ErrMissingDiscoverer = fmt.Errorf("an inner discoverer is missing")

// NewFilter creates a new Filter discoverer.
// It wraps the inner discoverer to filter its results according to
// passed filters.
func NewFilter(discoverer discovery.Discoverer,
	filters ...discovery.Filter) (*Filter, error) {
	if discoverer == nil {
		return nil, ErrMissingDiscoverer
	}

	return &Filter{
		discoverer: discoverer,
		filters:    filters,
	}, nil
}

// Discovery calls Discovery of an inner discoverer and returns a filtered
// list of nodes.
//
// Note that the context is only used in the inner discoverer.Discovery call.
// If the program is stuck on one of the Filter calls, context cancel would
// not cancel the method.
func (d *Filter) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	instances, err := d.discoverer.Discovery(ctx)
	if err != nil {
		return instances, err
	}

	var filteredInstances []discovery.Instance

	for _, inst := range instances {
		keepInstance := true

		for _, filter := range d.filters {
			if !filter.Filter(inst) {
				keepInstance = false
				break
			}
		}

		if keepInstance {
			filteredInstances = append(filteredInstances, inst)
		}
	}

	return filteredInstances, nil
}
