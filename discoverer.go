package discovery

import (
	"context"
)

// Discoverer is an interface for retrieving a list of instance configurations
// from a configuration source.
type Discoverer interface {
	// Discovery returns a list of instance configurations. In case of any
	// errors while trying to retrieve the list, it returns an error.
	Discovery(ctx context.Context) ([]Instance, error)
}
