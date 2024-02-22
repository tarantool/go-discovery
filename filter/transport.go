package filter

import (
	"github.com/tarantool/go-discovery"
)

// TransportAnyOf matches instances that have an Endpoint with a transport
// type in the set.
type TransportAnyOf struct {
	// Transports is a set of allowed transport types.
	Transports []discovery.Transport
}

// Filter returns true if the instance has an endpoint with transport type in
// the set.
func (f TransportAnyOf) Filter(instance discovery.Instance) bool {
	for _, endpoint := range instance.Endpoints {
		for _, target := range f.Transports {
			if endpoint.Transport == target {
				return true
			}
		}
	}
	return false
}
