package filter

import (
	"github.com/tarantool/go-discovery"
)

// URIAnyOf matches instances that have a URI in the set.
type URIAnyOf struct {
	// URIs is a set of allowed URIs.
	URIs []string
}

// Filter returns true if the instance has a URI in the set.
func (f URIAnyOf) Filter(instance discovery.Instance) bool {
	for _, endpoint := range instance.Endpoints {
		for _, target := range f.URIs {
			if endpoint.URI == target {
				return true
			}
		}
	}
	return false
}
