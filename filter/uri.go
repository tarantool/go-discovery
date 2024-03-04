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
	return isAnyValueInSet(instance.URI, f.URIs)
}
