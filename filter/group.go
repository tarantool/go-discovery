package filter

import (
	"github.com/tarantool/go-discovery"
)

// GroupOneOf matches instances that have a group in the set.
type GroupOneOf struct {
	// Groups is a set of allowed groups.
	Groups []string
}

// Filter returns true if the instance has a group in the set.
func (f GroupOneOf) Filter(instance discovery.Instance) bool {
	return isValueInSet(instance.Group, f.Groups)
}
