package filter

import (
	"github.com/tarantool/go-discovery"
)

// NameOneOf matches instances that have a name in the set.
type NameOneOf struct {
	// Names is a set of allowed names.
	Names []string
}

// Filter returns true if the instance has a name in the set.
func (f NameOneOf) Filter(instance discovery.Instance) bool {
	return isValueInSet(instance.Name, f.Names)
}
