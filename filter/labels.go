package filter

import (
	"github.com/tarantool/go-discovery"
)

// LabelsContain matches instances that have all application labels from the
// map with the same values.
type LabelsContain struct {
	// Labels is a set of application tags to pass the match.
	Labels map[string]string
}

// Filter returns true if the instance has all application tags from the set.
func (f LabelsContain) Filter(instance discovery.Instance) bool {
	for label, value := range f.Labels {
		if ivalue, ok := instance.Labels[label]; !ok || value != ivalue {
			return false
		}
	}
	return true
}
