package filter

import (
	"github.com/tarantool/go-discovery"
)

// ModeOneOf matches instances that have a mode in the set.
type ModeOneOf struct {
	// Modes is a set of allowed modes.
	Modes []discovery.Mode
}

// Filter returns true if the instance has a mode in the set.
func (f ModeOneOf) Filter(instance discovery.Instance) bool {
	return isValueInSet(instance.Mode, f.Modes)
}
