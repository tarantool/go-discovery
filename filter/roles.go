package filter

import (
	"github.com/tarantool/go-discovery"
)

// RolesContain matches instances that have all roles from the set.
type RolesContain struct {
	// Roles is a set of roles to pass the match.
	Roles []string
}

// Filter returns true if the instance has all roles from the set.
func (f RolesContain) Filter(instance discovery.Instance) bool {
	return isAllValuesInSet(f.Roles, instance.Roles)
}
