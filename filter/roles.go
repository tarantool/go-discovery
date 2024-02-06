package filter

import (
	"github.com/tarantool/go-discovery"
)

// RolesContains matches instances that have all roles from the set.
type RolesContains struct {
	// Roles is a set of roles to pass the match.
	Roles []string
}

// Filter returns true if the instance has all roles from the set.
func (f RolesContains) Filter(instance discovery.Instance) bool {
	return isAllValuesInSet(f.Roles, instance.Roles)
}
