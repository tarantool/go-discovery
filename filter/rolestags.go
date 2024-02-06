package filter

import (
	"github.com/tarantool/go-discovery"
)

// RolesTagsContains matches instances that have all role tags from the set.
type RolesTagsContains struct {
	// RolesTags is a set of role tags to pass the match.
	RolesTags []string
}

// Filter returns true if the instance has all role tags from the set.
func (f RolesTagsContains) Filter(instance discovery.Instance) bool {
	return isAllValuesInSet(f.RolesTags, instance.RolesTags)
}
