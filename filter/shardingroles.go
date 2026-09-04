package filter

import (
	"github.com/tarantool/go-discovery/v3"
)

// ShardingRolesContain matches instances that have all sharding roles
// from the set.
type ShardingRolesContain struct {
	// ShardingRoles is a set of sharding roles to pass the match.
	Roles []discovery.ShardingRole
}

// Filter returns true if the instance has all sharding roles from the
// set.
func (f ShardingRolesContain) Filter(instance discovery.Instance) bool {
	return isAllValuesInSet(f.Roles, instance.ShardingRoles)
}
