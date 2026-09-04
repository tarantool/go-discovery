package discovery

// ShardingRole is a sharding role of an instance.
type ShardingRole string

const (
	// ShardingRoleRouter defines an instance that routes requests to
	// storages.
	ShardingRoleRouter ShardingRole = "router"
	// ShardingRoleStorage defines an instance that stores a part of
	// the data.
	ShardingRoleStorage ShardingRole = "storage"
	// ShardingRoleRebalancer defines an instance that balances the
	// data between storages.
	ShardingRoleRebalancer ShardingRole = "rebalancer"
)
