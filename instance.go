package discovery

// Instance describes a single instance configuration.
type Instance struct {
	// Group is a name of the instance's group.
	Group string
	// Replicaset is a name of the instance's replicaset.
	Replicaset string
	// Name is a name of the instance.
	Name string
	// Mode is a current mode of the instance.
	Mode Mode
	// Endpoints is a set of available endpoints to connect to the instance.
	Endpoints []Endpoint
	// Roles is a set of the instance's roles.
	Roles []string
	// RolesTags is a set of string tags configured for roles by the
	// configuration path:
	// `roles_cfg.tags`
	//
	// https://github.com/tarantool/tarantool/blob/f58bfc97c627f1838696697339fb3e443c05767a/src/box/lua/config/instance_config.lua#L2194-L2197
	RolesTags []string
	// AppTags is a set of string tags configured for an instance application
	// by the configuration path:
	// `app.cfg.tags`
	//
	// https://github.com/tarantool/tarantool/blob/f58bfc97c627f1838696697339fb3e443c05767a/src/box/lua/config/instance_config.lua#L1637-L1643
	AppTags []string
}
