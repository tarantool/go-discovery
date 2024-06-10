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
	// URI is a set of available URIs to connect to the instance.
	URI []string
	// Roles is a set of the instance's roles.
	Roles []string
	// Labels is a map of string labels of the instance.
	Labels map[string]string
}
