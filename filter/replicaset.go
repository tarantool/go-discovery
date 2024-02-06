package filter

import (
	"github.com/tarantool/go-discovery"
)

// ReplicasetOneOf matches instances that have a replicaset in the set.
type ReplicasetOneOf struct {
	// Replicasets is a set of allowed replicasets.
	Replicasets []string
}

// Filter returns true if the instance has a replicaset in the set.
func (f ReplicasetOneOf) Filter(instance discovery.Instance) bool {
	return isValueInSet(instance.Replicaset, f.Replicasets)
}
