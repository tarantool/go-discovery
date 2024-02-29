// Package pool implements a pool of connections for Tarantool 3.0.
package pool

import "github.com/tarantool/go-discovery"

type Balancer interface {
	// Add adds an instance to balancer. Returns false if an instance was not added.
	Add(instance discovery.Instance) error
	// Remove removes an instance from the balancer by name.
	Remove(instanceName string)
	// Next return a next instance according to the balancer logic.
	Next(mode discovery.Mode) (string, bool)
}
