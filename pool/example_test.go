package pool_test

import (
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/pool"
)

// ExampleRoundRobinBalancer demonstrates how to create a Pool object with
// round-robin balancer. It sends requests to instances with round-robin
// strategy.
func ExampleRoundRobinBalancer() {
	balancer := pool.NewRoundRobinBalancer()
	dialerFactory := pool.NewNetDialerFactory("user", "pass", tarantool.Opts{
		Timeout: 5 * time.Second,
	})
	examplePool, err := pool.NewPool(dialerFactory, balancer)
	if err != nil {
		fmt.Println("Failed to create a pool: %w", err)
	}

	_, err = examplePool.Do(tarantool.NewPingRequest(), discovery.ModeAny).Get()
	// The pool is not subscribed to any configuration source yet so it will
	// return an error, see examples in the main package.
	fmt.Println(err, ".")

	// Output:
	// pool is not subscribed yet .
}

// ExamplePriorityBalancer demonstrates how to create a Pool object with
// priority balancer. The balancer sends requests to instances with a higher
// priority in round-robin until it exists.
func ExamplePriorityBalancer() {
	balancer := pool.NewPriorityBalancer(func(instance discovery.Instance) int {
		if instance.Replicaset == "datacenter-1" {
			// The balancer will send requests to replicaset "datacenter-1"
			// while there are any connected instance with the mode.
			return 20
		}
		if instance.Group == "Moscow" {
			// After that it will choose instances with Group == "Moscow".
			return 10
		}
		// It will send requests to other instances if instances with
		// Group == "Moscow" and Replicaset == "datacenter-1"
		// are not available for a mode.
		return 0
	})
	dialerFactory := pool.NewNetDialerFactory("user", "pass", tarantool.Opts{
		Timeout: 5 * time.Second,
	})

	examplePool, err := pool.NewPool(dialerFactory, balancer)
	if err != nil {
		fmt.Println("Failed to create a pool: %w", err)
	}

	_, err = examplePool.Do(tarantool.NewPingRequest(), discovery.ModeAny).Get()
	// The pool is not subscribed to any configuration source yet so it will
	// return an error, see examples in the main package.
	fmt.Println(err, ".")

	// Output:
	// pool is not subscribed yet .
}

// ExampleModeAdapter demonstrates how to adapt a pool.Pool object to the
// tarantool.Doer interface and send request with specific mode to the pool.
func ExampleDoerAdapter() {
	balancer := pool.NewRoundRobinBalancer()
	dialerFactory := pool.NewNetDialerFactory("user", "pass", tarantool.Opts{
		Timeout: 5 * time.Second,
	})
	examplePool, err := pool.NewPool(dialerFactory, balancer)
	if err != nil {
		fmt.Println("Failed to create a pool: %w", err)
	}

	doer := tarantool.Doer(pool.NewDoerAdapter(examplePool, discovery.ModeRW))

	_, err = doer.Do(tarantool.NewPingRequest()).Get()
	// The pool is not subscribed to any configuration source yet so it will
	// return an error, see examples in the main package.
	fmt.Println(err, ".")

	// Output:
	// pool is not subscribed yet .
}
