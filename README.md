# discovery

Package discovery implements cluster discovery helpers for Tarantool 3.0
implemented in Go language according to the [design document][design-document].

The main example demonstrates how to create a pool of connections based
on a cluster configuration in etcd.

```Go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/discoverer"
	"github.com/tarantool/go-discovery/filter"
	"github.com/tarantool/go-discovery/pool"
	"github.com/tarantool/go-discovery/scheduler"
	"github.com/tarantool/go-discovery/subscriber"
)

func main() {
	// We need to create a etcd client. The client will use a default port
	// for a local etcd instance. You could change the settings.
	etcd, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
	})
	if err != nil {
		fmt.Println("Unable to start etcd client:", err)
		return
	}
	defer etcd.Close()

	// The pool will try to connect to instances without TLS. It will send
	// requests in round-robin. But you could use pool.PriorityBalancer to
	// send requests by instances priority.
	examplePool, err := pool.NewPool(
		pool.NewNetDialerFactory("testuser", "testpass", tarantool.Opts{
			Timeout: 5 * time.Second,
		}),
		pool.NewRoundRobinBalancer(),
	)
	if err != nil {
		fmt.Println("Unable to create a pool:", err)
		return
	}

	for {
		// The scheduler will watch for updates from etcd.
		sched := scheduler.NewEtcdWatch(etcd, "/prefix")
		defer sched.Stop()

		disc := discoverer.NewFilter(
			// The base discoverer gets a list of instance configurations from
			// etcd.
			discoverer.NewEtcd(etcd, "/prefix"),
			// The filter filters the list and leaves only instances with
			// Group == "foo".
			filter.GroupOneOf{Groups: []string{"foo"}},
			// You can play with other filters here, see:
			// https://github.com/tarantool/go-discovery/filter/
			// subpackage.
		)

		// The Subscriber will send instance configurations into the pool
		// on updates.
		etcdSubscriber := subscriber.NewFilter(
			// The base subscriber watches for updates from etcd and generates
			// a list of update events from it.
			subscriber.NewSchedule(sched, disc),
			// The filter filters the list of updates and leaves only instances
			// with Name == "instance1".
			filter.NameOneOf{Names: []string{"instance1"}},
		)

		// Subscribe the pool for updates from the subscriber. Subscribtion
		// only to single subscriber at a moment is supported.
		err := etcdSubscriber.Subscribe(context.Background(), examplePool)
		if err != nil {
			fmt.Println("Failed to subscribe:", err)
			return
		}
		defer etcdSubscriber.Unsubscribe(examplePool)

		for {
			// Any request from go-tarantool could be used.
			request := tarantool.NewEvalRequest("return box.cfg.listen")
			// You could use discovery.ModeRW or discovery.ModeRO to send
			// requests to only RW or RO instances.
			result, err := examplePool.Do(request, discovery.ModeAny).Get()

			if errors.Is(err, pool.ErrUnsubscribed) {
				// Something happened with a scheduler (see log messages), we
				// need to recreate scheduler and subscribe to it again.
				time.Sleep(time.Second)
				fmt.Println("Unsubscribed from a scheduler.")
				break
			}

			if errors.Is(err, pool.ErrNoConnectedInstances) {
				// The pool is not connected to an instance yet. We could just
				// repeat the request. You need to make a more complex
				// business logic.
				fmt.Println("No instances.")
				time.Sleep(time.Second)
				continue
			}

			fmt.Println("Result:", result)
			fmt.Println("Error:", err)
			fmt.Println("Done.")
			return
		}
	}
}
```

# Run tests

To run default set of tests:

```shell
go test -v ./...
```

# Development

To generate code, you need to install two utilities:

```shell
go install golang.org/x/tools/cmd/stringer@latest
go install github.com/posener/goreadme/cmd/goreadme@latest
```

After that, you could generate the code.

```shell
go generate ./...
```

In addition, you need to install linter:

```shell
go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.51.1
```

You could run the linter with the command.

```shell
golangci-lint --config .golangci.yml run
```

Finally, codespell:

```shell
pip3 install codespell
codespell
```

You could also use our Makefile targets:

```shell
make deps
make format
make generate
make lint
make test
make testrace
```

# Documentation

You could run the `godoc` server on `localhost:6060` with the command:

```shell
make godoc_run
```

And open the generated documentation in another terminal or use the
[link][godoc-link]:

```shell
make godoc_open
```

# Examples

* [The main package](./example_test.go);
* [The discoverer subpackage](./discoverer/example_test.go);
* [The pool subpackage](./pool/example_test.go);
* [The scheduler subpackage](./scheduler/example_test.go);

[design-document]: https://www.notion.so/Cluster-discovery-Go-library-3613a0bd7e3a439d86f99c083d9d8ce4
[godoc-link]: http://localhost:6060/pkg/github.com/tarantool/go-discovery
