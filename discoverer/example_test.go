package discoverer_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"

	"github.com/tarantool/go-discovery/discoverer"
	tcshelper "github.com/tarantool/go-tarantool/v2/test_helpers/tcs"
)

func init() {
	log.SetOutput(io.Discard)
}

func ExampleEtcd_Discovery() {
	cluster := integration.NewLazyCluster()
	defer cluster.Terminate()

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: cluster.EndpointsV3(),
	})
	if err != nil {
		fmt.Println("Unable to start etcd client:", err)
		return
	}
	defer etcd.Close()

	etcddiscoverer := discoverer.NewEtcd(etcd, "foo")
	instances, err := etcddiscoverer.Discovery(context.Background())

	fmt.Println("Without keys in the prefix:")
	fmt.Println("Instances:", instances)
	fmt.Println("Error:", err)

	_, err = etcd.Put(context.Background(), "foo/config/key", `
database:
  mode: ro
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            iproto:
              advertise:
                client: localhost:3011
                peer:
                  params:
                    transport: ssl
            roles: [crud]
            labels:
              tags: "any,bar,3"
`)
	if err != nil {
		fmt.Println("Unable to put configuration into etcd:", err)
		return
	}

	instances, err = etcddiscoverer.Discovery(context.Background())

	fmt.Println("After publishing:")
	fmt.Println("Instances")
	for _, instance := range instances {
		fmt.Println("Group:", instance.Group)
		fmt.Println("Replicaset:", instance.Replicaset)
		fmt.Println("Name:", instance.Name)
		fmt.Println("Mode:", instance.Mode.String())
		fmt.Println("URI:", instance.URI)
		fmt.Println("Roles:", instance.Roles)
		fmt.Println("Labels:", instance.Labels)
	}
	fmt.Println("Error:", err)
	fmt.Println("Done.")

	// Output:
	// Without keys in the prefix:
	// Instances: []
	// Error: <nil>
	// After publishing:
	// Instances
	// Group: foo
	// Replicaset: bar
	// Name: zoo
	// Mode: ro
	// URI: [localhost:3011]
	// Roles: [crud]
	// Labels: map[tags:any,bar,3]
	// Error: <nil>
	// Done.
}

func ExampleTarantool_Discovery() {
	tcs, err := tcshelper.Start(0)
	if err != nil {
		if errors.Is(err, tcshelper.ErrNotSupported) {
			fmt.Println("TcS is not supported:", err)
			return
		}
		log.Fatalf("Failed to start TcS: %s", err)
	}
	discoverer := discoverer.NewTarantool(tcs.Doer(), "/foo")
	instances, err := discoverer.Discovery(context.Background())

	fmt.Println("Without keys in the prefix:")
	fmt.Println("Instances:", instances)
	fmt.Println("Error:", err)

	err = tcs.Put(context.Background(), "/foo/config/key", `
database:
  mode: ro
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            iproto:
              advertise:
                client: localhost:3011
                peer:
                  params:
                    transport: ssl
            roles: [crud]
            labels:
              tags: "any,bar,3"
`)
	if err != nil {
		fmt.Println("Unable to put configuration into etcd:", err)
		return
	}

	instances, err = discoverer.Discovery(context.Background())

	fmt.Println("After publishing:")
	fmt.Println("Instances")
	for _, instance := range instances {
		fmt.Println("Group:", instance.Group)
		fmt.Println("Replicaset:", instance.Replicaset)
		fmt.Println("Name:", instance.Name)
		fmt.Println("Mode:", instance.Mode.String())
		fmt.Println("URI:", instance.URI)
		fmt.Println("Roles:", instance.Roles)
		fmt.Println("Labels:", instance.Labels)
	}
	fmt.Println("Error:", err)
	fmt.Println("Done.")
}

func ExampleEtcd_Discovery_cancelled() {
	cluster := integration.NewLazyCluster()
	defer cluster.Terminate()

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: cluster.EndpointsV3(),
	})
	if err != nil {
		fmt.Println("Unable to start etcd client:", err)
		return
	}
	defer etcd.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	etcddiscoverer := discoverer.NewEtcd(etcd, "foo")
	instances, err := etcddiscoverer.Discovery(ctx)

	fmt.Println(instances)
	fmt.Println(err)
	fmt.Println("Done.")

	// Output:
	// []
	// context canceled
	// Done.
}
