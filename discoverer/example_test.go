package discoverer_test

import (
	"context"
	"fmt"
	"io"
	"log"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"

	"github.com/tarantool/go-discovery/discoverer"
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
              listen:
              - uri: localhost:3011
              - uri: localhost:3012
                transport: ssl
              - uri: localhost:3013
                transport: plain
            roles: [crud]
            roles_cfg:
              tags:
              - any
              - bar
              - 3
            app:
              cfg:
                tags:
                - foo
                - bar
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
		fmt.Println("RolesTags:", instance.RolesTags)
		fmt.Println("AppTags:", instance.AppTags)
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
	// URI: [localhost:3011 localhost:3012 localhost:3013]
	// Roles: [crud]
	// RolesTags: [any bar 3]
	// AppTags: [foo bar]
	// Error: <nil>
	// Done.
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
