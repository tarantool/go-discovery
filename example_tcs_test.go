package discovery_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tarantool/go-discovery/dial"
	"github.com/tarantool/go-discovery/discoverer"
	"github.com/tarantool/go-tarantool/v2"
	tcshelper "github.com/tarantool/go-tarantool/v2/test_helpers/tcs"
)

func Example_discoverer_Connectable_with_Tarantool() {
	// Start test Tarantool instance.
	ttInstance, err := exampleStartTarantool("127.0.0.1:3013")
	if err != nil {
		fmt.Println("Failed to start Tarantool:", err)
		return
	}
	defer exampleStopTarantool(ttInstance)

	tcs, err := tcshelper.Start(0)
	if err != nil {
		if errors.Is(err, tcshelper.ErrNotSupported) {
			fmt.Println("TcS is not supported:", err)
		} else {
			fmt.Println("Failed to start TCS:", err)
		}
		return
	}
	defer tcs.Stop()

	err = tcs.Put(context.Background(), "/prefix/config/all", `
groups:
  foo:
    replicasets:
      bar:
        instances:
          instance1:
            iproto:
              advertise:
                client: 127.0.0.1:3013
            roles: [crud]
            labels:
              tags: "any,bar,3"
          instance2:
            iproto:
              advertise:
                client: 127.0.0.1:3014
          instance3:
            iproto:
              advertise:
                client: 127.0.0.1:3015
`)

	if err != nil {
		fmt.Println("Failed to publish cluster configuration error:", err)
		return
	}

	// Create a net dialer factory.
	factory := dial.NewNetDialerFactory("testuser", "testpass",
		tarantool.Opts{Timeout: 5 * time.Second})

	// Create a Connectable discoverer based on the Etcd discoverer.
	disc := discoverer.NewConnectable(factory,
		discoverer.NewTarantool(tcs.Doer().(tarantool.Connector), "/prefix"))

	// Get a list of connectable instances.
	inst, err := disc.Discovery(context.Background())
	if err != nil {
		fmt.Println("Failed to discover connectable instances:", err)
		return
	}

	for _, instance := range inst {
		fmt.Println("Instance found")
		fmt.Println("Name:", instance.Name)
		fmt.Println("Mode:", instance.Mode)
		fmt.Println("URI:", instance.URI)
		fmt.Println("Roles:", instance.Roles)
		fmt.Println("Labels:", instance.Labels)
		fmt.Println("Group:", instance.Group)
		fmt.Println("Replicaset:", instance.Replicaset)
	}
	fmt.Println("Done.")
}
