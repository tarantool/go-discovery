package discovery_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/discoverer"
	"github.com/tarantool/go-discovery/filter"
	"github.com/tarantool/go-discovery/pool"
	"github.com/tarantool/go-discovery/scheduler"
	"github.com/tarantool/go-discovery/subscriber"
)

func init() {
	log.SetOutput(io.Discard)
}

func exampleStartTarantool(address string) (test_helpers.TarantoolInstance, error) {
	dialer := tarantool.NetDialer{
		Address:  address,
		User:     "testuser",
		Password: "testpass",
	}
	startOpts := test_helpers.StartOpts{
		Dialer:       dialer,
		InitScript:   "testdata/init.lua",
		Listen:       address,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 3,
		RetryTimeout: 500 * time.Millisecond,
	}

	inst, err := test_helpers.StartTarantool(startOpts)
	if err != nil {
		exampleStopTarantool(inst)
		return inst, err
	}
	return inst, nil
}

func exampleStopTarantool(instance test_helpers.TarantoolInstance) {
	test_helpers.StopTarantoolWithCleanup(instance)
}

// Example demonstrates how to create and use the simplest variant of the pool.
func Example() {
	// Start test Tarantool instance.
	ttInstance, err := exampleStartTarantool("127.0.0.1:3013")
	if err != nil {
		fmt.Println("Failed to start Tarantool:", err)
		return
	}
	defer exampleStopTarantool(ttInstance)

	// Start a test etcd cluster and connect with a client to it.
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

	// And publish a cluster configuration to it.
	_, err = etcd.Put(context.Background(), "/prefix/config/all", `
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

	// The pool will try to connect to instances without TLS. It will send
	// requests in round-robin. But you could use pool.PriorityBalancer to
	// send request into instances by a priority.
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
				etcdSubscriber.Unsubscribe(examplePool)
				break
			}

			if errors.Is(err, pool.ErrNoConnectedInstances) {
				// The pool is not connected to an instance yet. We could just
				// repeat the request.
				//
				//  fmt.Println("No instances.")
				//  time.Sleep(time.Second)
				//
				// Commented to make the example repeatable and fast.
				continue
			}

			etcdSubscriber.Unsubscribe(examplePool)
			fmt.Println("Result:", result)
			fmt.Println("Error:", err)
			fmt.Println("Done.")
			return
		}
	}

	// Output:
	// Result: [127.0.0.1:3013]
	// Error: <nil>
	// Done.
}

type exampleObserver struct {
	wgEvent sync.WaitGroup
}

func newExampleObserver() *exampleObserver {
	obs := &exampleObserver{}

	obs.wgEvent = sync.WaitGroup{}

	return obs
}

func (o *exampleObserver) Observe(events []discovery.Event, err error) {
	if err != nil {
		fmt.Println("Error from the observer:", err)
	} else {
		fmt.Println("An event found.")
		for _, event := range events {
			fmt.Println("Type:", event.Type)
			fmt.Println("Group:", event.New.Group)
			fmt.Println("Replicaset:", event.New.Replicaset)
			fmt.Println("Name:", event.New.Name)
			fmt.Println("Mode:", event.New.Mode.String())
			fmt.Println("URI:", event.New.URI)
			fmt.Println("Roles:", event.New.Roles)
			fmt.Println("RolesTags:", event.New.RolesTags)
			fmt.Println("AppTags:", event.New.AppTags)
		}
		o.wgEvent.Done()
	}
}

func Example_subscriber_Schedule_Etcd() {
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

	schedule := subscriber.NewSchedule(scheduler.NewEtcdWatch(etcd, "foo"),
		discoverer.NewEtcd(etcd, "foo"))

	obs := newExampleObserver()
	obs.wgEvent.Add(2)

	err = schedule.Subscribe(context.Background(), obs)
	if err != nil {
		fmt.Println("Subscribe error:", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = etcd.Put(ctx, "foo/config/key", `
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
	cancel()

	if err != nil {
		fmt.Println("Put error:", err)
		return
	}

	obs.wgEvent.Wait()
	schedule.Unsubscribe(obs)

	fmt.Println("Done.")

	// Output:
	// An event found.
	// An event found.
	// Type: add
	// Group: foo
	// Replicaset: bar
	// Name: zoo
	// Mode: ro
	// URI: [localhost:3011]
	// Roles: [crud]
	// RolesTags: [any bar 3]
	// AppTags: [foo bar]
	// Error from the observer: unsubscribed
	// Done.
}

func Example_subscriber_Schedule_Etcd_canceled() {
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

	schedule := subscriber.NewSchedule(scheduler.NewEtcdWatch(etcd, "foo"),
		discoverer.NewEtcd(etcd, "foo"))

	obs := newExampleObserver()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = schedule.Subscribe(ctx, obs)
	if err != nil {
		fmt.Println("Subscribe error:", err)
	}
	fmt.Println("Done.")

	// Output:
	// Subscribe error: context canceled
	// Done.
}

func Example_subscriber_Filter() {
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

	schedule := subscriber.NewSchedule(scheduler.NewEtcdWatch(etcd, "foo"),
		discoverer.NewEtcd(etcd, "foo"))

	filters := []discovery.Filter{
		discovery.FilterFunc(func(inst discovery.Instance) bool {
			return inst.Name == "zoo"
		}),
		discovery.FilterFunc(func(inst discovery.Instance) bool {
			return inst.Group == "foo"
		}),
	}
	filter := subscriber.NewFilter(schedule, filters...)

	obs := newExampleObserver()
	obs.wgEvent.Add(2)

	err = filter.Subscribe(context.Background(), obs)
	if err != nil {
		fmt.Println("Subscribe error:", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = etcd.Put(ctx, "foo/config/key", `
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
  foo2:
    replicasets:
      bar2:
        instances:
          zoo2:
            iproto:
              advertise:
                client: localhost:3012
                peer:
                  params:
                    transport: ssl
            roles: [crud]
            roles_cfg:
              tags:
              - any
              - bar
              - 3
            app:
              cfg:
                tags:
                - foo2
                - bar
`)
	cancel()

	if err != nil {
		fmt.Println("Put error:", err)
		return
	}

	obs.wgEvent.Wait()
	filter.Unsubscribe(obs)

	fmt.Println("Done.")

	// Output:
	// An event found.
	// An event found.
	// Type: add
	// Group: foo
	// Replicaset: bar
	// Name: zoo
	// Mode: ro
	// URI: [localhost:3011]
	// Roles: [crud]
	// RolesTags: [any bar 3]
	// AppTags: [foo bar]
	// Error from the observer: unsubscribed
	// Done.
}

func Example_subscriber_Filter_Etcd_canceled() {
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

	schedule := subscriber.NewSchedule(scheduler.NewEtcdWatch(etcd, "foo"),
		discoverer.NewEtcd(etcd, "foo"))

	filters := []discovery.Filter{
		discovery.FilterFunc(func(inst discovery.Instance) bool {
			return inst.Name == "zoo"
		}),
	}
	filter := subscriber.NewFilter(schedule, filters...)

	obs := newExampleObserver()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = filter.Subscribe(ctx, obs)
	if err != nil {
		fmt.Println("Subscribe error:", err)
	}
	fmt.Println("Done.")

	// Output:
	// Subscribe error: context canceled
	// Done.
}
