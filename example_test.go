package discovery_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/discoverer"
	"github.com/tarantool/go-discovery/scheduler"
	"github.com/tarantool/go-discovery/subscriber"
)

func init() {
	log.SetOutput(io.Discard)
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

	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
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

	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
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
      bar:
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
