package scheduler_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"

	"github.com/tarantool/go-discovery/scheduler"
)

func init() {
	log.SetOutput(io.Discard)
}

func ExamplePeriodic_Wait() {
	scheduler := scheduler.NewPeriodic(time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := scheduler.Wait(ctx)
	if err != nil {
		fmt.Println("Error:", err.Error()+".")
	} else {
		fmt.Println("Event tick.")
	}

	// Output:
	// Event tick.
}

func ExamplePeriodic_Stop() {
	scheduler := scheduler.NewPeriodic(10 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err := scheduler.Wait(ctx)
		if err != nil {
			fmt.Println("Error:", err.Error()+".")
		} else {
			fmt.Println("Event tick.")
		}
		wg.Done()
	}()

	scheduler.Stop()
	wg.Wait()

	// Output:
	// Error: scheduler was stopped.
}

func ExamplePeriodic_Wait_contextCancel() {
	scheduler := scheduler.NewPeriodic(10 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err := scheduler.Wait(ctx)
		if err != nil {
			fmt.Println("Error:", err.Error()+".")
		} else {
			fmt.Println("Event tick.")
		}
		wg.Done()
	}()

	cancel()
	wg.Wait()

	// Output:
	// Error: context canceled.
}

func ExampleEtcdWatch_Wait() {
	cluster := integration.NewLazyCluster()
	defer cluster.Terminate()

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: cluster.EndpointsGRPC(),
	})
	if err != nil {
		fmt.Println("Unable to start etcd client:", err)
		return
	}
	defer func() { _ = etcd.Close() }()

	scheduler := scheduler.NewEtcdWatch(etcd, "key")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err := scheduler.Wait(ctx)
		if err != nil {
			fmt.Println("Error:", err.Error()+".")
		} else {
			fmt.Println("Event received.")
		}
		wg.Done()
	}()

	ctxPut, cancelPut := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = etcd.Put(ctxPut, "key_prefix", "value")
	cancelPut()

	if err != nil {
		fmt.Println("Etcd put failed:", err)
		return
	}

	wg.Wait()

	// Output:
	// Event received.
}

func ExampleEtcdWatch_Stop() {
	cluster := integration.NewLazyCluster()
	defer cluster.Terminate()

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: cluster.EndpointsGRPC(),
	})
	if err != nil {
		fmt.Println("Unable to start etcd client:", err)
		return
	}
	defer func() { _ = etcd.Close() }()

	scheduler := scheduler.NewEtcdWatch(etcd, "key")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err := scheduler.Wait(ctx)
		if err != nil {
			fmt.Println("Error:", err.Error()+".")
		} else {
			fmt.Println("Event received.")
		}
		wg.Done()
	}()

	scheduler.Stop()
	wg.Wait()

	// Output:
	// Error: scheduler was stopped.
}

func ExampleEtcdWatch_Wait_contextCancel() {
	cluster := integration.NewLazyCluster()
	defer cluster.Terminate()

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: cluster.EndpointsGRPC(),
	})
	if err != nil {
		fmt.Println("Unable to start etcd client:", err)
		return
	}
	defer func() { _ = etcd.Close() }()

	scheduler := scheduler.NewEtcdWatch(etcd, "key")

	ctx, cancel := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err := scheduler.Wait(ctx)
		if err != nil {
			fmt.Println("Error:", err.Error()+".")
		} else {
			fmt.Println("Event received.")
		}
		wg.Done()
	}()

	cancel()
	wg.Wait()

	// Output:
	// Error: context canceled.
}
