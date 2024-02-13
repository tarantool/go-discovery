package scheduler_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tarantool/go-discovery/scheduler"
)

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
