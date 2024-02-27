package pool_test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/pool"
)

var _ pool.Balancer = &pool.RoundRobinBalancer{}

func TestRRBalancer_AddAndGet(t *testing.T) {
	rrBalancer := pool.NewRoundRobinBalancer()
	require.NotNil(t, rrBalancer)

	gen := func(n int) string { return fmt.Sprintf("inst_%d", n) }
	mode := discovery.ModeAny
	instPerMode := 10
	for i := 0; i < 30; i++ {
		if i == 10 {
			mode = discovery.ModeRO
		} else if i == 20 {
			mode = discovery.ModeRW
		}
		rrBalancer.Add(discovery.Instance{
			Name: gen(i),
			Mode: mode,
		})
	}

	for i := 0; i < 60; i++ {
		name, ok := rrBalancer.Next(discovery.ModeAny)
		assert.True(t, ok)
		assert.Equal(t, gen(i%30), name)
	}
	for i := 0; i < 20; i++ {
		name, ok := rrBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		assert.Equal(t, gen(i%instPerMode+10), name)
	}
	for i := 0; i < 20; i++ {
		name, ok := rrBalancer.Next(discovery.ModeRW)
		assert.True(t, ok)
		assert.Equal(t, gen(i%instPerMode+20), name)
	}
}

func TestRRBalancer_Overwrite(t *testing.T) {
	rrBalancer := pool.NewRoundRobinBalancer()
	require.NotNil(t, rrBalancer)

	for i := 0; i < 5; i++ {
		rrBalancer.Add(discovery.Instance{Name: strconv.Itoa(i), Mode: discovery.ModeRO})
	}
	for i := 5; i < 10; i++ {
		rrBalancer.Add(discovery.Instance{Name: strconv.Itoa(i), Mode: discovery.ModeRW})
	}

	// Add inst_3-4 as RW. Should be removed from RO.
	rrBalancer.Add(discovery.Instance{Name: strconv.Itoa(3), Mode: discovery.ModeRW})
	rrBalancer.Add(discovery.Instance{Name: strconv.Itoa(4), Mode: discovery.ModeRW})
	// RO: 0,1,2 left, 3 & 4 moved to RW.
	for i := 0; i < 5; i++ {
		name, ok := rrBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		assert.Equal(t, strconv.Itoa(i%3), name)
	}
	// RW: 5,6,7,8,9,3,4 instances.
	for i := 2; i < 15; i++ {
		name, ok := rrBalancer.Next(discovery.ModeRW)
		assert.True(t, ok)
		assert.Equal(t, strconv.Itoa(i%7+3), name)
	}
	// Check all mode. Nothing is changed.
	for i := 0; i < 20; i++ {
		name, ok := rrBalancer.Next(discovery.ModeAny)
		assert.True(t, ok)
		assert.Equal(t, strconv.Itoa(i%10), name)
	}
}

func TestRRBalancer_DontOverwriteExisting(t *testing.T) {
	rrBalancer := pool.NewRoundRobinBalancer()
	require.NotNil(t, rrBalancer)

	for i := 0; i < 5; i++ {
		rrBalancer.Add(discovery.Instance{Name: strconv.Itoa(i), Mode: discovery.ModeRO})
	}

	// Do not overwrite/move existing instances with the same mode.
	rrBalancer.Add(discovery.Instance{Name: strconv.Itoa(3), Mode: discovery.ModeRO})
	rrBalancer.Add(discovery.Instance{Name: strconv.Itoa(1), Mode: discovery.ModeRO})
	for i := 0; i < 10; i++ {
		name, ok := rrBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		assert.Equal(t, strconv.Itoa(i%5), name)
	}
}

func TestRRBalancer_RemovingInstances(t *testing.T) {
	rrBalancer := pool.NewRoundRobinBalancer()
	require.NotNil(t, rrBalancer)

	gen := func(n int) string { return fmt.Sprintf("inst_%d", n) }
	mode := discovery.ModeAny
	for i := 0; i < 30; i++ {
		if i == 10 {
			mode = discovery.ModeRO
		} else if i == 20 {
			mode = discovery.ModeRW
		}
		rrBalancer.Add(discovery.Instance{
			Name: gen(i),
			Mode: mode,
		})
	}

	for i := 0; i < 5; i++ {
		rrBalancer.Remove(gen(i))      // Removing 0-5. All mode instances.
		rrBalancer.Remove(gen(i + 11)) // Removing 11-15. RO instances.
		rrBalancer.Remove(gen(i + 28)) // Removing 28-32. 30-32 do not exist. RW instances.
	}

	// All.
	for i := 5; i <= 10; i++ {
		name, ok := rrBalancer.Next(discovery.ModeAny)
		assert.True(t, ok)
		assert.Equal(t, gen(i), name)
	}
	for i := 16; i < 28; i++ {
		name, ok := rrBalancer.Next(discovery.ModeAny)
		assert.True(t, ok)
		assert.Equal(t, gen(i), name)
	}
	for i := 5; i <= 10; i++ {
		name, ok := rrBalancer.Next(discovery.ModeAny)
		assert.True(t, ok)
		assert.Equal(t, gen(i), name)
	}

	// RO.
	name, ok := rrBalancer.Next(discovery.ModeRO)
	assert.True(t, ok)
	assert.Equal(t, gen(10), name)
	for i := 16; i < 20; i++ {
		name, ok = rrBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		assert.Equal(t, gen(i), name)
	}
	name, ok = rrBalancer.Next(discovery.ModeRO)
	assert.True(t, ok)
	assert.Equal(t, gen(10), name)

	// RW.
	for i := 20; i < 28; i++ {
		name, ok := rrBalancer.Next(discovery.ModeRW)
		assert.True(t, ok)
		assert.Equal(t, gen(i), name)
	}
	name, ok = rrBalancer.Next(discovery.ModeRW)
	assert.True(t, ok)
	assert.Equal(t, gen(20), name)
}

func TestRRBalancer_RemovingInstancesIterInProgress(t *testing.T) {
	rrBalancer := pool.NewRoundRobinBalancer()
	require.NotNil(t, rrBalancer)

	for i := 0; i < 10; i++ {
		rrBalancer.Add(discovery.Instance{Name: strconv.Itoa(i), Mode: discovery.ModeRO})
	}

	for i := 0; i < 5; i++ {
		name, ok := rrBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		assert.Equal(t, strconv.Itoa(i), name)
	}
	// Remove some already passed instances.
	rrBalancer.Remove("0")
	rrBalancer.Remove("2")
	// The next element should be 5 still.
	for i := 5; i < 7; i++ {
		name, ok := rrBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		assert.Equal(t, strconv.Itoa(i), name)
	}
	// Next element should be 7. Remove it.
	rrBalancer.Remove("7")
	// The next element should be 5 still.
	for i := 8; i < 10; i++ {
		name, ok := rrBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		assert.Equal(t, strconv.Itoa(i), name)
	}
}

func TestRRBalancer_AsyncGet(t *testing.T) {
	rrBalancer := pool.NewRoundRobinBalancer()
	require.NotNil(t, rrBalancer)

	gen := func(n int) string { return fmt.Sprintf("inst_%d", n) }

	routinesCount := 10
	var wg sync.WaitGroup
	wg.Add(routinesCount)
	for i := 0; i < routinesCount; i++ {
		start := i * 100
		go func() {
			for i := start; i < start+100; i++ {
				rrBalancer.Add(discovery.Instance{
					Name: gen(i),
					Mode: discovery.ModeRW,
				})
			}
			wg.Done()
		}()
	}
	wg.Wait()

	names := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		name, ok := rrBalancer.Next(discovery.ModeRW)
		assert.True(t, ok)
		names[name] = true
	}
	assert.Equal(t, 1000, len(names))

	names = make(map[string]bool)
	assert.Equal(t, 0, len(names))
	var namesChan = make(chan string, 500)
	for i := 0; i < routinesCount; i++ {
		start := i * 100
		go func() {
			for i := start; i < start+100; i++ {
				name, ok := rrBalancer.Next(discovery.ModeRW)
				if !ok {
					t.Error("failed to get next instance")
					close(namesChan)
				}
				namesChan <- name
			}
		}()
	}

	for i := 0; i < 1000; i++ {
		name := <-namesChan
		names[name] = true
	}
	assert.Equal(t, 1000, len(names))
}

func TestRRBalancer_AsyncRemove(t *testing.T) {
	rrBalancer := pool.NewRoundRobinBalancer()
	require.NotNil(t, rrBalancer)

	gen := func(n int) string { return fmt.Sprintf("inst_%d", n) }

	for i := 0; i < 1000; i++ {
		rrBalancer.Add(discovery.Instance{Name: gen(i), Mode: discovery.ModeRO})
		rrBalancer.Add(discovery.Instance{Name: gen(i + 1000), Mode: discovery.ModeRW})
	}

	names := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		name, ok := rrBalancer.Next(discovery.ModeRW)
		assert.True(t, ok)
		names[name] = true
	}
	assert.Equal(t, 1000, len(names))

	routinesCount := 5
	var wg sync.WaitGroup
	wg.Add(routinesCount * 2) // Routines count for RO and RW.
	for i := 0; i < 500; i += 100 {
		startRO := i
		go func() {
			// Remove 0-499.
			for i := startRO; i < startRO+100; i++ {
				rrBalancer.Remove(gen(i))
			}
			wg.Done()
		}()
		startRW := i + 1500
		go func() {
			// Remove 1500-1999.
			for i := startRW; i < startRW+100; i++ {
				rrBalancer.Remove(gen(i))
			}
			wg.Done()
		}()
	}

	wg.Wait()
	for i := 0; i < 1000; i++ {
		name, ok := rrBalancer.Next(discovery.ModeRO)
		require.True(t, ok)
		require.Equal(t, gen(i%500+500), name)
	}
	for i := 1000; i < 2000; i++ {
		name, ok := rrBalancer.Next(discovery.ModeRW)
		require.True(t, ok)
		require.Equal(t, gen(i%500+1000), name)
	}
	for i := 500; i < 2500; i++ {
		name, ok := rrBalancer.Next(discovery.ModeAny)
		require.True(t, ok)
		require.Equal(t, gen(i%1000+500), name)
	}
}
