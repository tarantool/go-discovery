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

var _ pool.Balancer = &pool.PriorityBalancer{}

func gen(n int) string { return fmt.Sprintf("inst_%d", n) }

func priorityFromGroup(inst discovery.Instance) int {
	n, _ := strconv.Atoi(inst.Group)
	return n
}

func TestPriorityBalancer_AddAndGet(t *testing.T) {
	prBalancer := pool.NewPriorityBalancer(func(discovery.Instance) int { return 0 })
	require.NotNil(t, prBalancer)

	for i := 0; i < 5; i++ {
		prBalancer.Add(discovery.Instance{
			Name: gen(i),
			Mode: discovery.ModeRO,
		})
	}

	for _, mode := range [...]discovery.Mode{discovery.ModeAny, discovery.ModeRO} {
		for i := 0; i < 10; i++ {
			name, ok := prBalancer.Next(mode)
			assert.True(t, ok)
			assert.Equal(t, gen(i%5), name)
		}
	}

	name, ok := prBalancer.Next(discovery.ModeRW)
	assert.False(t, ok)
	assert.Equal(t, "", name)
}

func TestPriorityBalancer_AddAndGetDifferentPriorities(t *testing.T) {
	prBalancer := pool.NewPriorityBalancer(priorityFromGroup)
	require.NotNil(t, prBalancer)

	for i := 0; i < 6; i++ {
		prBalancer.Add(discovery.Instance{Name: gen(i), Mode: discovery.ModeRO,
			Group: strconv.Itoa(i / 2),
		})
	}
	for i := 6; i < 12; i++ {
		prBalancer.Add(discovery.Instance{Name: gen(i), Mode: discovery.ModeRW,
			Group: strconv.Itoa(i / 2),
		})
	}
	prBalancer.Add(discovery.Instance{Name: gen(99), Mode: discovery.ModeAny,
		Group: "100"})

	// Highest priority for RO is 2 with elements: 4, 5.
	for i := 4; i < 6; i++ {
		name, ok := prBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		assert.Equal(t, gen(i), name)
	}

	// Highest priority for RW is 5 with elements: 10, 11.
	for i := 10; i < 11; i++ {
		name, ok := prBalancer.Next(discovery.ModeRW)
		assert.True(t, ok)
		assert.Equal(t, gen(i), name)
	}

	// Highest priority for All is 100 with elements: 99.
	for i := 0; i < 2; i++ {
		name, ok := prBalancer.Next(discovery.ModeAny)
		assert.True(t, ok)
		assert.Equal(t, gen(99), name)
	}
}

func TestPriorityBalancer_AddAndGetDifferentPrioritiesHigherFirst(t *testing.T) {
	prBalancer := pool.NewPriorityBalancer(priorityFromGroup)
	require.NotNil(t, prBalancer)

	for i := 5; i >= 0; i-- {
		prBalancer.Add(discovery.Instance{Name: gen(i), Mode: discovery.ModeRO,
			Group: strconv.Itoa(i / 2),
		})
	}

	// Highest priority for RO is 2 with elements: 4, 5.
	for i := 5; i > 3; i-- {
		name, ok := prBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		assert.Equal(t, gen(i), name)
	}
}

func TestPriorityBalancer_Remove(t *testing.T) {
	prBalancer := pool.NewPriorityBalancer(priorityFromGroup)
	require.NotNil(t, prBalancer)

	for i := 0; i < 6; i++ {
		prBalancer.Add(discovery.Instance{Name: gen(i), Mode: discovery.ModeRO,
			Group: strconv.Itoa(i / 2),
		})
	}
	for i := 6; i < 12; i++ {
		prBalancer.Add(discovery.Instance{Name: gen(i), Mode: discovery.ModeRW,
			Group: strconv.Itoa(i / 2),
		})
	}
	prBalancer.Add(discovery.Instance{Name: gen(99), Mode: discovery.ModeAny,
		Group: "100"})

	// Highest priority for All is 100 with elements: 99.
	for i := 0; i < 2; i++ {
		name, ok := prBalancer.Next(discovery.ModeAny)
		assert.True(t, ok)
		assert.Equal(t, gen(99), name)
	}

	prBalancer.Remove(gen(99))
	// Highest priority for All is 5 with elements: 10, 11.
	for i := 0; i < 4; i++ {
		name, ok := prBalancer.Next(discovery.ModeAny)
		assert.True(t, ok)
		assert.Equal(t, gen(10+i%2), name)
	}
	// Highest priority for RO is 2 with elements: 4, 5.
	for i := 0; i < 4; i++ {
		name, ok := prBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		assert.Equal(t, gen(4+i%2), name)
	}
}

func TestPriorityBalancer_RemoveAndContinue(t *testing.T) {
	prBalancer := pool.NewPriorityBalancer(priorityFromGroup)
	require.NotNil(t, prBalancer)

	for i := 0; i < 8; i++ {
		prBalancer.Add(discovery.Instance{Name: gen(i), Mode: discovery.ModeRO,
			Group: "0",
		})
	}

	for i := 0; i < 2; i++ {
		name, ok := prBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		assert.Equal(t, gen(i), name)
	}
	prBalancer.Remove(gen(1))
	prBalancer.Remove(gen(2))
	prBalancer.Remove(gen(3))
	name, ok := prBalancer.Next(discovery.ModeRO)
	assert.True(t, ok)
	assert.Equal(t, gen(4), name)

	prBalancer.Add(discovery.Instance{Name: gen(8), Mode: discovery.ModeRO,
		Group: "2"})
	name, ok = prBalancer.Next(discovery.ModeRO)
	assert.True(t, ok)
	assert.Equal(t, gen(8), name)

	// After removing high priority string, continue from 4th element of the previous priority.
	prBalancer.Remove(gen(8))
	for i := 5; i < 8; i++ {
		name, ok := prBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		assert.Equal(t, gen(i), name)
	}
}

func TestPriorityBalancer_AddExisting(t *testing.T) {
	prBalancer := pool.NewPriorityBalancer(priorityFromGroup)
	require.NotNil(t, prBalancer)

	for i := 0; i < 4; i++ {
		prBalancer.Add(discovery.Instance{Name: gen(i), Mode: discovery.ModeRO, Group: "4"})
	}

	for i := 0; i < 4; i++ {
		name, ok := prBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		require.Equal(t, gen(i), name)
	}
	prBalancer.Add(discovery.Instance{Name: gen(0), Mode: discovery.ModeRW, Group: "3"})

	// No 0 elem in RO.
	for i := 1; i < 4; i++ {
		name, ok := prBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		require.Equal(t, gen(i), name)
	}
	name, ok := prBalancer.Next(discovery.ModeRW)
	assert.True(t, ok)
	assert.Equal(t, gen(0), name)

	// Since 0 item priority is changed, it is moved for All mode also. No 0 elem.
	for i := 0; i < 6; i++ {
		name, ok := prBalancer.Next(discovery.ModeAny)
		assert.True(t, ok)
		require.Equal(t, gen(1+i%3), name)
	}

	// Append existing item with the same priority, but other mode.
	prBalancer.Add(discovery.Instance{Name: gen(1), Mode: discovery.ModeRW, Group: "4"})
	// No 0 and 1 elem in RO.
	for i := 2; i < 4; i++ {
		name, ok := prBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		require.Equal(t, gen(i), name)
	}

	// Order of All mode items is not changed, because existing item was not removed.
	for i := 0; i < 6; i++ {
		name, ok := prBalancer.Next(discovery.ModeAny)
		assert.True(t, ok)
		require.Equal(t, gen(1+i%3), name)
	}
}

func TestPriorityBalancer_AsyncAddAndGet(t *testing.T) {
	prBalancer := pool.NewPriorityBalancer(priorityFromGroup)
	require.NotNil(t, prBalancer)

	routinesCount := 10
	var wg sync.WaitGroup
	wg.Add(routinesCount)
	for i := 0; i < routinesCount; i++ {
		start := i * 1000
		go func() {
			for i := start; i < start+1000; i++ {
				prBalancer.Add(discovery.Instance{
					Name:  gen(i),
					Mode:  discovery.ModeRO,
					Group: strconv.Itoa(start / 1000)})
			}
			wg.Done()
		}()
	}
	wg.Wait()

	names := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		name, ok := prBalancer.Next(discovery.ModeRO)
		assert.True(t, ok)
		names[name] = true
	}
	assert.Equal(t, 1000, len(names))

	names = make(map[string]bool)
	assert.Equal(t, 0, len(names))
	var namesChan = make(chan string, 500)
	defer close(namesChan)
	for i := 0; i < routinesCount; i++ {
		start := i * 100
		go func() {
			for i := start; i < start+100; i++ {
				name, _ := prBalancer.Next(discovery.ModeRO)
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

func TestPriorityBalancer_AsyncRemove(t *testing.T) {
	prBalancer := pool.NewPriorityBalancer(priorityFromGroup)
	require.NotNil(t, prBalancer)

	for i := 0; i < 1000; i++ {
		prBalancer.Add(discovery.Instance{Name: gen(i), Mode: discovery.ModeRO, Group: "1"})
		prBalancer.Add(discovery.Instance{Name: gen(i + 1000), Mode: discovery.ModeRW, Group: "0"})
	}

	routinesCount := 5
	var wg sync.WaitGroup
	wg.Add(routinesCount * 2) // Routines count for RO and RW.
	for i := 0; i < 500; i += 100 {
		startRO := i
		go func() {
			// Remove 0-499.
			for i := startRO; i < startRO+100; i++ {
				prBalancer.Remove(gen(i))
			}
			wg.Done()
		}()
		startRW := i + 1500
		go func() {
			// Remove 1500-1999.
			for i := startRW; i < startRW+100; i++ {
				prBalancer.Remove(gen(i))
			}
			wg.Done()
		}()
	}
	wg.Wait()

	for i := 0; i < 1000; i++ {
		name, ok := prBalancer.Next(discovery.ModeRO)
		require.True(t, ok)
		require.Equal(t, gen(i%500+500), name)
	}
	for i := 1000; i < 2000; i++ {
		name, ok := prBalancer.Next(discovery.ModeRW)
		require.True(t, ok)
		require.Equal(t, gen(i%500+1000), name)
	}
	for i := 0; i < 1000; i++ {
		name, ok := prBalancer.Next(discovery.ModeAny)
		require.True(t, ok)
		require.Equal(t, gen(i%500+500), name)
	}
}
