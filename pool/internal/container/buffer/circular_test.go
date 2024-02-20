package buffer_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-discovery/pool/internal/container/buffer"
)

func TestCircularBuffer_PushAndGet(t *testing.T) {
	buf := buffer.NewCircular[string]()
	require.NotNil(t, buf)

	gen := func(n int) string { return fmt.Sprintf("inst_%d", n) }

	for i := 0; i < 5; i++ {
		buf.Push(gen(i))
	}

	for i := 0; i < 10; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, gen(i%5), val)
	}

	// Add new.
	buf.Push(gen(5))
	for i := 0; i < 12; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, gen(i%6), val)
	}

	// Push already existing value. Still 6 elements.
	buf.Push(gen(1))
	for i := 0; i < 12; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, gen(i%6), val)
	}
}

func TestCircularBuffer_PushExistingElement(t *testing.T) {
	buf := buffer.NewCircular[string]()
	require.NotNil(t, buf)

	gen := func(n int) string { return fmt.Sprintf("inst_%d", n) }

	for i := 0; i < 5; i++ {
		buf.Push(gen(i))
	}

	// Push already existing value. Still 5 elements.
	buf.Push(gen(1))
	for i := 0; i < 10; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, gen(i%5), val)
	}
}

func TestCircularBuffer_Remove(t *testing.T) {
	buf := buffer.NewCircular[string]()
	require.NotNil(t, buf)

	gen := func(n int) string { return fmt.Sprintf("inst_%d", n) }

	for i := 0; i < 5; i++ {
		buf.Push(gen(i))
	}

	buf.Delete(gen(0))
	buf.Delete(gen(2))
	buf.Delete(gen(3))
	assert.True(t, buf.Contains("inst_4"))
	assert.True(t, buf.Contains("inst_1"))
	assert.False(t, buf.Contains("inst_0"))
	assert.False(t, buf.Contains("inst_2"))
	assert.False(t, buf.Contains("inst_3"))
	assert.Equal(t, 2, buf.Len())
}

func TestCircularBuffer_RemoveAndGet(t *testing.T) {
	buf := buffer.NewCircular[string]()
	require.NotNil(t, buf)

	gen := func(n int) string { return fmt.Sprintf("inst_%d", n) }

	for i := 0; i < 5; i++ {
		buf.Push(gen(i))
	}

	buf.Delete(gen(2)) // Remove "inst_2".
	for i := 0; i < 5; i++ {
		if i == 2 {
			continue
		}
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, gen(i%5), val)
	}

	// Move forward.
	val, ok := buf.GetNext()
	assert.True(t, ok)
	assert.Equal(t, gen(0), val)
	val, ok = buf.GetNext()
	assert.True(t, ok)
	assert.Equal(t, gen(1), val)

	buf.Delete(gen(0)) // Remove "inst_0".
	// inst_2 is removed already, so inst_3 is the next.
	val, ok = buf.GetNext()
	assert.True(t, ok)
	assert.Equal(t, gen(3), val)
	val, ok = buf.GetNext()
	assert.True(t, ok)
	assert.Equal(t, gen(4), val)
	// inst_0 is removed, so _1 is the next.
	val, ok = buf.GetNext()
	assert.True(t, ok)
	assert.Equal(t, gen(1), val)

	buf.Delete(gen(3)) // Remove "inst_3".
	val, ok = buf.GetNext()
	assert.True(t, ok)
	assert.Equal(t, gen(4), val)
}

func TestCircularBuffer_Empty(t *testing.T) {
	buf := buffer.NewCircular[string]()
	require.NotNil(t, buf)
	val, ok := buf.GetNext()
	assert.False(t, ok)
	assert.Empty(t, val)
}

func TestCircularBuffer_AsyncGet(t *testing.T) {
	buf := buffer.NewCircular[int]()
	require.NotNil(t, buf)

	for i := 0; i < 1000; i++ {
		buf.Push(i)
	}
	require.Equal(t, 1000, buf.Len())

	routinesCount := 10
	var wg sync.WaitGroup
	wg.Add(routinesCount)
	var sum atomic.Int32
	for i := 0; i < routinesCount; i++ {
		go func() {
			for i := 0; i < 100; i++ {
				num, ok := buf.GetNext()
				assert.True(t, ok)
				sum.Add(int32(num))
			}
			wg.Done()
		}()
	}
	wg.Wait()
	assert.Equal(t, 500*999, int(sum.Load()))
}
