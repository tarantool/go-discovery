package buffer_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-discovery/pool/internal/container/buffer"
)

func gen(n int) string { return fmt.Sprintf("inst_%d", n) }

func TestPrioritizedCircular_PushAndGet(t *testing.T) {
	buf := buffer.NewPriorityCircular[string]()
	require.NotNil(t, buf)

	for i := 0; i < 5; i++ {
		buf.Push(gen(i), 0)
	}

	for i := 0; i < 10; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, gen(i%5), val)
	}

	// Push higher priority values.
	for i := 10; i < 15; i++ {
		buf.Push(gen(i), 10)
	}
	for i := 0; i < 10; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, gen(i%5+10), val)
	}

	// Push middle priority values.
	for i := 5; i < 10; i++ {
		buf.Push(gen(i), 5)
	}
	// GetNext still returns higher priority values.
	for i := 0; i < 10; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, gen(i%5+10), val)
	}
}

func TestPrioritizedCircular_PushAlreadyExisting(t *testing.T) {
	buf := buffer.NewPriorityCircular[string]()
	require.NotNil(t, buf)

	for i := 0; i < 5; i++ {
		buf.Push(gen(i), 1)
	}
	buf.Push(gen(3), 1)
	buf.Push(gen(4), 1)

	for i := 0; i < 10; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, gen(i%5), val)
	}

	// Push existing elements with lower priority.
	// This will remove duplicate elements from higher priority.
	for i := 0; i < 3; i++ {
		buf.Push(gen(i), 0)
	}
	for i := 0; i < 4; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, gen(3+i%2), val)
	}
}

func TestPrioritizedCircular_GetFromEmpty(t *testing.T) {
	buf := buffer.NewPriorityCircular[string]()
	require.NotNil(t, buf)

	val, ok := buf.GetNext()
	assert.False(t, ok)
	assert.Equal(t, "", val)
}

func TestPrioritizedCircular_DeleteOnePriority(t *testing.T) {
	buf := buffer.NewPriorityCircular[int]()
	require.NotNil(t, buf)

	for i := 0; i < 5; i++ {
		buf.Push(i, 0)
	}
	buf.Delete(2)
	buf.Delete(3)

	for i := 0; i < 10; i++ {
		if i%5 == 2 || i%5 == 3 {
			continue
		}
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, i%5, val)
	}
}

func TestPrioritizedCircular_DeleteMultiplePriorities(t *testing.T) {
	buf := buffer.NewPriorityCircular[int]()
	require.NotNil(t, buf)

	for i := 0; i < 5; i++ {
		buf.Push(i, 0)
	}
	for i := 3; i < 6; i++ {
		buf.Push(i, 1)
	}
	buf.Delete(3)
	buf.Delete(4)

	// Only 5 element left with highest 1 priority.
	for i := 0; i < 2; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, 5, val)
	}

	// Remove the latest highest priority element. Only 0,1,2 left with 0 priority.
	buf.Delete(5)
	for i := 0; i < 3; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, i%3, val)
	}
}

func TestPrioritizedCircular_NextContinuesFromWhereItStopped(t *testing.T) {
	buf := buffer.NewPriorityCircular[int]()
	require.NotNil(t, buf)

	for i := 0; i < 5; i++ {
		buf.Push(i, 0)
	}

	// Get 2 elements. 0, 1 - with priority 0.
	for i := 0; i < 2; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, i, val)
	}

	// Add higher priority element.
	buf.Push(10, 1)
	for i := 0; i < 2; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, 10, val)
	}

	// Remove higher priority element. GetNext continues from where it stopped - 2,3,4.
	buf.Delete(10)
	for i := 2; i < 5; i++ {
		val, ok := buf.GetNext()
		assert.True(t, ok)
		assert.Equal(t, i, val)
	}
}
