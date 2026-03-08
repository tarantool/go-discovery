// Package buffer implements buffers for a pool container.
package buffer

import (
	"sync/atomic"

	"golang.org/x/exp/slices"
)

/*
CircularBuffer is a circular buffer with no size. It grows forever and
circular algorithm is only for getting the elements. The implementation is not thread-safe.
This helper container is used by higher-level balancer algorithm, which has its own sync
logic. Only iteration with `GetNext` is guaranteed to be thread-safe if the buffer is not changed.
*/
type CircularBuffer[T comparable] struct {
	// index is an index to get next element from.
	index atomic.Int32
	// data is a main storage for buffer data.
	data []T
}

// NewCircular creates new circular buffer.
func NewCircular[T comparable]() *CircularBuffer[T] {
	return &CircularBuffer[T]{
		index: atomic.Int32{},
		data:  make([]T, 0),
	}
}

// GetNext returns the next element from the buffer. Updates internal iterator in thread-safe
// manner.
func (b *CircularBuffer[T]) GetNext() (T, bool) {
	var x T
	bufLen := int32(len(b.data))
	if bufLen == 0 {
		return x, false
	}

	oldIndex := b.index.Load()
	newIndex := (oldIndex + 1) % bufLen
	for !b.index.CompareAndSwap(oldIndex, newIndex) {
		oldIndex = b.index.Load()
		newIndex = (oldIndex + 1) % bufLen
	}
	return b.data[oldIndex], true
}

// Push appends the element to the buffer if it does not exist.
func (b *CircularBuffer[T]) Push(value T) {
	if slices.Contains(b.data, value) {
		return
	}
	b.data = append(b.data, value)
}

// Delete removes the element from circular buffer.
func (b *CircularBuffer[T]) Delete(value T) {
	shiftedIndex := int(b.index.Load())
	removeIndex := slices.Index(b.data, value)
	// For loop is just to make sure. The `Add` method does not allow duplicates.
	for removeIndex != -1 {
		b.data = slices.Delete(b.data, removeIndex, removeIndex+1)
		if removeIndex < shiftedIndex {
			shiftedIndex--
		}
		removeIndex = slices.Index(b.data, value)
	}
	b.index.Store(int32(shiftedIndex))
}

// Len returns current length of the buffer.
func (b *CircularBuffer[T]) Len() int {
	return len(b.data)
}

// Contains returns true if the buffer contains a passed value.
func (b *CircularBuffer[T]) Contains(value T) bool {
	return slices.Contains(b.data, value)
}
