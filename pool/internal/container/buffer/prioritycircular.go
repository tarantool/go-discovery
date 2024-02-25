package buffer

import "golang.org/x/exp/slices"

// priorityGroup is a container for a circular buffer of elements of one priority.
type priorityGroup[T comparable] struct {
	// priority is a priority of all items in buf.
	priority int
	// buf is buffer containing items.
	buf *CircularBuffer[T]
}

// PriorityCircular is a priority circular container.
type PriorityCircular[T comparable] struct {
	// priorities is a list of items grouped by priorities.
	priorities []priorityGroup[T]
}

// NewPriorityCircular creates new priority circular buffer.
func NewPriorityCircular[T comparable]() *PriorityCircular[T] {
	return &PriorityCircular[T]{
		priorities: make([]priorityGroup[T], 0),
	}
}

// GetNext returns a next element of the highest priority in circular manner.
func (b *PriorityCircular[T]) GetNext() (x T, ret bool) {
	if len(b.priorities) == 0 {
		return
	}
	return b.priorities[0].buf.GetNext()
}

// Push appends the element to the buffer keeping the elements sorted.
func (b *PriorityCircular[T]) Push(value T, priority int) {
	index, found := slices.BinarySearchFunc(b.priorities, priority,
		func(i1 priorityGroup[T], _ int) int {
			return priority - i1.priority
		})
	if found {
		// Remove the value from all priorities, except found.
		b.priorities[index].buf.Push(value)
		b.priorities = slices.DeleteFunc(b.priorities, func(instances priorityGroup[T]) bool {
			if instances.priority != priority {
				instances.buf.Delete(value)
			}
			return instances.buf.Len() == 0
		})
	} else {
		b.Delete(value)
		newBuf := NewCircular[T]()
		newBuf.Push(value)
		b.priorities = slices.Insert(b.priorities, index, priorityGroup[T]{priority, newBuf})
	}
}

// Delete removes an element from the priority buffer.
func (b *PriorityCircular[T]) Delete(value T) {
	b.priorities = slices.DeleteFunc(b.priorities, func(instances priorityGroup[T]) bool {
		instances.buf.Delete(value)
		return instances.buf.Len() == 0
	})
}
