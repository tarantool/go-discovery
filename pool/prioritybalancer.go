package pool

import (
	"sync"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/pool/internal/container/buffer"
)

// priorityFunc returns a priority for the passed instance.
type priorityFunc func(instance discovery.Instance) int

// PriorityBalancer implements priority-based round-robin logic for getting an instance.
type PriorityBalancer struct {
	mu              sync.RWMutex
	instancesByMode map[discovery.Mode]*buffer.PriorityCircular[string]
	priorityFunc    priorityFunc
}

// NewPriorityBalancer creates new priority balancer.
func NewPriorityBalancer(priorityFunc func(discovery.Instance) int) *PriorityBalancer {
	return &PriorityBalancer{
		instancesByMode: map[discovery.Mode]*buffer.PriorityCircular[string]{
			discovery.ModeAny: buffer.NewPriorityCircular[string](),
			discovery.ModeRO:  buffer.NewPriorityCircular[string](),
			discovery.ModeRW:  buffer.NewPriorityCircular[string](),
		},
		priorityFunc: priorityFunc,
	}
}

// Remove removes instance name from the balancer.
func (b *PriorityBalancer) Remove(instNameToRemove string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, instances := range b.instancesByMode {
		instances.Delete(instNameToRemove)
	}
}

// Add adds a new instance name to the balancer.
func (b *PriorityBalancer) Add(instance discovery.Instance) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Remove instance name from other modes.
	switch instance.Mode {
	case discovery.ModeRO:
		b.instancesByMode[discovery.ModeRW].Delete(instance.Name)
	case discovery.ModeRW:
		b.instancesByMode[discovery.ModeRO].Delete(instance.Name)
	}

	instancePriority := b.priorityFunc(instance)
	b.instancesByMode[instance.Mode].Push(instance.Name, instancePriority)
	b.instancesByMode[discovery.ModeAny].Push(instance.Name, instancePriority)
	return nil
}

// Next returns a next highest priority instance of the specified mode.
func (b *PriorityBalancer) Next(mode discovery.Mode) (string, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.instancesByMode[mode].GetNext()
}
