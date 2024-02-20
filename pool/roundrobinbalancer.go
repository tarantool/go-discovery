package pool

import (
	"sync"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/pool/internal/container/buffer"
)

// RoundRobinBalancer implements round-robin logic for getting an instance.
type RoundRobinBalancer struct {
	mu              sync.RWMutex
	instancesByMode map[discovery.Mode]*buffer.CircularBuffer[string]
}

// NewRoundRobinBalancer creates new round-robin balancer.
func NewRoundRobinBalancer() *RoundRobinBalancer {
	return &RoundRobinBalancer{
		instancesByMode: map[discovery.Mode]*buffer.CircularBuffer[string]{
			discovery.ModeAll: buffer.NewCircular[string](),
			discovery.ModeRO:  buffer.NewCircular[string](),
			discovery.ModeRW:  buffer.NewCircular[string](),
		},
	}
}

// Remove removes instance name from the balancer.
func (b *RoundRobinBalancer) Remove(instNameToRemove string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, instances := range b.instancesByMode {
		instances.Delete(instNameToRemove)
	}
}

// Add adds new instance name to the balancer.
func (b *RoundRobinBalancer) Add(instance discovery.Instance) {
	b.mu.Lock()
	defer b.mu.Unlock()

	instances := b.instancesByMode[instance.Mode]
	if instances.Contains(instance.Name) {
		return // Already added.
	}

	// Remove instance name from other modes.
	switch instance.Mode {
	case discovery.ModeRO:
		b.instancesByMode[discovery.ModeRW].Delete(instance.Name)
	case discovery.ModeRW:
		b.instancesByMode[discovery.ModeRO].Delete(instance.Name)
	}

	instances.Push(instance.Name)
	b.instancesByMode[discovery.ModeAll].Push(instance.Name)
}

// Next returns next instance.
func (b *RoundRobinBalancer) Next(mode discovery.Mode) (string, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.instancesByMode[mode].GetNext()
}
