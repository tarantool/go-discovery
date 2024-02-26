package pool

import (
	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-discovery"
)

// DoerAdapter adapts a ModeDoer to a tarantool.Doer interface. The adapter
// helps to use the Pool object as tarantool.Doer.
type DoerAdapter struct {
	doer ModeDoer
	mode discovery.Mode
}

// NewDoerAdapter creates a new DoerAdapter object for the specified mode.
func NewDoerAdapter(doer ModeDoer, mode discovery.Mode) *DoerAdapter {
	return &DoerAdapter{
		doer: doer,
		mode: mode,
	}
}

// Do executes the request with the adapter's mode.
func (d *DoerAdapter) Do(request tarantool.Request) *tarantool.Future {
	return d.doer.Do(request, d.mode)
}
