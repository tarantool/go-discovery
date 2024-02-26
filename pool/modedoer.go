package pool

import (
	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-discovery"
)

// ModeDoer is an interface to execute a request on an instance with the
// specified mode.
type ModeDoer interface {
	// Do executes the request on an instance with the specified mode.
	Do(tarantool.Request, discovery.Mode) *tarantool.Future
}
