package pool

import (
	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-tarantool/v2"
)

// DialerFactory is an interface that wraps the method for creating tarantool dialers.
type DialerFactory interface {
	// NewDialer creates new tarantool dialer.
	NewDialer(instance discovery.Instance) (tarantool.Dialer, error)
}
