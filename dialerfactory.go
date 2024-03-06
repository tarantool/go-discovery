package discovery

import (
	"github.com/tarantool/go-tarantool/v2"
)

// DialerFactory is an interface that wraps the method for creating a tarantool
// connection dialer and connection options.
type DialerFactory interface {
	// NewDialer creates a new tarantool connection dialer and connection
	// options.
	NewDialer(instance Instance) (tarantool.Dialer, tarantool.Opts, error)
}
