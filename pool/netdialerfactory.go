package pool

import (
	"fmt"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-tarantool/v2"
)

// NetDialerFactory creates network dialer object for the specified instance.
type NetDialerFactory struct {
	// username is a user name to use for connection.
	username string
	// password is a password to use for connection.
	password string
	// opts is a connection options to use for a connection.
	opts tarantool.Opts
}

// NewNetDialerFactory creates new net dialer factory for a pair
// username/password and connection options.
func NewNetDialerFactory(username, password string, opts tarantool.Opts) *NetDialerFactory {
	factory := &NetDialerFactory{
		username: username,
		password: password,
		opts:     opts,
	}
	return factory
}

// NewDialer creates new network dialer and options for it.
func (f *NetDialerFactory) NewDialer(
	instance discovery.Instance) (tarantool.Dialer, tarantool.Opts, error) {
	if len(instance.Endpoints) == 0 {
		return nil, f.opts, fmt.Errorf("%s instance URI list is empty", instance.Name)
	}

	// First look for unix-socket URI.
	var (
		uri           string
		endpointIndex int
	)
	if endpointIndex = slices.IndexFunc(instance.Endpoints,
		func(endpoint discovery.Endpoint) bool {
			return (strings.HasPrefix(endpoint.URI, "unix://") ||
				strings.HasPrefix(endpoint.URI, "unix:") ||
				strings.HasPrefix(endpoint.URI, "unix/:")) &&
				endpoint.Transport == discovery.TransportPlain
		}); endpointIndex == -1 {
		if endpointIndex = slices.IndexFunc(instance.Endpoints,
			func(endpoint discovery.Endpoint) bool {
				return endpoint.Transport == discovery.TransportPlain
			}); endpointIndex == -1 {
			return nil, f.opts,
				fmt.Errorf("%s instance does not have a plain endpoint", instance.Name)
		}
	}
	uri = instance.Endpoints[endpointIndex].URI

	return &tarantool.NetDialer{
		Address:  uri,
		User:     f.username,
		Password: f.password,
	}, f.opts, nil
}
