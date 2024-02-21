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
}

// NewNetDialerFactory creates new net dialer factory.
func NewNetDialerFactory(username, password string) *NetDialerFactory {
	factory := &NetDialerFactory{
		username: username,
		password: password,
	}
	return factory
}

// NewDialer creates new network dialer.
func (f *NetDialerFactory) NewDialer(instance discovery.Instance) (tarantool.Dialer, error) {
	if len(instance.URI) == 0 {
		return nil, fmt.Errorf("%s instance URI list is empty", instance.Name)
	}

	// First look for unix-socket URI.
	var uri string
	if uriIndex := slices.IndexFunc(instance.URI, func(uri string) bool {
		return strings.HasPrefix(uri, "unix://") ||
			strings.HasPrefix(uri, "unix:") ||
			strings.HasPrefix(uri, "unix/:")
	}); uriIndex == -1 {
		uri = instance.URI[0]
	} else {
		uri = instance.URI[uriIndex]
	}

	return &tarantool.NetDialer{
		Address:  uri,
		User:     f.username,
		Password: f.password,
	}, nil
}
