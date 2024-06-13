package dial

import (
	"fmt"

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
	if len(instance.URI) == 0 {
		return nil, f.opts,
			fmt.Errorf("%s instance URI list is empty", instance.Name)
	}

	var dialers []tarantool.Dialer
	for _, uri := range instance.URI {
		dialers = append(dialers, &tarantool.NetDialer{
			Address:  uri,
			User:     f.username,
			Password: f.password,
		})
	}

	return CompositeDialer{
		Dialers: dialers,
	}, f.opts, nil
}
