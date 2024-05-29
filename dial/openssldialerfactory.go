package dial

import (
	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/dial/internal/parse"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tlsdialer"
)

// OpenSslDialerFactory creates OpenSSL dialer object for the specified instance.
type OpenSslDialerFactory struct {
	// username for logging in to Tarantool.
	username string
	// password for logging in to Tarantool.
	password string

	// openSslOpts is options related to setting up an OpenSSL connection to
	// a Tarantool instance.
	openSslOpts OpenSslDialerOpts

	// tarantoolOpts is a connection options to use for a connection.
	tarantoolOpts tarantool.Opts
}

type OpenSslDialerOpts struct {
	// auth is an authentication method.
	Auth tarantool.Auth
	// sslKeyFile is a path to a private SSL key file.
	SslKeyFile string
	// sslCertFile is a path to an SSL certificate file.
	SslCertFile string
	// sslCaFile is a path to a trusted certificate authorities (CA) file.
	SslCaFile string
	// sslCiphers is a colon-separated (:) list of SSL cipher suites the connection
	// can use.
	//
	// We don't provide a list of supported ciphers. This is what OpenSSL
	// does. The only limitation is usage of TLSv1.2 (because other protocol
	// versions don't seem to support the GOST cipher). To add additional
	// ciphers (GOST cipher), you must configure OpenSSL.
	//
	// See also
	//
	// * https://www.openssl.org/docs/man1.1.1/man1/ciphers.html
	SslCiphers string
	// sslPassword is a password for decrypting the private SSL key file.
	// The priority is as follows: try to decrypt with SslPassword, then
	// try SslPasswordFile.
	SslPassword string
	// sslPasswordFile is a path to the list of passwords for decrypting
	// the private SSL key file. The connection tries every line from the
	// file as a password.
	SslPasswordFile string
}

// NewOpenSslDialerFactory creates new OpenSSL dialer factory for a pair
// username/password, auth, ssl certs and connection options.
func NewOpenSslDialerFactory(
	username string,
	password string,
	openSslOpts OpenSslDialerOpts,
	tarantoolOpts tarantool.Opts,
) *OpenSslDialerFactory {
	factory := &OpenSslDialerFactory{
		username:      username,
		password:      password,
		openSslOpts:   openSslOpts,
		tarantoolOpts: tarantoolOpts,
	}
	return factory
}

// NewDialer creates new OpenSSL dialer and options for it.
func (f *OpenSslDialerFactory) NewDialer(
	instance discovery.Instance) (tarantool.Dialer, tarantool.Opts, error) {
	uri, err := parse.GetURI(instance)
	if err != nil {
		return nil, f.tarantoolOpts, err
	}

	return &tlsdialer.OpenSSLDialer{
		Address:         uri,
		User:            f.username,
		Password:        f.password,
		Auth:            f.openSslOpts.Auth,
		SslKeyFile:      f.openSslOpts.SslKeyFile,
		SslCertFile:     f.openSslOpts.SslCertFile,
		SslCaFile:       f.openSslOpts.SslCaFile,
		SslCiphers:      f.openSslOpts.SslCiphers,
		SslPassword:     f.openSslOpts.SslPassword,
		SslPasswordFile: f.openSslOpts.SslPasswordFile,
	}, f.tarantoolOpts, nil
}
