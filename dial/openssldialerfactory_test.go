package dial_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/dial"
	"github.com/tarantool/go-tlsdialer"
)

var _ discovery.DialerFactory = &dial.OpenSslDialerFactory{}

func TestOpenSslDialerFactory_NewDialer(t *testing.T) {
	openSslOpts := dial.OpenSslDialerOpts{
		SslKeyFile:  "testdata/localhost.key",
		SslCertFile: "testdata/localhost.crt",
		SslCaFile:   "testdata/ca.crt",
		SslCiphers:  "ECDHE-RSA-AES256-GCM-SHA384",
	}

	tarantoolOpts := tarantool.Opts{
		Timeout: time.Second,
	}

	factory := dial.NewOpenSslDialerFactory("user", "password", openSslOpts, tarantoolOpts)
	got, gotOpts, err := factory.NewDialer(discovery.Instance{
		URI: []string{
			"localhost:3301",
			"localhost:3302",
		},
	})

	want := tlsdialer.OpenSSLDialer{
		Address:     "localhost:3301",
		User:        "user",
		Password:    "password",
		SslKeyFile:  "testdata/localhost.key",
		SslCertFile: "testdata/localhost.crt",
		SslCaFile:   "testdata/ca.crt",
		SslCiphers:  "ECDHE-RSA-AES256-GCM-SHA384",
	}

	assert.NoError(t, err)
	gotNet, ok := got.(*tlsdialer.OpenSSLDialer)
	require.True(t, ok)
	assert.Equal(t, want, *gotNet)
	assert.Equal(t, tarantoolOpts, gotOpts)
}
