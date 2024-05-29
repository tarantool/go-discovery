package dial_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/dial"
)

var _ discovery.DialerFactory = &dial.NetDialerFactory{}

func TestNetDialerFactory_NewDialer(t *testing.T) {
	opts := tarantool.Opts{
		Timeout: time.Second,
	}

	factory := dial.NewNetDialerFactory("user", "password", opts)
	got, gotOpts, err := factory.NewDialer(discovery.Instance{
		URI: []string{
			"localhost:3301",
			"localhost:3302",
		},
	})
	want := tarantool.NetDialer{
		Address:  "localhost:3301",
		User:     "user",
		Password: "password",
	}

	assert.NoError(t, err)
	gotNet, ok := got.(*tarantool.NetDialer)
	require.True(t, ok)
	assert.Equal(t, want, *gotNet)
	assert.Equal(t, opts, gotOpts)
}
