package discovery_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-discovery"
)

func TestTransport_String(t *testing.T) {
	cases := []struct {
		Transport discovery.Transport
		Expected  string
	}{
		{discovery.TransportPlain, "plain"},
		{discovery.TransportSSL, "ssl"},
		{discovery.Transport(2), "Transport(2)"},
	}

	for _, tc := range cases {
		t.Run(tc.Expected, func(t *testing.T) {
			require.Equal(t, tc.Expected, tc.Transport.String())
		})
	}
}
