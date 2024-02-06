package discovery_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-discovery"
)

func TestMode_String(t *testing.T) {
	cases := []struct {
		Mode     discovery.Mode
		Expected string
	}{
		{discovery.ModeAll, "all"},
		{discovery.ModeRO, "ro"},
		{discovery.ModeRW, "rw"},
		{discovery.Mode(3), "Mode(3)"},
	}

	for _, tc := range cases {
		t.Run(tc.Expected, func(t *testing.T) {
			require.Equal(t, tc.Expected, tc.Mode.String())
		})
	}
}
