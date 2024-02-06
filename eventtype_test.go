package discovery_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-discovery"
)

func TestEventType_String(t *testing.T) {
	cases := []struct {
		EventType discovery.EventType
		Expected  string
	}{
		{discovery.EventTypeAdd, "add"},
		{discovery.EventTypeUpdate, "update"},
		{discovery.EventTypeRemove, "remove"},
		{discovery.EventType(3), "EventType(3)"},
	}

	for _, tc := range cases {
		t.Run(tc.Expected, func(t *testing.T) {
			require.Equal(t, tc.Expected, tc.EventType.String())
		})
	}
}
