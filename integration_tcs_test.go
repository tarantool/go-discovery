package discovery_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/dial"
	"github.com/tarantool/go-discovery/discoverer"
	"github.com/tarantool/go-discovery/filter"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
	"github.com/tarantool/go-tarantool/v2/test_helpers/tcs"
)

func TestDiscoverer_Tcs_and_Filter(t *testing.T) {
	if ok, err := test_helpers.IsTcsSupported(); err != nil || !ok {
		t.Skip("TCS is not supported")
	}
	tcs := tcs.StartTesting(t, 0)
	defer tcs.Stop()

	err := tcs.Put(context.Background(), "/prefix/config/foo", `
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo: {}
  zoo:
    replicasets:
      any:
        instances:
          foo: {}
`)
	require.NoError(t, err)

	disc := discoverer.NewFilter(discoverer.NewTarantool(tcs.Doer(), "/prefix"),
		filter.NameOneOf{Names: []string{"foo"}})

	instances, err := disc.Discovery(context.Background())
	require.NoError(t, err)

	assert.ElementsMatch(t, []discovery.Instance{
		{
			Group:      "zoo",
			Replicaset: "any",
			Name:       "foo",
			Mode:       discovery.ModeRW,
		},
	}, instances)
}

func TestDiscoverer_Tcs_Connectable(t *testing.T) {
	if ok, err := test_helpers.IsTcsSupported(); err != nil || !ok {
		t.Skip("TCS is not supported")
	}
	defer stopTarantool(startTarantool(t))
	tcs := tcs.StartTesting(t, 0)
	defer tcs.Stop()

	err := tcs.Put(context.Background(), "/prefix/config/foo", `
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            iproto:
              advertise:
                client: 127.0.0.1:3013
  zoo:
    replicasets:
      any:
        instances:
          foo: {}
`)
	require.NoError(t, err)

	factory := dial.NewNetDialerFactory(ttUsername, ttPassword, opts)

	disc := discoverer.NewConnectable(factory,
		discoverer.NewTarantool(tcs.Doer(), "/prefix"))

	inst, err := disc.Discovery(context.Background())
	assert.NoError(t, err)

	assert.NotNil(t, inst)
	assert.Equal(t, 1, len(inst))
	assert.Equal(t, discovery.Instance{
		Group:      "foo",
		Replicaset: "bar",
		Name:       "zoo",
		Mode:       discovery.ModeRW,
		URI:        []string{"127.0.0.1:3013"},
	}, inst[0])
}
