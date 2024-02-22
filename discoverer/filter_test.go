package discoverer_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/discoverer"
)

var _ discovery.Discoverer = discoverer.NewFilter(nil)

type mockDiscoverer struct {
	instances []discovery.Instance
}

func (d *mockDiscoverer) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return d.instances, nil
	}
}

func TestFilter_Discovery(t *testing.T) {
	instances := []discovery.Instance{
		{
			Name: "Woods",
		},
		{
			Name: "Bishop",
		},
	}

	cases := []struct {
		name    string
		filter  discovery.Filter
		instOut []discovery.Instance
	}{
		{
			name: "Keep all",
			filter: discovery.FilterFunc(func(inst discovery.Instance) bool {
				return inst.Name == "Woods" || inst.Name == "Bishop"
			}),
			instOut: instances,
		},
		{
			name: "Filter some",
			filter: discovery.FilterFunc(func(inst discovery.Instance) bool {
				return inst.Name == "Bishop"
			}),
			instOut: []discovery.Instance{
				{
					Name: "Bishop",
				},
			},
		},
		{
			name: "Filter all",
			filter: discovery.FilterFunc(func(_ discovery.Instance) bool {
				return false
			}),
			instOut: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			innerDisc := &mockDiscoverer{instances: instances}
			ctx := context.Background()

			disc := discoverer.NewFilter(innerDisc, tc.filter)
			retInst, err := disc.Discovery(ctx)

			assert.NoError(t, err)
			assert.Equal(t, tc.instOut, retInst)
		})
	}
}

func TestFilter_Discovery_multipleFilters(t *testing.T) {
	instances := []discovery.Instance{
		{
			Group: "first",
			Name:  "Woods",
		},
		{
			Group: "first",
			Name:  "Bishop",
		},
		{
			Group: "second",
			Name:  "Woods",
		},
		{
			Group: "second",
			Name:  "Bishop",
		},
	}

	filterGroup := discovery.FilterFunc(func(inst discovery.Instance) bool {
		return inst.Group == "second"
	})
	filterName := discovery.FilterFunc(func(inst discovery.Instance) bool {
		return inst.Name == "Woods"
	})

	innerDisc := &mockDiscoverer{instances: instances}
	ctx := context.Background()

	disc := discoverer.NewFilter(innerDisc, filterGroup, filterName)

	retInst, err := disc.Discovery(ctx)

	assert.NoError(t, err)
	assert.Equal(t, []discovery.Instance{instances[2]}, retInst)
}

func TestFilter_DiscoveryContextCancel(t *testing.T) {
	instances := []discovery.Instance{
		{},
	}
	innerDisc := &mockDiscoverer{instances: instances}

	disc := discoverer.NewFilter(innerDisc)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	retInst, err := disc.Discovery(ctx)

	assert.Nil(t, retInst)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}

func TestFilter_Discovery_nilDiscoverer(t *testing.T) {
	disc := discoverer.NewFilter(nil)
	assert.NotNil(t, disc)

	instances, err := disc.Discovery(context.Background())
	assert.Nil(t, instances)
	assert.Equal(t, discoverer.ErrMissingDiscoverer, err)
}
