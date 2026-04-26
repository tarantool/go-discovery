package discoverer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/discoverer"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

var _ discovery.Discoverer = discoverer.NewTarantool(nil, "foo")

const tcsPrefix = "foo"

type mockTntClient struct {
	doer *test_helpers.MockDoer
}

func (m *mockTntClient) Do(req tarantool.Request) *tarantool.Future {
	return m.doer.Do(req)
}

func (m *mockTntClient) NewWatcher(_ string, _ tarantool.WatchCallback) (tarantool.Watcher, error) {
	return nil, nil
}

func makeTcsResponse(values []string) []interface{} {
	items := make([]interface{}, 0, len(values))
	for i, value := range values {
		items = append(items, map[string]interface{}{
			"path":         []byte(fmt.Sprintf("/foo/config/config%d", i)),
			"value":        []byte(value),
			"mod_revision": 0,
		})
	}

	return []interface{}{
		map[string]interface{}{
			"data": map[string]interface{}{
				"is_success": true,
				"responses":  []interface{}{items},
			},
			"revision": 0,
		},
	}
}

func makeSingleTcsResponse(value string) []interface{} {
	return makeTcsResponse([]string{value})
}

func TestNewTarantool_missing(t *testing.T) {
	tnt := discoverer.NewTarantool(nil, "foo")
	require.NotNil(t, tnt)

	instances, err := tnt.Discovery(context.Background())
	assert.Nil(t, instances)
	assert.ErrorIs(t, err, discoverer.ErrMissingTarantool)
}

func TestTarantool_Discovery_call_args(t *testing.T) {
	doer := test_helpers.NewMockDoer(t,
		test_helpers.NewMockResponse(t, nil))
	client := &mockTntClient{doer: &doer}

	tnt := discoverer.NewTarantool(client, tcsPrefix)
	require.NotNil(t, tnt)

	instances, err := tnt.Discovery(context.Background())
	assert.Nil(t, instances)
	assert.Error(t, err)

	assert.Len(t, doer.Requests, 1)
	now := time.Now()
	deadline, ok := doer.Requests[0].Ctx().Deadline()
	require.True(t, ok)
	require.InDelta(t, deadline.Sub(now), 3*time.Second,
		float64(100*time.Millisecond))
}

func TestTarantool_Discovery_ctx_deadline(t *testing.T) {
	doer := test_helpers.NewMockDoer(t, errors.New("any"))
	client := &mockTntClient{doer: &doer}

	tnt := discoverer.NewTarantool(client, tcsPrefix)
	require.NotNil(t, tnt)

	duration := time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	instances, err := tnt.Discovery(ctx)
	assert.Nil(t, instances)
	assert.Error(t, err)

	assert.Len(t, doer.Requests, 1)
	now := time.Now()
	deadline, ok := doer.Requests[0].Ctx().Deadline()
	require.True(t, ok)
	require.InDelta(t, deadline.Sub(now), duration,
		float64(100*time.Millisecond))
}

func TestTarantool_Discovery_return_error(t *testing.T) {
	doer := test_helpers.NewMockDoer(t, errors.New("any"))
	client := &mockTntClient{doer: &doer}

	tnt := discoverer.NewTarantool(client, tcsPrefix)
	require.NotNil(t, tnt)

	instances, err := tnt.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.Len(t, doer.Requests, 1)
	assert.ErrorContains(t, err, "any")
}

func TestTarantool_Discovery_expired_context(t *testing.T) {
	doer := test_helpers.NewMockDoer(t, errors.New("any"))
	client := &mockTntClient{doer: &doer}

	tnt := discoverer.NewTarantool(client, tcsPrefix)
	require.NotNil(t, tnt)

	ctx, cancel := context.WithDeadline(context.Background(),
		time.Now().Add(-time.Second))
	defer cancel()

	instances, err := tnt.Discovery(ctx)
	assert.Nil(t, instances)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Len(t, doer.Requests, 0)
}

func TestTarantool_Discovery_invalid_data(t *testing.T) {
	doer := test_helpers.NewMockDoer(t,
		test_helpers.NewMockResponse(t, makeSingleTcsResponse("- foo\n2")))
	client := &mockTntClient{doer: &doer}

	tnt := discoverer.NewTarantool(client, tcsPrefix)
	require.NotNil(t, tnt)

	instances, err := tnt.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.ErrorContains(t, err, "failed to unmarshall")
}

func TestTarantool_Discovery_scalar_no_instances(t *testing.T) {
	// "foo" is a valid YAML scalar. It parses successfully but does not
	// contain any instances, so Discovery returns nil without an error.
	doer := test_helpers.NewMockDoer(t,
		test_helpers.NewMockResponse(t, makeSingleTcsResponse("foo")))
	client := &mockTntClient{doer: &doer}

	tnt := discoverer.NewTarantool(client, tcsPrefix)
	require.NotNil(t, tnt)

	instances, err := tnt.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.NoError(t, err)
}

func TestTarantool_Discovery_empty_storage(t *testing.T) {
	resp := test_helpers.NewMockResponse(t, makeTcsResponse(nil))
	doer := test_helpers.NewMockDoer(t, resp, resp)
	client := &mockTntClient{doer: &doer}

	tnt := discoverer.NewTarantool(client, tcsPrefix)
	require.NotNil(t, tnt)

	instances, err := tnt.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.NoError(t, err)
}

func TestTarantool_Discovery(t *testing.T) {
	cases := getDiscoveryCases()
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			resp := test_helpers.NewMockResponse(t,
				makeTcsResponse(tc.Values))
			doer := test_helpers.NewMockDoer(t, resp, resp)
			client := &mockTntClient{doer: &doer}

			tnt := discoverer.NewTarantool(client, tcsPrefix)
			require.NotNil(t, tnt)

			instances, err := tnt.Discovery(context.Background())
			assert.ElementsMatch(t, tc.Expected, instances)
			assert.NoError(t, err)
		})
	}
}
