package discoverer_test

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-discovery/discoverer"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

const prefix = "foo"

type rawKV = map[string]any
type rawResponseData = map[string][]rawKV

func makeArrayResponseData(values []string) []rawResponseData {
	d := make([]rawKV, 0, len(values))
	for _, value := range values {
		d = append(d, rawKV{
			"path":         "/" + prefix + "/config",
			"value":        value,
			"mod_revision": 0,
		})
	}
	resp := rawResponseData{
		"data": d,
	}
	return []rawResponseData{resp}
}

func makeSingleResponseData(value string) []rawResponseData {
	return makeArrayResponseData([]string{value})
}

func TestNewTarantool_missing(t *testing.T) {
	tnt := discoverer.NewTarantool(nil, "foo")
	require.NotNil(t, tnt)

	instances, err := tnt.Discovery(context.Background())
	assert.Nil(t, instances)
	assert.ErrorIs(t, err, discoverer.ErrMissingTarantool)
}

func TestTarantool_TarantoolGetter_call_args(t *testing.T) {
	doer := test_helpers.NewMockDoer(t,
		test_helpers.NewMockResponse(t, nil))
	tnt := discoverer.NewTarantool(&doer, "foo")
	require.NotNil(t, tnt)

	instances, err := tnt.Discovery(context.Background())
	assert.Nil(t, instances)
	assert.Error(t, err)

	assert.Len(t, doer.Requests, 1)
	require.Equal(t,
		tarantool.NewCallRequest("config.storage.get").
			Args([]any{"foo/config/"}).
			Context(doer.Requests[0].Ctx()),
		doer.Requests[0])

	now := time.Now()
	deadline, ok := doer.Requests[0].Ctx().Deadline()
	require.True(t, ok)
	require.InDelta(t, deadline.Sub(now), 3*time.Second,
		float64(100*time.Millisecond))
}

func TestTarantool_TarantoolGetter_ctx_deadline(t *testing.T) {
	doer := test_helpers.NewMockDoer(t, errors.New("any"))

	tnt := discoverer.NewTarantool(&doer, "foo")
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

func TestTarantool_TarantoolGetter_return_error(t *testing.T) {
	doer := test_helpers.NewMockDoer(t, errors.New("any"))

	tnt := discoverer.NewTarantool(&doer, "foo")
	require.NotNil(t, tnt)

	instances, err := tnt.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.Len(t, doer.Requests, 1)
	assert.ErrorContains(t, err, "any")
}

/* FIXME: Can't do such test due to no way endless repeating `DeadlineExceeded` error.
func TestTarantool_TarantoolGetter_return_deadline_error_retry(t *testing.T) {
	doer := test_helpers.NewMockDoer(t,
		fmt.Errorf("any: %w", context.DeadlineExceeded),
		fmt.Errorf("any: %w", context.DeadlineExceeded),
		//Q: How many times will the response be repeated?
	)

	tnt := discoverer.NewTarantool(&doer, "foo")
	require.NotNil(t, tnt)

	duration := 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	before := time.Now()
	instances, err := tnt.Discovery(ctx)
	now := time.Now()

	assert.Nil(t, instances)
	assert.Greater(t, doer.Requests, 1)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.InDelta(t, now.Sub(before), duration, float64(100*time.Millisecond))
}
.*/

func TestTarantool_Discovery_invalid_data(t *testing.T) {
	doer := test_helpers.NewMockDoer(t,
		test_helpers.NewMockResponse(t, makeSingleResponseData("- foo\n2")))

	tnt := discoverer.NewTarantool(&doer, "foo")
	require.NotNil(t, tnt)

	instances, err := tnt.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.ErrorContains(t, err, "failed to decode config")
}

func TestTarantool_Discovery_invalid_cluster_config(t *testing.T) {
	doer := test_helpers.NewMockDoer(t,
		test_helpers.NewMockResponse(t, makeSingleResponseData("foo")))

	etcd := discoverer.NewTarantool(&doer, "foo")
	require.NotNil(t, etcd)

	instances, err := etcd.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.ErrorContains(t, err, "failed to unmarshal ClusterConfig")
}

func TestTarantool_Discovery(t *testing.T) {
	cases := getDiscoveryCases()
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			doer := test_helpers.NewMockDoer(t,
				test_helpers.NewMockResponse(t,
					makeArrayResponseData(tc.Values)))

			tnt := discoverer.NewTarantool(&doer, "foo")
			require.NotNil(t, tnt)

			instances, err := tnt.Discovery(context.Background())
			assert.ElementsMatch(t, tc.Expected, instances)
			assert.NoError(t, err)
		})
	}
}
