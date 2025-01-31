package discoverer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/discoverer"
)

var _ discovery.Discoverer = discoverer.NewEtcd(nil, "foo")

type etcdGetterMock struct {
	clientv3.KV
	Ctx      context.Context
	Key      string
	Opts     []clientv3.OpOption
	Calls    int
	Response *clientv3.GetResponse
	Err      error
}

func (m *etcdGetterMock) Get(ctx context.Context, key string,
	opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	m.Calls++
	m.Ctx = ctx
	m.Key = key
	m.Opts = opts
	return m.Response, m.Err
}

func TestNewEtcd_missing(t *testing.T) {
	etcd := discoverer.NewEtcd(nil, "foo")
	require.NotNil(t, etcd)

	instances, err := etcd.Discovery(context.Background())
	assert.Nil(t, instances)
	assert.ErrorIs(t, err, discoverer.ErrMissingEtcd)
}

func TestEtcd_EtcdGetter_call_args(t *testing.T) {
	mock := etcdGetterMock{
		Err: fmt.Errorf("any"),
	}

	etcd := discoverer.NewEtcd(&mock, "foo")
	require.NotNil(t, etcd)

	instances, err := etcd.Discovery(context.Background())
	assert.Nil(t, instances)
	assert.Error(t, err)

	assert.Equal(t, "foo/config/", mock.Key)
	assert.Len(t, mock.Opts, 1)
	now := time.Now()
	deadline, ok := mock.Ctx.Deadline()
	require.True(t, ok)
	require.InDelta(t, deadline.Sub(now), 3*time.Second,
		float64(100*time.Millisecond))
}

func TestEtcd_EtcdGetter_ctx_deadline(t *testing.T) {
	mock := etcdGetterMock{
		Err: fmt.Errorf("any"),
	}

	etcd := discoverer.NewEtcd(&mock, "foo")
	require.NotNil(t, etcd)

	var duration = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	instances, err := etcd.Discovery(ctx)
	assert.Nil(t, instances)
	assert.Error(t, err)

	now := time.Now()
	deadline, ok := mock.Ctx.Deadline()
	require.True(t, ok)
	require.InDelta(t, deadline.Sub(now), duration,
		float64(100*time.Millisecond))
}

func TestEtcd_EtcdGetter_return_error(t *testing.T) {
	mock := etcdGetterMock{
		Err: fmt.Errorf("any"),
	}

	etcd := discoverer.NewEtcd(&mock, "foo")
	require.NotNil(t, etcd)

	instances, err := etcd.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.Equal(t, 1, mock.Calls)
	assert.ErrorContains(t, err, "any")
}

func TestEtcd_EtcdGetter_return_deadline_error_retry(t *testing.T) {
	mock := etcdGetterMock{
		Err: fmt.Errorf("any: %w", context.DeadlineExceeded),
	}

	etcd := discoverer.NewEtcd(&mock, "foo")
	require.NotNil(t, etcd)

	var duration = 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	before := time.Now()
	instances, err := etcd.Discovery(ctx)
	now := time.Now()

	assert.Nil(t, instances)
	assert.Greater(t, mock.Calls, 1)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.InDelta(t, now.Sub(before), duration, float64(100*time.Millisecond))
}

func TestEtcd_Discovery_invalid_data(t *testing.T) {
	mock := etcdGetterMock{
		Response: &clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				&mvccpb.KeyValue{
					Value: []byte("- foo\n2"),
				},
			},
		},
	}

	etcd := discoverer.NewEtcd(&mock, "foo")
	require.NotNil(t, etcd)

	instances, err := etcd.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.ErrorContains(t, err, "failed to decode config")
}

func TestEtcd_Discovery_invalid_cluster_config(t *testing.T) {
	mock := etcdGetterMock{
		Response: &clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				&mvccpb.KeyValue{
					Value: []byte("foo"),
				},
			},
		},
	}

	etcd := discoverer.NewEtcd(&mock, "foo")
	require.NotNil(t, etcd)

	instances, err := etcd.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.ErrorContains(t, err, "failed to unmarshal ClusterConfig")
}

func TestEtcd_Discovery(t *testing.T) {
	cases := getDiscoveryCases()
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			var kvs []*mvccpb.KeyValue
			for _, value := range tc.Values {
				kvs = append(kvs, &mvccpb.KeyValue{Value: []byte(value)})
			}

			mock := etcdGetterMock{
				Response: &clientv3.GetResponse{
					Kvs: kvs,
				},
			}

			etcd := discoverer.NewEtcd(&mock, "foo")
			require.NotNil(t, etcd)

			instances, err := etcd.Discovery(context.Background())
			assert.ElementsMatch(t, tc.Expected, instances)
			assert.NoError(t, err)
		})
	}
}
