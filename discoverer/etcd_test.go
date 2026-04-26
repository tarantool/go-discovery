package discoverer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/discoverer"
)

var _ discovery.Discoverer = discoverer.NewEtcd(nil, "foo")

type mockTxn struct {
	cmps    []clientv3.Cmp
	thenOps []clientv3.Op
	elseOps []clientv3.Op
	commit  func() (*clientv3.TxnResponse, error)
}

func (m *mockTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	m.cmps = append(m.cmps, cs...)
	return m
}

func (m *mockTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	m.thenOps = append(m.thenOps, ops...)
	return m
}

func (m *mockTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	m.elseOps = append(m.elseOps, ops...)
	return m
}

func (m *mockTxn) Commit() (*clientv3.TxnResponse, error) {
	return m.commit()
}

type mockEtcdClient struct {
	ctx   context.Context
	txn   *mockTxn
	calls int
}

func (m *mockEtcdClient) Txn(ctx context.Context) clientv3.Txn {
	m.calls++
	m.ctx = ctx
	return m.txn
}

func (m *mockEtcdClient) Watch(_ context.Context, _ string,
	_ ...clientv3.OpOption) clientv3.WatchChan {
	return nil
}

func makeEtcdTxnResponse(kvs []*clientv3.GetResponse) *clientv3.TxnResponse {
	responses := make([]*etcdserverpb.ResponseOp, 0, len(kvs))
	for _, getResp := range kvs {
		responses = append(responses, &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponseRange{
				ResponseRange: (*etcdserverpb.RangeResponse)(getResp),
			},
		})
	}
	return &clientv3.TxnResponse{
		Succeeded: true,
		Responses: responses,
	}
}

func TestNewEtcd_missing(t *testing.T) {
	etcd := discoverer.NewEtcd(nil, "foo")
	require.NotNil(t, etcd)

	instances, err := etcd.Discovery(context.Background())
	assert.Nil(t, instances)
	assert.ErrorIs(t, err, discoverer.ErrMissingEtcd)
}

func TestEtcd_Discovery_call_args(t *testing.T) {
	txn := &mockTxn{
		commit: func() (*clientv3.TxnResponse, error) {
			return nil, fmt.Errorf("any")
		},
	}
	mock := &mockEtcdClient{txn: txn}

	etcd := discoverer.NewEtcd(mock, "foo")
	require.NotNil(t, etcd)

	instances, err := etcd.Discovery(context.Background())
	assert.Nil(t, instances)
	assert.Error(t, err)

	assert.Equal(t, 1, mock.calls)
	now := time.Now()
	deadline, ok := mock.ctx.Deadline()
	require.True(t, ok)
	require.InDelta(t, deadline.Sub(now), 3*time.Second,
		float64(100*time.Millisecond))
}

func TestEtcd_Discovery_ctx_deadline(t *testing.T) {
	txn := &mockTxn{
		commit: func() (*clientv3.TxnResponse, error) {
			return nil, fmt.Errorf("any")
		},
	}
	mock := &mockEtcdClient{txn: txn}

	etcd := discoverer.NewEtcd(mock, "foo")
	require.NotNil(t, etcd)

	duration := time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	instances, err := etcd.Discovery(ctx)
	assert.Nil(t, instances)
	assert.Error(t, err)

	now := time.Now()
	deadline, ok := mock.ctx.Deadline()
	require.True(t, ok)
	require.InDelta(t, deadline.Sub(now), duration,
		float64(100*time.Millisecond))
}

func TestEtcd_Discovery_return_error(t *testing.T) {
	txn := &mockTxn{
		commit: func() (*clientv3.TxnResponse, error) {
			return nil, fmt.Errorf("any")
		},
	}
	mock := &mockEtcdClient{txn: txn}

	etcd := discoverer.NewEtcd(mock, "foo")
	require.NotNil(t, etcd)

	instances, err := etcd.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.Equal(t, 1, mock.calls)
	assert.ErrorContains(t, err, "any")
}

func TestEtcd_Discovery_deadline_retry(t *testing.T) {
	txn := &mockTxn{
		commit: func() (*clientv3.TxnResponse, error) {
			return nil, fmt.Errorf("any: %w", context.DeadlineExceeded)
		},
	}
	mock := &mockEtcdClient{txn: txn}

	etcd := discoverer.NewEtcd(mock, "foo")
	require.NotNil(t, etcd)

	duration := 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	before := time.Now()
	instances, err := etcd.Discovery(ctx)
	now := time.Now()

	assert.Nil(t, instances)
	assert.Greater(t, mock.calls, 1)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.InDelta(t, now.Sub(before), duration, float64(100*time.Millisecond))
}

func TestEtcd_Discovery_expired_context(t *testing.T) {
	txn := &mockTxn{
		commit: func() (*clientv3.TxnResponse, error) {
			return nil, fmt.Errorf("any")
		},
	}
	mock := &mockEtcdClient{txn: txn}

	etcd := discoverer.NewEtcd(mock, "foo")
	require.NotNil(t, etcd)

	ctx, cancel := context.WithDeadline(context.Background(),
		time.Now().Add(-time.Second))
	defer cancel()

	instances, err := etcd.Discovery(ctx)
	assert.Nil(t, instances)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 0, mock.calls)
}

func TestEtcd_Discovery_invalid_data(t *testing.T) {
	kvs := []*mvccpb.KeyValue{
		{
			Key:   []byte("/foo/config/config0"),
			Value: []byte("- foo\n2"),
		},
	}
	getResp := &clientv3.GetResponse{Kvs: kvs}
	txn := &mockTxn{
		commit: func() (*clientv3.TxnResponse, error) {
			return makeEtcdTxnResponse([]*clientv3.GetResponse{getResp}), nil
		},
	}
	mock := &mockEtcdClient{txn: txn}

	etcd := discoverer.NewEtcd(mock, "foo")
	require.NotNil(t, etcd)

	instances, err := etcd.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.ErrorContains(t, err, "failed to unmarshall")
}

func TestEtcd_Discovery_scalar_no_instances(t *testing.T) {
	// "foo" is a valid YAML scalar. It parses successfully but does not
	// contain any instances, so Discovery returns nil without an error.
	kvs := []*mvccpb.KeyValue{
		{
			Key:   []byte("/foo/config/config0"),
			Value: []byte("foo"),
		},
	}
	getResp := &clientv3.GetResponse{Kvs: kvs}
	txn := &mockTxn{
		commit: func() (*clientv3.TxnResponse, error) {
			return makeEtcdTxnResponse([]*clientv3.GetResponse{getResp}), nil
		},
	}
	mock := &mockEtcdClient{txn: txn}

	etcd := discoverer.NewEtcd(mock, "foo")
	require.NotNil(t, etcd)

	instances, err := etcd.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.NoError(t, err)
}

func TestEtcd_Discovery_empty_storage(t *testing.T) {
	getResp := &clientv3.GetResponse{Kvs: nil}
	txn := &mockTxn{
		commit: func() (*clientv3.TxnResponse, error) {
			return makeEtcdTxnResponse([]*clientv3.GetResponse{getResp}), nil
		},
	}
	mock := &mockEtcdClient{txn: txn}

	etcd := discoverer.NewEtcd(mock, "foo")
	require.NotNil(t, etcd)

	instances, err := etcd.Discovery(context.Background())

	assert.Nil(t, instances)
	assert.NoError(t, err)
}

func TestEtcd_Discovery(t *testing.T) {
	cases := getDiscoveryCases()
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			var kvs []*mvccpb.KeyValue
			for i, value := range tc.Values {
				kvs = append(kvs, &mvccpb.KeyValue{
					Key:   []byte(fmt.Sprintf("/foo/config/config%d", i)),
					Value: []byte(value),
				})
			}
			getResp := &clientv3.GetResponse{Kvs: kvs}
			txn := &mockTxn{
				commit: func() (*clientv3.TxnResponse, error) {
					return makeEtcdTxnResponse([]*clientv3.GetResponse{getResp}), nil
				},
			}
			mock := &mockEtcdClient{txn: txn}

			etcd := discoverer.NewEtcd(mock, "foo")
			require.NotNil(t, etcd)

			instances, err := etcd.Discovery(context.Background())
			assert.ElementsMatch(t, tc.Expected, instances)
			assert.NoError(t, err)
		})
	}
}
