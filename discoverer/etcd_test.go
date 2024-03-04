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
	cases := []struct {
		Name     string
		Values   []string
		Expected []discovery.Instance
		Err      string
	}{
		{
			Name:     "nil",
			Values:   nil,
			Expected: nil,
		},
		{
			Name:     "empty",
			Values:   []string{""},
			Expected: nil,
		},
		{
			Name: "no instances",
			Values: []string{
				`groups:
  foo:
    replicasets:
      bar:
        instances: {}
`},
		},
		{
			Name: "merge configs",
			Values: []string{
				`groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo: {}
`,
				`groups:
  foo:
    replicasets:
      zoo:
        instances:
          car: {}
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRW,
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "zoo",
					Name:       "car",
					Mode:       discovery.ModeRW,
				},
			},
		},
		{
			Name: "default mode single",
			Values: []string{
				`groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo: {}
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRW,
				},
			},
		},
		{
			Name: "off failover mode single",
			Values: []string{
				`
replication:
  failover: "off"
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo: {}
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRW,
				},
			},
		},
		{
			Name: "default mode multiple",
			Values: []string{
				`groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo: {}
          car: {}
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "car",
					Mode:       discovery.ModeRO,
				},
			},
		},
		{
			Name: "off failover multiple failover off",
			Values: []string{
				`
replication:
  failover: "off"
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo: {}
          car: {}
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "car",
					Mode:       discovery.ModeRO,
				},
			},
		},
		{
			Name: "default mode database mode set",
			Values: []string{
				`
database:
  mode: "rw"
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            database:
              mode: ro
          car: {}
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "car",
					Mode:       discovery.ModeRW,
				},
			},
		},
		{
			Name: "off failover database mode set",
			Values: []string{
				`
replication:
  failover: "off"
database:
  mode: "rw"
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            database:
              mode: ro
          car: {}
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "car",
					Mode:       discovery.ModeRW,
				},
			},
		},
		{
			Name: "manual failover mode",
			Values: []string{
				`
replication:
  failover: "manual"
database:
  mode: "rw"
leader: "zoo"
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            database:
              mode: ro
          car: {}
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRW,
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "car",
					Mode:       discovery.ModeRO,
				},
			},
		},
		{
			Name: "other failover mode",
			Values: []string{
				`
replication:
  failover: "any other"
database:
  mode: "rw"
leader: "zoo"
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            database:
              mode: ro
          car: {}
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeAny,
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "car",
					Mode:       discovery.ModeAny,
				},
			},
		},
		{
			Name: "uri",
			Values: []string{
				`
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            iproto:
              advertise:
                client: "foo"
                peer:
                  params:
                    transport: "ssl"
          car:
            iproto:
              advertise:
                client: "zoo"
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
					URI:        []string{"foo"},
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "car",
					Mode:       discovery.ModeRO,
					URI:        []string{"zoo"},
				},
			},
		},
		{
			Name: "roles",
			Values: []string{
				`
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            roles:
            - foo
            - bar
          car:
            roles:
            - zoo
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
					Roles:      []string{"foo", "bar"},
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "car",
					Mode:       discovery.ModeRO,
					Roles:      []string{"zoo"},
				},
			},
		},
		{
			Name: "roles tags",
			Values: []string{
				`
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            roles_cfg:
              tags:
              - foo
              - bar
          car:
            roles_cfg:
              tags:
              - zoo
          tar:
            roles_cfg:
              tags:
              - 2.2
          no:
            roles_cfg:
              tags: "something"
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
					RolesTags:  []string{"foo", "bar"},
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "car",
					Mode:       discovery.ModeRO,
					RolesTags:  []string{"zoo"},
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "tar",
					Mode:       discovery.ModeRO,
					RolesTags:  []string{"2.2"},
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "no",
					Mode:       discovery.ModeRO,
					RolesTags:  nil,
				},
			},
		},
		{
			Name: "app tags",
			Values: []string{
				`
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            app:
              cfg:
                tags:
                - foo
                - bar
          car:
            app:
              cfg:
                tags:
                - zoo
          tar:
            app:
              cfg:
                tags:
                - 2.2
          no:
            app:
              cfg:
                tags: "something"
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
					AppTags:    []string{"foo", "bar"},
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "car",
					Mode:       discovery.ModeRO,
					AppTags:    []string{"zoo"},
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "tar",
					Mode:       discovery.ModeRO,
					AppTags:    []string{"2.2"},
				},
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "no",
					Mode:       discovery.ModeRO,
					AppTags:    nil,
				},
			},
		},
		{
			Name: "full set of params",
			Values: []string{
				`
database:
  mode: ro
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            iproto:
              advertise:
                client: "localhost:3011"
            roles: [crud]
            roles_cfg:
              tags:
              - any
              - bar
              - 3
            app:
              cfg:
                tags:
                - foo
                - bar
`},
			Expected: []discovery.Instance{
				discovery.Instance{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
					URI: []string{
						"localhost:3011",
					},
					Roles:     []string{"crud"},
					RolesTags: []string{"any", "bar", "3"},
					AppTags:   []string{"foo", "bar"},
				},
			},
		},
	}

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
