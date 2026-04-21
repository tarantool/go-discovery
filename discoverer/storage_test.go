package discoverer_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/discoverer"
	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/driver/etcd"
	"github.com/tarantool/go-storage/kv"
	"github.com/tarantool/go-storage/operation"
	"github.com/tarantool/go-storage/predicate"
	"github.com/tarantool/go-storage/tx"
	"github.com/tarantool/go-storage/watch"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

// mockTx is a mock implementation of tx.Tx interface.
type mockTx struct {
	data []kv.KeyValue
	err  error
}

func (m *mockTx) If(_ ...predicate.Predicate) tx.Tx {
	return m
}

func (m *mockTx) Then(_ ...operation.Operation) tx.Tx {
	return m
}

func (m *mockTx) Else(_ ...operation.Operation) tx.Tx {
	return m
}

func (m *mockTx) Commit() (tx.Response, error) {
	if m.err != nil {
		return tx.Response{}, m.err
	}
	return tx.Response{
		Succeeded: true,
		Results: []tx.RequestResponse{
			{Values: m.data},
		},
	}, nil
}

// mockStorage is a mock implementation of storage.Storage interface.
type mockStorage struct {
	data []kv.KeyValue
	err  error
}

func (m *mockStorage) Watch(_ context.Context, _ []byte, _ ...watch.Option) <-chan watch.Event {
	return nil
}

func (m *mockStorage) Tx(_ context.Context) tx.Tx {
	return &mockTx{data: m.data, err: m.err}
}

func (m *mockStorage) Range(_ context.Context, _ ...storage.RangeOption) ([]kv.KeyValue, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.data, nil
}

func TestStorage_Discovery_mock_storage(t *testing.T) {
	configYAML := `
groups:
  group1:
    replicasets:
      repl1:
        instances:
          inst1: {}
`
	mock := &mockStorage{
		data: []kv.KeyValue{
			{
				Key:   []byte("/test-prefix/config/config1"),
				Value: []byte(configYAML),
			},
		},
	}

	sd := discoverer.NewStorageDiscoverer(mock, "/test-prefix/")

	instances, err := sd.Discovery(context.Background())
	require.NoError(t, err)

	expected := []discovery.Instance{
		{
			Group:      "group1",
			Replicaset: "repl1",
			Name:       "inst1",
			Mode:       discovery.ModeRW,
		},
	}
	assert.ElementsMatch(t, expected, instances)
}

func TestStorage_Discovery_multiple_configs(t *testing.T) {
	config1YAML := `
groups:
  group1:
    replicasets:
      repl1:
        instances:
          inst1: {}
`
	config2YAML := `
groups:
  group2:
    replicasets:
      repl2:
        instances:
          inst2: {}
`
	mock := &mockStorage{
		data: []kv.KeyValue{
			{
				Key:   []byte("/test-prefix/config/config1"),
				Value: []byte(config1YAML),
			},
			{
				Key:   []byte("/test-prefix/config/config2"),
				Value: []byte(config2YAML),
			},
		},
	}

	sd := discoverer.NewStorageDiscoverer(mock, "/test-prefix/")

	instances, err := sd.Discovery(context.Background())
	require.NoError(t, err)

	expected := []discovery.Instance{
		{
			Group:      "group1",
			Replicaset: "repl1",
			Name:       "inst1",
			Mode:       discovery.ModeRW,
		},
		{
			Group:      "group2",
			Replicaset: "repl2",
			Name:       "inst2",
			Mode:       discovery.ModeRW,
		},
	}
	assert.ElementsMatch(t, expected, instances)
}

func TestStorage_Discovery_empty_result(t *testing.T) {
	mock := &mockStorage{
		data: []kv.KeyValue{},
	}

	sd := discoverer.NewStorageDiscoverer(mock, "/test-prefix/")

	instances, err := sd.Discovery(context.Background())
	require.NoError(t, err)
	assert.Empty(t, instances)
}

func TestStorage_Discovery_range_error(t *testing.T) {
	mock := &mockStorage{
		err: fmt.Errorf("storage error"),
	}

	sd := discoverer.NewStorageDiscoverer(mock, "/test-prefix/")

	instances, err := sd.Discovery(context.Background())
	require.Error(t, err)
	assert.Nil(t, instances)
	assert.ErrorContains(t, err, "storage error")
}

func TestStorage_Discovery_invalid_data(t *testing.T) {
	mock := &mockStorage{
		data: []kv.KeyValue{
			{
				Key:   []byte("/test-prefix/config/config1"),
				Value: []byte("- foo\n2"),
			},
		},
	}

	sd := discoverer.NewStorageDiscoverer(mock, "/test-prefix/")

	instances, err := sd.Discovery(context.Background())
	require.Error(t, err)
	assert.Nil(t, instances)
	assert.ErrorContains(t, err, "failed to unmarshall")
}

func TestStorage_Discovery_cases(t *testing.T) {
	cases := getDiscoveryCases()
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			var data []kv.KeyValue
			for i, value := range tc.Values {
				data = append(data, kv.KeyValue{
					Key:   []byte(fmt.Sprintf("/test-prefix/config/config%d", i)),
					Value: []byte(value),
				})
			}

			mock := &mockStorage{data: data}
			sd := discoverer.NewStorageDiscoverer(mock, "/test-prefix/")

			instances, err := sd.Discovery(context.Background())
			if tc.Err != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.Err)
			} else {
				require.NoError(t, err)
				assert.ElementsMatch(t, tc.Expected, instances)
			}
		})
	}
}

func TestStorage_Discovery_etcd_integration(t *testing.T) {
	integrationCluster := integration.NewLazyCluster()
	defer integrationCluster.Terminate()

	client, err := clientv3.New(clientv3.Config{
		Endpoints: integrationCluster.EndpointsGRPC(),
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	driver := etcd.New(client)
	st := storage.NewStorage(driver)

	cases := []struct {
		name     string
		configs  map[string]string
		expected []discovery.Instance
	}{
		{
			name: "single",
			configs: map[string]string{
				"config1": `
groups:
  group1:
    replicasets:
      repl1:
        instances:
          inst1: {}
`,
			},
			expected: []discovery.Instance{
				{
					Group:      "group1",
					Replicaset: "repl1",
					Name:       "inst1",
					Mode:       discovery.ModeRW,
				},
			},
		},
		{
			name: "multiple",
			configs: map[string]string{
				"config1": `
groups:
  group1:
    replicasets:
      repl1:
        instances:
          inst1: {}
`,
				"config2": `
groups:
  group2:
    replicasets:
      repl2:
        instances:
          inst2: {}
`,
			},
			expected: []discovery.Instance{
				{
					Group:      "group1",
					Replicaset: "repl1",
					Name:       "inst1",
					Mode:       discovery.ModeRW,
				},
				{
					Group:      "group2",
					Replicaset: "repl2",
					Name:       "inst2",
					Mode:       discovery.ModeRW,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			prefix := fmt.Sprintf("/test-prefix-%s", tc.name)

			for key, yamlData := range tc.configs {
				fullKey := prefix + "/config/" + key
				_, err = client.Put(ctx, fullKey, yamlData)
				require.NoError(t, err)
			}

			sd := discoverer.NewStorageDiscoverer(st, prefix)
			instances, err := sd.Discovery(ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.expected, instances)

			sdEtcd := discoverer.NewEtcd(client, prefix)
			instancesEtcd, err := sdEtcd.Discovery(ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.expected, instancesEtcd)
		})
	}
}
