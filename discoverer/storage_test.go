package discoverer_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/discoverer"
	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/driver/etcd"
	"github.com/tarantool/tt/lib/cluster"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
	"gopkg.in/yaml.v3"
)

func TestStorage_Discovery_Integration(t *testing.T) {
	integrationCluster := integration.NewLazyCluster()
	defer integrationCluster.Terminate()

	client, err := clientv3.New(clientv3.Config{
		Endpoints: integrationCluster.EndpointsGRPC(),
	})
	require.NoError(t, err)
	defer client.Close()

	driver := etcd.New(client)
	st := storage.NewStorage(driver)

	cases := []struct {
		name     string
		configs  map[string]cluster.ClusterConfig
		expected []discovery.Instance
	}{
		{
			name: "single",
			configs: map[string]cluster.ClusterConfig{
				"config1": {
					Groups: map[string]cluster.GroupConfig{
						"group1": {
							Replicasets: map[string]cluster.ReplicasetConfig{
								"repl1": {
									Instances: map[string]cluster.InstanceConfig{
										"inst1": {},
									},
								},
							},
						},
					},
				},
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
			configs: map[string]cluster.ClusterConfig{
				"config1": {
					Groups: map[string]cluster.GroupConfig{
						"group1": {
							Replicasets: map[string]cluster.ReplicasetConfig{
								"repl1": {
									Instances: map[string]cluster.InstanceConfig{
										"inst1": {},
									},
								},
							},
						},
					},
				},
				"config2": {
					Groups: map[string]cluster.GroupConfig{
						"group2": {
							Replicasets: map[string]cluster.ReplicasetConfig{
								"repl2": {
									Instances: map[string]cluster.InstanceConfig{
										"inst2": {},
									},
								},
							},
						},
					},
				},
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
			prefix := fmt.Sprintf("/test-prefix-%s/", tc.name)

			// Write configs to etcd.
			sd := discoverer.NewStorageDiscoverer(st, prefix)
			for key, cc := range tc.configs {
				b, err := yaml.Marshal(cc)
				require.NoError(t, err)

				fullKey := getConfigPrefix(prefix) + key
				_, err = client.Put(ctx, fullKey, string(b))
				require.NoError(t, err)
			}

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

// getConfigPrefix returns a full configuration prefix.
func getConfigPrefix(basePrefix string) string {
	prefix := strings.TrimRight(basePrefix, "/")
	return fmt.Sprintf("%s/%s/", prefix, "config")
}
