package discoverer

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/tarantool/go-config"
	"github.com/tarantool/go-config/collectors"
	"github.com/tarantool/go-config/tree"
	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/integrity"
	"gopkg.in/yaml.v3"
)

// rawBytesMarshaller is a pass-through marshaller for []byte values.
type rawBytesMarshaller struct{}

// Marshal returns data as-is.
func (rawBytesMarshaller) Marshal(data []byte) ([]byte, error) {
	return data, nil
}

// Unmarshal returns data as-is.
func (rawBytesMarshaller) Unmarshal(data []byte) ([]byte, error) {
	return data, nil
}

// checkTimeout calculates the remaining time before the context deadline.
// If the context has no deadline, a default timeout of 3 seconds is used.
func checkTimeout(ctx context.Context) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		// We need to choose some acceptable timeout value. Not too big,
		// but not too small. 3 seconds looks good for me.
		return 3 * time.Second, nil
	}

	if timeout := time.Until(deadline); timeout > 0 {
		return timeout, nil
	}

	return 0, context.DeadlineExceeded
}

// buildInstances retrieves and parses instance configurations from a storage.
func buildInstances(
	ctx context.Context,
	st storage.Storage,
	prefix string,
) ([]discovery.Instance, error) {
	configPrefix := getConfigPrefix(prefix)
	typed := integrity.NewTypedBuilder[[]byte](st).
		WithPrefix(configPrefix).
		WithMarshaller(rawBytesMarshaller{}).
		Build()

	collector := collectors.NewStorage(typed, "config", yamlFormat{})

	builder := config.NewBuilder()
	builder = builder.AddCollector(collector)
	builder = builder.WithInheritance(
		config.Levels(
			config.Global,
			"groups",
			"replicasets",
			"instances",
		),
	)

	cfg, errs := builder.Build(ctx)
	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to build config: %w", errs[0])
	}

	allConfigs, err := cfg.EffectiveAll()
	if err != nil {
		return nil, fmt.Errorf("failed to get effective configs: %w", err)
	}

	if len(allConfigs) == 0 {
		return nil, nil
	}

	// Count instances per replicaset for mode determination.
	replicasetCount := make(map[string]int)
	for key := range allConfigs {
		parts := strings.Split(key, "/")
		if len(parts) >= 6 {
			replicasetCount[parts[1]+"/"+parts[3]]++
		}
	}

	const (
		failoverOff    = "off"
		failoverManual = "manual"
		modeRW         = "rw"
	)

	var instances []discovery.Instance
	for key, instCfg := range allConfigs {
		parts := strings.Split(key, "/")
		if len(parts) < 6 {
			continue
		}

		group := parts[1]
		replicaset := parts[3]
		iname := parts[5]

		instance := discovery.Instance{
			Group:      group,
			Replicaset: replicaset,
			Name:       iname,
		}

		var failover string
		_, _ = instCfg.Get(config.NewKeyPath("replication/failover"), &failover)

		var mode string
		_, _ = instCfg.Get(config.NewKeyPath("database/mode"), &mode)

		var leader string
		_, _ = instCfg.Get(config.NewKeyPath("leader"), &leader)

		gr := group + "/" + replicaset
		switch failover {
		case "", failoverOff:
			if mode == modeRW ||
				(replicasetCount[gr] == 1 && mode == "") {
				instance.Mode = discovery.ModeRW
			} else {
				instance.Mode = discovery.ModeRO
			}
		case failoverManual:
			if instance.Name == leader {
				instance.Mode = discovery.ModeRW
			} else {
				instance.Mode = discovery.ModeRO
			}
		default:
			instance.Mode = discovery.ModeAny
		}

		var advertiseClient string
		if _, err := instCfg.Get(
			config.NewKeyPath("iproto/advertise/client"), &advertiseClient,
		); err == nil && advertiseClient != "" {
			instance.URI = []string{advertiseClient}
		} else {
			var listen []any
			if _, err := instCfg.Get(
				config.NewKeyPath("iproto/listen"), &listen,
			); err == nil {
				for _, item := range listen {
					if m, ok := item.(map[string]any); ok {
						if uri, ok := m["uri"].(string); ok {
							instance.URI = append(instance.URI, uri)
						}
					}
				}
			}
		}

		var roles []string
		_, _ = instCfg.Get(config.NewKeyPath("roles"), &roles)
		instance.Roles = roles

		var labels map[string]string
		_, _ = instCfg.Get(config.NewKeyPath("labels"), &labels)
		instance.Labels = labels

		instances = append(instances, instance)
	}

	return instances, nil
}

// yamlFormat is a custom YAML format that preserves empty mappings as leaf
// nodes. It is used to ensure that empty instance configurations (e.g.
// "inst1: {}") are correctly represented in the configuration tree.
type yamlFormat struct {
	data []byte
}

// Name implements the Format interface.
func (yamlFormat) Name() string { return "yaml" }

// KeepOrder implements the Format interface.
func (yamlFormat) KeepOrder() bool { return false }

// From implements the Format interface.
func (f yamlFormat) From(r io.Reader) collectors.Format {
	d, _ := io.ReadAll(r)
	return yamlFormat{data: d}
}

// Parse implements the Format interface.
func (f yamlFormat) Parse() (*tree.Node, error) {
	var m map[string]any

	if err := yaml.Unmarshal(f.data, &m); err != nil {
		return nil, fmt.Errorf("failed to unmarshal yaml: %w", err)
	}

	root := tree.New()
	flattenMapIntoTree(root, config.NewKeyPath(""), m)

	return root, nil
}

// flattenMapIntoTree recursively inserts map values into a tree.Node. Empty
// maps are inserted as leaf nodes so that structural keys remain visible.
func flattenMapIntoTree(
	node *tree.Node,
	prefix config.KeyPath,
	m map[string]any,
) {
	if len(m) == 0 {
		node.Set(prefix, map[string]any{})
		return
	}

	for k, v := range m {
		path := prefix.Append(k)

		switch val := v.(type) {
		case map[string]any:
			flattenMapIntoTree(node, path, val)
		default:
			node.Set(path, val)
		}
	}
}

// getConfigPrefix returns a full configuration prefix.
func getConfigPrefix(basePrefix string) string {
	prefix := strings.TrimRight(basePrefix, "/")
	if prefix != "" && !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	return prefix + "/config/"
}
