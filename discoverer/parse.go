package discoverer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tarantool/go-config"
	ttconfig "github.com/tarantool/go-config/tarantool"
	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-storage"
	"github.com/tarantool/go-storage/integrity"
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
	configPrefix := normalizeConfigPrefix(prefix)
	typed := integrity.NewTypedBuilder[[]byte](st).
		WithPrefix(configPrefix).
		WithMarshaller(rawBytesMarshaller{}).
		Build()

	builder := ttconfig.New().WithStorage(typed).WithoutSchema()
	cfg, err := builder.Build(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to build tarantool config: %w", err)
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
		parts, ok := parseInstanceKey(key)
		if !ok {
			continue
		}
		replicasetCount[parts.group+"/"+parts.replicaset]++
	}

	const (
		failoverOff    = "off"
		failoverManual = "manual"
		modeRW         = "rw"
	)

	var instances []discovery.Instance
	for key, instCfg := range allConfigs {
		parts, ok := parseInstanceKey(key)
		if !ok {
			continue
		}

		instance := discovery.Instance{
			Group:      parts.group,
			Replicaset: parts.replicaset,
			Name:       parts.instance,
		}

		var failover string
		_, _ = instCfg.Get(config.NewKeyPath("replication/failover"), &failover)

		var mode string
		_, _ = instCfg.Get(config.NewKeyPath("database/mode"), &mode)

		var leader string
		_, _ = instCfg.Get(config.NewKeyPath("leader"), &leader)

		gr := parts.group + "/" + parts.replicaset
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
			instance.URI = getListenURI(instCfg)
		}

		instance.Roles = getStringSlice(instCfg, config.NewKeyPath("roles"))

		var labels map[string]string
		_, _ = instCfg.Get(config.NewKeyPath("labels"), &labels)
		instance.Labels = labels

		instances = append(instances, instance)
	}

	return instances, nil
}

func getStringSlice(cfg config.Config, path config.KeyPath) []string {
	var result []string
	_, _ = cfg.Get(path, &result)
	return result
}

func getListenURI(cfg config.Config) []string {
	var listen any
	_, _ = cfg.Get(config.NewKeyPath("iproto/listen"), &listen)

	switch v := listen.(type) {
	case string:
		return []string{v}
	case []any:
		var uris []string
		for _, item := range v {
			switch it := item.(type) {
			case string:
				uris = append(uris, it)
			case map[string]any:
				if uri, ok := it["uri"].(string); ok {
					uris = append(uris, uri)
				}
			}
		}
		return uris
	}
	return nil
}

// instanceKeyParts holds the parsed components of an instance config key.
// The expected key format from EffectiveAll is:
//
//	groups/<group>/replicasets/<replicaset>/instances/<instance>
type instanceKeyParts struct {
	group      string
	replicaset string
	instance   string
}

// parseInstanceKey extracts group, replicaset, and instance names from a
// configuration key returned by EffectiveAll. It returns false if the key
// does not represent an instance path.
func parseInstanceKey(key string) (instanceKeyParts, bool) {
	parts := strings.Split(key, "/")
	// Expected: ["groups", "<group>", "replicasets", "<replicaset>", "instances", "<instance>"].
	if len(parts) != 6 {
		return instanceKeyParts{}, false
	}
	if parts[0] != "groups" || parts[2] != "replicasets" || parts[4] != "instances" {
		return instanceKeyParts{}, false
	}
	return instanceKeyParts{
		group:      parts[1],
		replicaset: parts[3],
		instance:   parts[5],
	}, true
}

// normalizeConfigPrefix builds a full configuration prefix from a base prefix.
// It ensures the prefix always has a leading slash, then delegates to
// [tarantoolconfig.ConfigPrefix] to append the storage key segment.
func normalizeConfigPrefix(base string) string {
	prefix := strings.TrimRight(base, "/")
	if prefix != "" && !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	return ttconfig.ConfigPrefix(prefix)
}
