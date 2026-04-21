package discoverer

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

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

// Pre-built key paths read for each instance.
var (
	pathFailover  = config.NewKeyPath("replication/failover")
	pathMode      = config.NewKeyPath("database/mode")
	pathLeader    = config.NewKeyPath("leader")
	pathAdvertise = config.NewKeyPath("iproto/advertise/client")
	pathListen    = config.NewKeyPath("iproto/listen")
	pathRoles     = config.NewKeyPath("roles")
	pathLabels    = config.NewKeyPath("labels")
	pathRolesCfg  = config.NewKeyPath("roles_cfg")
)

// buildInstances retrieves and parses instance configurations from a storage.
func buildInstances(
	ctx context.Context,
	st storage.Storage,
	prefix string,
) ([]discovery.Instance, error) {
	typed := integrity.NewTypedBuilder[[]byte](st).
		WithPrefix(normalizeConfigPrefix(prefix)).
		WithMarshaller(rawBytesMarshaller{}).
		Build()

	cfg, err := ttconfig.New().WithStorage(typed).WithoutSchema().Build(ctx)
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
		if parts, ok := parseInstanceKey(key); ok {
			replicasetCount[parts.group+"/"+parts.replicaset]++
		}
	}

	var instances []discovery.Instance
	for key, instCfg := range allConfigs {
		parts, ok := parseInstanceKey(key)
		if !ok {
			continue
		}

		// get reads an optional config value at path; missing keys are not errors.
		get := func(path config.KeyPath, dest any, what string) error {
			if err := getConfigValue(instCfg, path, dest); err != nil {
				return fmt.Errorf("failed to get %s for instance %s: %w",
					what, parts.instance, err)
			}
			return nil
		}

		var failover, mode, leader, advertiseClient string
		var roles []string
		var labels map[string]string
		var rolesCfg map[string]any
		if err := get(pathFailover, &failover, "failover"); err != nil {
			return nil, err
		}
		if err := get(pathMode, &mode, "mode"); err != nil {
			return nil, err
		}
		if err := get(pathLeader, &leader, "leader"); err != nil {
			return nil, err
		}
		if err := get(pathAdvertise, &advertiseClient, "advertise client"); err != nil {
			return nil, err
		}
		if err := get(pathRoles, &roles, "roles"); err != nil {
			return nil, err
		}
		if err := get(pathLabels, &labels, "labels"); err != nil {
			return nil, err
		}
		if err := get(pathRolesCfg, &rolesCfg, "roles_cfg"); err != nil {
			return nil, err
		}

		instance := discovery.Instance{
			Group:      parts.group,
			Replicaset: parts.replicaset,
			Name:       parts.instance,
			Mode: resolveMode(failover, mode, leader, parts.instance,
				replicasetCount[parts.group+"/"+parts.replicaset]),
			Roles:    roles,
			Labels:   labels,
			RolesCfg: rolesCfg,
		}

		if advertiseClient != "" {
			instance.URI = []string{advertiseClient}
		} else {
			uris, err := getListenURI(instCfg)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to get listen URI for instance %s: %w", parts.instance, err)
			}
			instance.URI = uris
		}

		instances = append(instances, instance)
	}

	return instances, nil
}

// resolveMode picks an instance Mode from its failover/mode/leader settings.
// instancesInReplicaset is the total number of instances in the same replicaset,
// used to default a single-instance replicaset to RW.
func resolveMode(failover, mode, leader, name string, instancesInReplicaset int) discovery.Mode {
	const (
		failoverOff    = "off"
		failoverManual = "manual"
		modeRW         = "rw"
	)

	switch failover {
	case "", failoverOff:
		if mode == modeRW || (instancesInReplicaset == 1 && mode == "") {
			return discovery.ModeRW
		}
		return discovery.ModeRO
	case failoverManual:
		if name == leader {
			return discovery.ModeRW
		}
		return discovery.ModeRO
	default:
		return discovery.ModeAny
	}
}

func getListenURI(cfg config.Config) ([]string, error) {
	var listen any
	if err := getConfigValue(cfg, pathListen, &listen); err != nil {
		return nil, err
	}

	switch v := listen.(type) {
	case string:
		return []string{v}, nil
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
		return uris, nil
	}
	return nil, nil
}

func getConfigValue(cfg config.Config, path config.KeyPath, dest any) error {
	_, err := cfg.Get(path, dest)
	if err != nil && !errors.Is(err, config.ErrKeyNotFound) {
		return err
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
	base = strings.TrimRight(base, "/")
	if base == "" {
		return ttconfig.ConfigPrefix("")
	}
	return ttconfig.ConfigPrefix(path.Join("/", base))
}
