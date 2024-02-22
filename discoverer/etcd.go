package discoverer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tarantool/tt/lib/cluster"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"

	"github.com/tarantool/go-discovery"
)

// Etcd discovers a list of instance configurations from etcd.
type Etcd struct {
	// etcd obtains information from etcd.
	etcd clientv3.KV
	// The configuration prefix for etcd.
	prefix string
}

// ErrMissingEtcd is an error that tells that the provided etcd object is nil.
var ErrMissingEtcd = fmt.Errorf("etcd object is missing")

// NewEtcd creates a new etcd discoverer to retrieve a list of instance
// configurations from etcd.
//
// The prefix must have the same value as config.etcd.prefix.
func NewEtcd(etcd clientv3.KV, prefix string) *Etcd {
	return &Etcd{
		etcd:   etcd,
		prefix: prefix,
	}
}

// Discovery retrieves a list of instance configurations from etcd.
func (d *Etcd) Discovery(ctx context.Context) ([]discovery.Instance, error) {
	if d.etcd == nil {
		return nil, ErrMissingEtcd
	}

	const (
		fmtMissed = "a configuration data not found in etcd for prefix \"%s/config/\""
	)
	for {
		var timeout time.Duration
		deadline, ok := ctx.Deadline()
		if ok {
			now := time.Now()
			if now.Before(deadline) {
				timeout = deadline.Sub(now)
			}
			// Else the deadline is expired and we will return an error on
			// a next <-ctx.Done() check.
		} else {
			// We need to choose some acceptable timeout value. Not too big,
			// but not too small. 3 seconds looks good for me.
			timeout = 3 * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		dataCollector := cluster.NewEtcdAllCollector(d.etcd, d.prefix, timeout)
		collector := cluster.NewYamlCollectorDecorator(dataCollector)
		config, err := collector.Collect()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// Too small timeout? We could retry until the main ctx is not
				// expired.
				continue
			}

			strMissed := fmt.Sprintf(fmtMissed, strings.TrimRight(d.prefix, "/"))
			if err.Error() == strMissed {
				return nil, nil
			}

			return nil, fmt.Errorf("failed to get data from etcd: %w", err)
		}

		return parseConfig(config)
	}
}

// parseConfig parses a cluster configuration and returns instances
// configuration.
func parseConfig(config *cluster.Config) ([]discovery.Instance, error) {
	cconfig, err := cluster.MakeClusterConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse configuration: %w", err)
	}

	var instances []discovery.Instance
	for gname, group := range cconfig.Groups {
		for rname, replicaset := range group.Replicasets {
			for iname := range replicaset.Instances {
				iconfig := cluster.Instantiate(cconfig, iname)

				instance, err := parseInstanceConfig(iconfig, replicaset,
					discovery.Instance{
						Group:      gname,
						Replicaset: rname,
						Name:       iname,
					})
				if err != nil {
					return nil,
						fmt.Errorf("failed to parse an instance configuration: %w", err)
				}

				instances = append(instances, instance)
			}
		}
	}

	return instances, nil
}

// parseInstanceConfig parses an instance configuration fields.
func parseInstanceConfig(config *cluster.Config,
	replicaset cluster.ReplicasetConfig,
	instance discovery.Instance) (discovery.Instance, error) {
	const (
		failoverOff    = "off"
		failoverManual = "manual"
		modeRW         = "rw"
		modeRO         = "ro"
	)
	var parsed struct {
		Replication struct {
			Failover string
		}
		Database struct {
			Mode string
		}
		Iproto struct {
			Listen []struct {
				URI    string
				Params struct {
					Transport string
				}
			}
		}
		Leader string
		Roles  []string
	}

	if err := yaml.Unmarshal([]byte(config.String()), &parsed); err != nil {
		return instance,
			fmt.Errorf("failed to parse configuration: %w", err)
	}

	// Check the mode, see:
	// https://github.com/tarantool/tarantool/blob/0e86fbdeaff3094c86c419c9a4c2d29b55d322b9/src/box/lua/config/instance_config.lua#L1376
	instance.Mode = discovery.ModeAny
	if parsed.Replication.Failover == "" || parsed.Replication.Failover == failoverOff {
		if parsed.Database.Mode == modeRW {
			instance.Mode = discovery.ModeRW
		} else if len(replicaset.Instances) == 1 && parsed.Database.Mode == "" {
			instance.Mode = discovery.ModeRW
		} else {
			instance.Mode = discovery.ModeRO
		}
	} else if parsed.Replication.Failover == failoverManual {
		if instance.Name == parsed.Leader {
			instance.Mode = discovery.ModeRW
		} else {
			instance.Mode = discovery.ModeRO
		}
	}
	// Else Mode = ModeAny.

	// Collect URI.
	var uri []string
	for _, listen := range parsed.Iproto.Listen {
		uri = append(uri, listen.URI)
	}
	instance.URI = uri

	// Roles already parsed.
	instance.Roles = parsed.Roles

	// Parse tags.
	instance.RolesTags = parseRolesTags(config)
	instance.AppTags = parseAppTags(config)

	return instance, nil
}

// parseRolesTags tries to parse roles_cfg.tags as a set of strings.
func parseRolesTags(config *cluster.Config) []string {
	var parsed struct {
		RolesCfg struct {
			Tags []string
		} `yaml:"roles_cfg"`
	}

	if err := yaml.Unmarshal([]byte(config.String()), &parsed); err != nil {
		// It is ok to fail here because roles_cfg.tags is not defined in
		// a schema actually and a user may add any value for it.
		return nil
	}

	return parsed.RolesCfg.Tags
}

// parseRolesTags tries to parse app.cfg.tags as a set of strings.
func parseAppTags(config *cluster.Config) []string {
	var parsed struct {
		App struct {
			Cfg struct {
				Tags []string
			}
		}
	}

	if err := yaml.Unmarshal([]byte(config.String()), &parsed); err != nil {
		// It is ok to fail here because app.cfg.tags is not defined in
		// a schema actually and a user may add any value for it.
		return nil
	}

	return parsed.App.Cfg.Tags
}
