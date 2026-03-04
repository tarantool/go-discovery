package discoverer

import (
	"context"
	"fmt"
	"time"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/tt/lib/cluster"
	"gopkg.in/yaml.v2"
)

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
			Advertise struct {
				Client string
			}
			Listen []struct {
				URI string
			}
		}
		Leader string
		Roles  []string
		Labels map[string]string
	}

	if err := yaml.Unmarshal([]byte(config.String()), &parsed); err != nil {
		return instance,
			fmt.Errorf("failed to parse configuration: %w", err)
	}

	// Check the mode, see:
	// https://github.com/tarantool/tarantool/blob/0e86fbdeaff3094c86c419c9a4c2d29b55d322b9/src/box/lua/config/instance_config.lua#L1376
	instance.Mode = discovery.ModeAny
	if parsed.Replication.Failover == "" || parsed.Replication.Failover == failoverOff {
		if (parsed.Database.Mode == modeRW) ||
			(len(replicaset.Instances) == 1 && parsed.Database.Mode == "") {
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
	if parsed.Iproto.Advertise.Client != "" {
		uri = append(uri, parsed.Iproto.Advertise.Client)
	} else {
		for _, listen := range parsed.Iproto.Listen {
			if listen.URI != "" {
				uri = append(uri, listen.URI)
			}
		}
	}
	instance.URI = uri

	// Roles already parsed.
	instance.Roles = parsed.Roles

	// Parse tags.
	instance.Labels = parsed.Labels

	return instance, nil
}
