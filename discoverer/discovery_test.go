package discoverer_test

import "github.com/tarantool/go-discovery"

// discoveryCase is a structure that contains a set of values for single test case.
type discoveryCase struct {
	Name     string
	Values   []string
	Expected []discovery.Instance
	Err      string
}

// getDiscoveryCases returns a set of values for testing the discovery.
func getDiscoveryCases() []discoveryCase {
	return []discoveryCase{

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
			Values: []string{`groups:
  foo:
    replicasets:
      bar:
        instances: {}
`},
			Expected: nil,
		},
		{
			Name: "merge configs",
			Values: []string{`groups:
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
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRW,
				},
				{
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
				{
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
				{
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
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
				},
				{
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
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
				},
				{
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
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
				},
				{
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
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
				},
				{
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
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRW,
				},
				{
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
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeAny,
				},
				{
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
              listen:
              - uri: "foo"
                params:
                  transport: "ssl"
          foo:
            iproto:
              listen:
              - uri: "foo"
                params:
                  transport: "ssl"
              - uri: "car"
`},
			Expected: []discovery.Instance{
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
					URI:        []string{"foo"},
				},
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "car",
					Mode:       discovery.ModeRO,
					URI:        []string{"zoo"},
				},
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "foo",
					Mode:       discovery.ModeRO,
					URI:        []string{"foo", "car"},
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
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
					Roles:      []string{"foo", "bar"},
				},
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "car",
					Mode:       discovery.ModeRO,
					Roles:      []string{"zoo"},
				},
			},
		},
		{
			Name: "labels",
			Values: []string{
				`
groups:
  foo:
    replicasets:
      bar:
        instances:
          zoo:
            labels:
              foo: bar
              bar: zoo
          car:
            labels:
              tags: "a,b,c"
          tar:
            labels:
              float: 2.2
          no: {}
`},
			Expected: []discovery.Instance{
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
					Labels: map[string]string{
						"foo": "bar",
						"bar": "zoo",
					},
				},
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "car",
					Mode:       discovery.ModeRO,
					Labels: map[string]string{
						"tags": "a,b,c",
					},
				},
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "tar",
					Mode:       discovery.ModeRO,
					Labels: map[string]string{
						"float": "2.2",
					},
				},
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "no",
					Mode:       discovery.ModeRO,
					Labels:     nil,
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
              listen:
              - uri: "localhost:3012"
            roles: [crud]
            labels:
              tags: "any,bar,3"
`},
			Expected: []discovery.Instance{
				{
					Group:      "foo",
					Replicaset: "bar",
					Name:       "zoo",
					Mode:       discovery.ModeRO,
					URI: []string{
						"localhost:3011",
					},
					Roles: []string{"crud"},
					Labels: map[string]string{
						"tags": "any,bar,3",
					},
				},
			},
		},
	}
}
