package filter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/filter"
)

var _ discovery.Filter = filter.GroupOneOf{}
var _ discovery.Filter = filter.ReplicasetOneOf{}
var _ discovery.Filter = filter.NameOneOf{}
var _ discovery.Filter = filter.ModeOneOf{}
var _ discovery.Filter = filter.URIAnyOf{}
var _ discovery.Filter = filter.TransportAnyOf{}
var _ discovery.Filter = filter.RolesContains{}
var _ discovery.Filter = filter.RolesTagsContains{}
var _ discovery.Filter = filter.AppTagsContains{}

func TestFilters(t *testing.T) {
	cases := []struct {
		Name     string
		Instance discovery.Instance
		Filter   discovery.Filter
		Expected bool
	}{
		{
			Name:     "group_one_of_empty_isntance_and_filter",
			Instance: discovery.Instance{},
			Filter:   filter.GroupOneOf{},
			Expected: false,
		},
		{
			Name:     "group_one_of_empty_filter",
			Instance: discovery.Instance{Group: "foo"},
			Filter:   filter.GroupOneOf{},
			Expected: false,
		},
		{
			Name:     "group_one_of_no_match",
			Instance: discovery.Instance{Group: "zoo"},
			Filter:   filter.GroupOneOf{[]string{"foo", "bar"}},
			Expected: false,
		},
		{
			Name:     "group_one_of_match",
			Instance: discovery.Instance{Group: "foo"},
			Filter:   filter.GroupOneOf{[]string{"foo", "bar"}},
			Expected: true,
		},
		{
			Name:     "replicaset_one_of_empty_isntance_and_filter",
			Instance: discovery.Instance{},
			Filter:   filter.ReplicasetOneOf{},
			Expected: false,
		},
		{
			Name:     "replicaset_one_of_empty_filter",
			Instance: discovery.Instance{Replicaset: "foo"},
			Filter:   filter.ReplicasetOneOf{},
			Expected: false,
		},
		{
			Name:     "replicaset_one_of_no_match",
			Instance: discovery.Instance{Replicaset: "zoo"},
			Filter:   filter.ReplicasetOneOf{[]string{"foo", "bar"}},
			Expected: false,
		},
		{
			Name:     "replicaset_one_of_match",
			Instance: discovery.Instance{Replicaset: "foo"},
			Filter:   filter.ReplicasetOneOf{[]string{"foo", "bar"}},
			Expected: true,
		},
		{
			Name:     "name_one_of_empty_isntance_and_filter",
			Instance: discovery.Instance{},
			Filter:   filter.NameOneOf{},
			Expected: false,
		},
		{
			Name:     "name_one_of_empty_filter",
			Instance: discovery.Instance{Name: "foo"},
			Filter:   filter.NameOneOf{},
			Expected: false,
		},
		{
			Name:     "name_one_of_no_match",
			Instance: discovery.Instance{Name: "zoo"},
			Filter:   filter.NameOneOf{[]string{"foo", "bar"}},
			Expected: false,
		},
		{
			Name:     "name_one_of_match",
			Instance: discovery.Instance{Name: "foo"},
			Filter:   filter.NameOneOf{[]string{"foo", "bar"}},
			Expected: true,
		},
		{
			Name:     "mode_one_of_empty_isntance_and_filter",
			Instance: discovery.Instance{},
			Filter:   filter.ModeOneOf{},
			Expected: false,
		},
		{
			Name:     "mode_one_of_empty_filter",
			Instance: discovery.Instance{Mode: discovery.ModeRW},
			Filter:   filter.ModeOneOf{},
			Expected: false,
		},
		{
			Name:     "mode_one_of_no_match",
			Instance: discovery.Instance{Mode: discovery.ModeRW},
			Filter: filter.ModeOneOf{
				[]discovery.Mode{
					discovery.ModeAny,
					discovery.ModeRO,
				}},
			Expected: false,
		},
		{
			Name:     "mode_one_of_match",
			Instance: discovery.Instance{Mode: discovery.ModeAny},
			Filter: filter.ModeOneOf{
				[]discovery.Mode{
					discovery.ModeAny,
					discovery.ModeRO,
				}},
			Expected: true,
		},
		{
			Name:     "uri_one_of_empty_isntance_and_filter",
			Instance: discovery.Instance{},
			Filter:   filter.URIAnyOf{},
			Expected: false,
		},
		{
			Name: "uri_one_of_empty_filter",
			Instance: discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						URI: "foo",
					},
				},
			},
			Filter:   filter.URIAnyOf{},
			Expected: false,
		},
		{
			Name: "uri_one_of_no_match",
			Instance: discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						URI: "zoo",
					},
					discovery.Endpoint{
						URI: "car",
					},
				},
			},
			Filter:   filter.URIAnyOf{[]string{"foo", "bar"}},
			Expected: false,
		},
		{
			Name: "uri_one_of_match",
			Instance: discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						URI: "foo",
					},
				},
			},
			Filter:   filter.URIAnyOf{[]string{"foo", "bar"}},
			Expected: true,
		},
		{
			Name: "uri_one_of_match_any",
			Instance: discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						URI: "foo",
					},
					discovery.Endpoint{
						URI: "car",
					},
				},
			},
			Filter:   filter.URIAnyOf{[]string{"zoo", "car"}},
			Expected: true,
		},
		{
			Name:     "transport_one_of_empty_isntance_and_filter",
			Instance: discovery.Instance{},
			Filter:   filter.TransportAnyOf{},
			Expected: false,
		},
		{
			Name: "transport_one_of_empty_filter",
			Instance: discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						Transport: discovery.TransportPlain,
					},
				},
			},
			Filter:   filter.TransportAnyOf{},
			Expected: false,
		},
		{
			Name: "transport_one_of_no_match",
			Instance: discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						Transport: discovery.TransportPlain,
					},
					discovery.Endpoint{
						Transport: discovery.TransportSSL,
					},
				},
			},
			Filter: filter.TransportAnyOf{[]discovery.Transport{
				discovery.Transport(665),
				discovery.Transport(667),
			}},
			Expected: false,
		},
		{
			Name: "transport_one_of_match",
			Instance: discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						Transport: discovery.TransportPlain,
					},
				},
			},
			Filter: filter.TransportAnyOf{[]discovery.Transport{
				discovery.Transport(665),
				discovery.TransportPlain,
			}},
			Expected: true,
		},
		{
			Name: "transport_one_of_match_any",
			Instance: discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						Transport: discovery.TransportPlain,
					},
					discovery.Endpoint{
						Transport: discovery.TransportSSL,
					},
				},
			},
			Filter: filter.TransportAnyOf{[]discovery.Transport{
				discovery.Transport(665),
				discovery.TransportSSL,
			}},
			Expected: true,
		},
		{
			Name:     "roles_contains_empty_isntance_and_filter",
			Instance: discovery.Instance{},
			Filter:   filter.RolesContains{},
			Expected: false,
		},
		{
			Name:     "roles_contains_empty_filter",
			Instance: discovery.Instance{Roles: []string{"foo"}},
			Filter:   filter.RolesContains{},
			Expected: false,
		},
		{
			Name:     "roles_contains_no_match",
			Instance: discovery.Instance{Roles: []string{"zoo", "car"}},
			Filter:   filter.RolesContains{[]string{"foo", "bar"}},
			Expected: false,
		},
		{
			Name:     "roles_contains_not_full_match",
			Instance: discovery.Instance{Roles: []string{"foo"}},
			Filter:   filter.RolesContains{[]string{"foo", "bar"}},
			Expected: false,
		},
		{
			Name:     "roles_contains_full_match",
			Instance: discovery.Instance{Roles: []string{"foo", "car"}},
			Filter:   filter.RolesContains{[]string{"foo", "car"}},
			Expected: true,
		},
		{
			Name:     "roles_contains_extra_match",
			Instance: discovery.Instance{Roles: []string{"foo", "car", "zoo"}},
			Filter:   filter.RolesContains{[]string{"foo", "car"}},
			Expected: true,
		},
		{
			Name:     "roles_tags_contains_empty_isntance_and_filter",
			Instance: discovery.Instance{},
			Filter:   filter.RolesTagsContains{},
			Expected: false,
		},
		{
			Name:     "roles_tags_contains_empty_filter",
			Instance: discovery.Instance{RolesTags: []string{"foo"}},
			Filter:   filter.RolesTagsContains{},
			Expected: false,
		},
		{
			Name:     "roles_tags_contains_no_match",
			Instance: discovery.Instance{RolesTags: []string{"zoo", "car"}},
			Filter:   filter.RolesTagsContains{[]string{"foo", "bar"}},
			Expected: false,
		},
		{
			Name:     "roles_tags_contains_not_full_match",
			Instance: discovery.Instance{RolesTags: []string{"foo"}},
			Filter:   filter.RolesTagsContains{[]string{"foo", "bar"}},
			Expected: false,
		},
		{
			Name:     "roles_tags_contains_full_match",
			Instance: discovery.Instance{RolesTags: []string{"foo", "car"}},
			Filter:   filter.RolesTagsContains{[]string{"foo", "car"}},
			Expected: true,
		},
		{
			Name:     "roles_tags_contains_extra_match",
			Instance: discovery.Instance{RolesTags: []string{"foo", "car", "zoo"}},
			Filter:   filter.RolesTagsContains{[]string{"foo", "car"}},
			Expected: true,
		},
		{
			Name:     "app_tags_contains_empty_isntance_and_filter",
			Instance: discovery.Instance{},
			Filter:   filter.AppTagsContains{},
			Expected: false,
		},
		{
			Name:     "app_tags_contains_empty_filter",
			Instance: discovery.Instance{AppTags: []string{"foo"}},
			Filter:   filter.AppTagsContains{},
			Expected: false,
		},
		{
			Name:     "app_tags_contains_no_match",
			Instance: discovery.Instance{AppTags: []string{"zoo", "car"}},
			Filter:   filter.AppTagsContains{[]string{"foo", "bar"}},
			Expected: false,
		},
		{
			Name:     "app_tags_contains_not_full_match",
			Instance: discovery.Instance{AppTags: []string{"foo"}},
			Filter:   filter.AppTagsContains{[]string{"foo", "bar"}},
			Expected: false,
		},
		{
			Name:     "app_tags_contains_full_match",
			Instance: discovery.Instance{AppTags: []string{"foo", "car"}},
			Filter:   filter.AppTagsContains{[]string{"foo", "car"}},
			Expected: true,
		},
		{
			Name:     "app_tags_contains_extra_match",
			Instance: discovery.Instance{AppTags: []string{"foo", "car", "zoo"}},
			Filter:   filter.AppTagsContains{[]string{"foo", "car"}},
			Expected: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			result := tc.Filter.Filter(tc.Instance)
			require.Equal(t, tc.Expected, result)
		})
	}
}
