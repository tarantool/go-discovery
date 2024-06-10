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
var _ discovery.Filter = filter.RolesContain{}
var _ discovery.Filter = filter.LabelsContain{}

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
			Name:     "uri_one_of_empty_filter",
			Instance: discovery.Instance{URI: []string{"foo"}},
			Filter:   filter.URIAnyOf{},
			Expected: false,
		},
		{
			Name:     "uri_one_of_no_match",
			Instance: discovery.Instance{URI: []string{"zoo", "car"}},
			Filter:   filter.URIAnyOf{[]string{"foo", "bar"}},
			Expected: false,
		},
		{
			Name:     "uri_one_of_match",
			Instance: discovery.Instance{URI: []string{"foo"}},
			Filter:   filter.URIAnyOf{[]string{"foo", "bar"}},
			Expected: true,
		},
		{
			Name:     "uri_one_of_match_any",
			Instance: discovery.Instance{URI: []string{"foo", "car"}},
			Filter:   filter.URIAnyOf{[]string{"zoo", "car"}},
			Expected: true,
		},
		{
			Name:     "roles_contain_empty_isntance_and_filter",
			Instance: discovery.Instance{},
			Filter:   filter.RolesContain{},
			Expected: true,
		},
		{
			Name:     "roles_contain_empty_filter",
			Instance: discovery.Instance{Roles: []string{"foo"}},
			Filter:   filter.RolesContain{},
			Expected: true,
		},
		{
			Name:     "roles_contain_no_match",
			Instance: discovery.Instance{Roles: []string{"zoo", "car"}},
			Filter:   filter.RolesContain{[]string{"foo", "bar"}},
			Expected: false,
		},
		{
			Name:     "roles_contain_not_full_match",
			Instance: discovery.Instance{Roles: []string{"foo"}},
			Filter:   filter.RolesContain{[]string{"foo", "bar"}},
			Expected: false,
		},
		{
			Name:     "roles_contain_full_match",
			Instance: discovery.Instance{Roles: []string{"foo", "car"}},
			Filter:   filter.RolesContain{[]string{"foo", "car"}},
			Expected: true,
		},
		{
			Name:     "roles_contain_extra_match",
			Instance: discovery.Instance{Roles: []string{"foo", "car", "zoo"}},
			Filter:   filter.RolesContain{[]string{"foo", "car"}},
			Expected: true,
		},
		{
			Name:     "labels_contain_empty_isntance_and_filter",
			Instance: discovery.Instance{},
			Filter:   filter.LabelsContain{},
			Expected: true,
		},
		{
			Name: "labels_contain_empty_filter",
			Instance: discovery.Instance{Labels: map[string]string{
				"foo": "bar",
			}},
			Filter:   filter.LabelsContain{},
			Expected: true,
		},
		{
			Name: "labels_contain_no_match",
			Instance: discovery.Instance{Labels: map[string]string{
				"zoo": "car",
			}},
			Filter: filter.LabelsContain{map[string]string{
				"foo": "bar",
			}},
			Expected: false,
		},
		{
			Name: "labels_contain_no_value_match",
			Instance: discovery.Instance{Labels: map[string]string{
				"zoo": "car",
			}},
			Filter: filter.LabelsContain{map[string]string{
				"zoo": "bar",
			}},
			Expected: false,
		},
		{
			Name: "labels_contain_not_full_match",
			Instance: discovery.Instance{Labels: map[string]string{
				"zoo": "car",
			}},
			Filter: filter.LabelsContain{map[string]string{
				"bar": "foo",
				"zoo": "car",
			}},
			Expected: false,
		},
		{
			Name: "labels_contain_full_match",
			Instance: discovery.Instance{Labels: map[string]string{
				"bar": "foo",
				"zoo": "car",
			}},
			Filter: filter.LabelsContain{map[string]string{
				"bar": "foo",
				"zoo": "car",
			}},
			Expected: true,
		},
		{
			Name: "labels_contain_extra_match",
			Instance: discovery.Instance{Labels: map[string]string{
				"bar": "foo",
				"zoo": "car",
				"car": "zoo",
			}},
			Filter: filter.LabelsContain{map[string]string{
				"bar": "foo",
				"zoo": "car",
			}},
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
