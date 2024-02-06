package filter

import (
	"github.com/tarantool/go-discovery"
)

// AppTagsContains matches instances that have all application tags from the
// set.
type AppTagsContains struct {
	// AppTags is a set of application tags to pass the match.
	AppTags []string
}

// Filter returns true if the instance has all application tags from the set.
func (f AppTagsContains) Filter(instance discovery.Instance) bool {
	return isAllValuesInSet(f.AppTags, instance.AppTags)
}
