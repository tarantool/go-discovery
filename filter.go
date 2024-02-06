package discovery

// Filter is an interface to filter an instance by some conditions. A set
// of filters implemented in the `filter` subpackage.
type Filter interface {
	// Filter return true if an instance matches a condition.
	Filter(instance Instance) bool
}

// FilterFunc allows to adopt a function to a filter, as example:
//
//	var filter Filter = FilterFunc(func(intstance Instance) bool {
//	    return true
//	})
type FilterFunc func(instance Instance) bool

// Filter calls a function itself to implement the Filter interface.
func (f FilterFunc) Filter(instance Instance) bool {
	return f(instance)
}

var _ Filter = FilterFunc(func(_ Instance) bool {
	return true
})
