package filter

// isValueInSet returns true if the value presents in the set.
func isValueInSet[K comparable](value K, set []K) bool {
	for _, v := range set {
		if value == v {
			return true
		}
	}
	return false
}

// isAllValuesInSet returns true if all values present in the set.
func isAllValuesInSet[K comparable](values []K, set []K) bool {
	result := false
	for _, value := range values {
		if !isValueInSet(value, set) {
			return false
		}
		result = true
	}
	return result
}
