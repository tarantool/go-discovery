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

// isAnyValueInSet returns true if any value from values in the set.
func isAnyValueInSet[K comparable](values []K, set []K) bool {
	for _, value := range values {
		if isValueInSet(value, set) {
			return true
		}
	}
	return false
}

// isAllValuesInSet returns true if all values present in the set.
func isAllValuesInSet[K comparable](values []K, set []K) bool {
	for _, value := range values {
		if !isValueInSet(value, set) {
			return false
		}
	}
	return true
}
