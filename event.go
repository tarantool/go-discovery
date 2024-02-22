package discovery

// Event is a base event for an instance configuration update.
type Event struct {
	// Type of the current event.
	Type EventType
	// Old refers to a previous configuration of the instance. The value
	// present for EventTypeUpdate and EventTypeRemove.
	Old Instance
	// New refers to a new configuration of the instance. This value is
	// present for EventTypeUpdate and EventTypeAdd.
	New Instance
}
