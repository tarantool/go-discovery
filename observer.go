package discovery

// Observer interface to subscribe for a new events from a configuration update
// source.
type Observer interface {
	// Observe receives either a list of new events or an error. After
	// receiving an error, the observer will no longer receive new events.
	Observe(events []Event, err error)
}
