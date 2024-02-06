package discovery

// EventType defines an enumeration of available event types.
type EventType int

//go:generate stringer -type EventType -trimprefix EventType -linecomment

//nolint:godot
const (
	// EventTypeAdded defines an instance configuration add event.
	EventTypeAdd EventType = iota // add
	// EventTypeAdded defines an instance configuration update event.
	EventTypeUpdate // update
	// EventTypeAdded defines an instance configuration remove event.
	EventTypeRemove // remove
)
