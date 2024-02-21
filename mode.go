package discovery

// Mode defines an enumeration of a read-write work mode for an instance.
type Mode int

//go:generate stringer -type Mode -trimprefix Mode -linecomment

//nolint:godot
const (
	// ModeAny is used when a target instance could be both.
	ModeAny Mode = iota // any
	// ModeRO defines a read-only instance.
	ModeRO // ro
	// ModeRW defines a read-write instance.
	ModeRW // rw
)
