package discovery

// Mode defines an enumeration of a read-write work mode for an instance.
type Mode int

//go:generate stringer -type Mode -trimprefix Mode -linecomment

//nolint:godot
const (
	// ModeAll is used when a target instance could be both.
	ModeAll Mode = iota // all
	// ModeRO defines a read-only instance.
	ModeRO // ro
	// ModeRW defines a read-write instance.
	ModeRW // rw
)
