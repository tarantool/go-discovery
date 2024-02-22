package discovery

// Transport defines an enumeration of a connection transport type for an
// instance.
type Transport int

//go:generate stringer -type Transport -trimprefix Transport -linecomment

//nolint:godot
const (
	// TransportPlain is a base connection without encryption.
	TransportPlain Transport = iota // plain
	// TransportSSL uses TLS encryption.
	TransportSSL // ssl
)
