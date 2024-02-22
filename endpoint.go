package discovery

// Endpoint describes an instance endpoint configuration.
type Endpoint struct {
	// URI is the endpoint URI.
	URI string
	// Transport is a transport type to connect to the endpoint.
	Transport Transport
}
