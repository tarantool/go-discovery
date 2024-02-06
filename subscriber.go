package discovery

// Subscriber is an interface that allows to subscribe to an instance
// configuration update events.
type Subscriber interface {
	// Subscribe subscribes an observer to new update configuration events.
	Subscribe(observer Observer) error
	// Unsubscribe unsubscribes the observer. A subscriber should not send new
	// events to the observer after the call.
	Unsubscribe(observer Observer)
}
