package discovery

import "context"

// Subscriber is an interface that allows to subscribe to an instance
// configuration update events.
type Subscriber interface {
	// Subscribe subscribes an observer to new update configuration events. The
	// subscriber should send the first part of update events to the observer
	// before exit from the call to confirm subscription.
	// The context is used to cancel in-progress subscription and does not
	// Unsubscribe the observer on expiration after successful subscription.
	Subscribe(ctx context.Context, observer Observer) error
	// Unsubscribe unsubscribes the observer. A subscriber should not send new
	// events to the observer after the call.
	Unsubscribe(observer Observer)
}
