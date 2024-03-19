package subscriber

import "fmt"

var (
	// ErrMissingSubscriber is an error that tells that the provided inner
	// subscriber is nil.
	ErrMissingSubscriber = fmt.Errorf("inner subscriber is missing")
	// ErrSubscriptionExists is an error that tells that the subscription for
	// this Schedule subscriber already exists.
	// Only one subscribed observer is allowed for a Schedule subscriber.
	// To reuse a Schedule, method Unsubscribe() should be called for an
	// unused observer.
	ErrSubscriptionExists = fmt.Errorf("subscription already exists")
)
