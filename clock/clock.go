// Package clock abstracts time so that protocol code can run against the
// wall clock in production and against a virtual, fully deterministic clock
// inside the simulator.
//
// Protocol code must never call time.Now, time.Sleep or time.AfterFunc
// directly; it asks a Clock instead. The contract every implementation must
// honour is that AfterFunc never runs f synchronously inside the call, so a
// caller may hold a lock while scheduling a timer.
package clock

import "time"

// Timer is a handle to a scheduled callback.
type Timer interface {
	// Stop prevents the callback from running. It reports whether the call
	// stopped the timer (false if it already fired or was stopped).
	Stop() bool
}

// Clock is the source of time for protocol code.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// AfterFunc arranges for f to run once, d after the call. f always runs
	// asynchronously with respect to the caller.
	AfterFunc(d time.Duration, f func()) Timer
}

// Real is a Clock backed by the operating system clock.
type Real struct{}

// NewReal returns the wall clock.
func NewReal() Real { return Real{} }

// Now implements Clock.
func (Real) Now() time.Time { return time.Now() }

// AfterFunc implements Clock using time.AfterFunc, which runs f on its own
// goroutine.
func (Real) AfterFunc(d time.Duration, f func()) Timer {
	return time.AfterFunc(d, f)
}
