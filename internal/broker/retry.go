package broker

import (
	"errors"
	"time"
)

// Decision tells the broker what to do after a handler failure.
type Decision struct {
	Retryable bool
	// Reason is a stable, machine-readable failure code. When empty the
	// broker falls back to "handler_error".
	Reason string
}

// NackError is implemented by handler errors that carry retry metadata.
type NackError interface {
	error
	Decision() Decision
}

// Backoff computes the delay before attempt n (n is 1-based for the
// upcoming attempt, so the first retry uses Backoff(1)).
type Backoff func(n int) time.Duration

// RetryPolicy configures retries for one Consumer.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts (first delivery included).
	// Values <= 1 mean no retries.
	MaxAttempts int
	Backoff     Backoff
}

// ConstantBackoff returns a Backoff that always waits d.
func ConstantBackoff(d time.Duration) Backoff {
	return func(int) time.Duration { return d }
}

// ExponentialBackoff returns d*2^(n-1), capped at max (max <= 0 disables
// the cap).
func ExponentialBackoff(d, max time.Duration) Backoff {
	return func(n int) time.Duration {
		if n < 1 {
			n = 1
		}
		w := d
		for i := 1; i < n; i++ {
			w *= 2
			if max > 0 && w >= max {
				return max
			}
		}
		if max > 0 && w > max {
			return max
		}
		return w
	}
}

type nackError struct {
	err       error
	retryable bool
	reason    string
}

func (e *nackError) Error() string {
	if e.err == nil {
		return e.reason
	}
	if e.reason == "" {
		return e.err.Error()
	}
	return e.reason + ": " + e.err.Error()
}

func (e *nackError) Unwrap() error { return e.err }

func (e *nackError) Decision() Decision {
	return Decision{Retryable: e.retryable, Reason: e.reason}
}

// NewNackError wraps err with an explicit decision.
func NewNackError(err error, retryable bool, reason string) NackError {
	return &nackError{err: err, retryable: retryable, reason: reason}
}

// NonRetryable marks err as permanently failing with the given reason.
// Non-retryable failures bypass backoff and go straight to the dead log.
func NonRetryable(err error, reason string) NackError {
	return &nackError{err: err, retryable: false, reason: reason}
}

// classify turns any handler error into a Decision.
func classify(err error) Decision {
	if err == nil {
		return Decision{}
	}
	var ne NackError
	if errors.As(err, &ne) {
		d := ne.Decision()
		if d.Reason == "" {
			d.Reason = "handler_error"
		}
		return d
	}
	return Decision{Retryable: true, Reason: "handler_error"}
}

// Delay returns the wait before the given upcoming attempt number (1 = the
// first redelivery after the initial attempt). A nil Backoff means retry
// immediately.
func (p RetryPolicy) Delay(upcomingAttempt int) time.Duration {
	if p.Backoff == nil {
		return 0
	}
	return p.Backoff(upcomingAttempt)
}
