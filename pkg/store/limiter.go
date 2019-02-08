package store

import "github.com/pkg/errors"

// Limiter is a simple mechanism for checking if something has passed a certain threshold.
type Limiter struct {
	limit uint64
}

// NewLimiter returns a new limiter with a specified limit. 0 disables the limit.
func NewLimiter(limit uint64) *Limiter {
	return &Limiter{limit: limit}
}

// Check checks if the passed number exceeds the limits or not.
func (l *Limiter) Check(num uint64) error {
	if l.limit == 0 {
		return nil
	}
	if num > l.limit {
		return errors.Errorf("limit %v violated (got %v)", l.limit, num)
	}
	return nil
}
