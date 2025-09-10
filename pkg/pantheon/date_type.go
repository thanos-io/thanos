// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pantheon

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Date represents a date in YYYY-MM-DD format.
type Date struct {
	time.Time
}

// MarshalJSON implements json.Marshaler for Date.
func (d Date) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Format("2006-01-02"))
}

// UnmarshalJSON implements json.Unmarshaler for Date.
func (d *Date) UnmarshalJSON(data []byte) error {
	var dateStr string
	if err := json.Unmarshal(data, &dateStr); err != nil {
		return err
	}

	t, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return fmt.Errorf("invalid date format, expected YYYY-MM-DD: %v", err)
	}

	d.Time = t
	return nil
}

// MarshalYAML implements yaml.Marshaler for Date.
func (d Date) MarshalYAML() (interface{}, error) {
	return d.Format("2006-01-02"), nil
}

// UnmarshalYAML implements yaml.Unmarshaler for Date.
func (d *Date) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var dateStr string
	if err := unmarshal(&dateStr); err != nil {
		return err
	}

	t, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return fmt.Errorf("invalid date format, expected YYYY-MM-DD: %v", err)
	}

	d.Time = t
	return nil
}

// String returns the date in YYYY-MM-DD format.
func (d Date) String() string {
	return d.Format("2006-01-02")
}

// NewDate creates a new Date from year, month, and day.
func NewDate(year int, month time.Month, day int) Date {
	return Date{time.Date(year, month, day, 0, 0, 0, 0, time.UTC)}
}

// ParseDate parses a date string in YYYY-MM-DD format.
func ParseDate(dateStr string) (Date, error) {
	t, err := time.Parse("2006-01-02", strings.TrimSpace(dateStr))
	if err != nil {
		return Date{}, fmt.Errorf("invalid date format, expected YYYY-MM-DD: %v", err)
	}
	return Date{t}, nil
}

// Before reports whether the date d is before other.
func (d Date) Before(other Date) bool {
	return d.Time.Before(other.Time)
}

// After reports whether the date d is after other.
func (d Date) After(other Date) bool {
	return d.Time.After(other.Time)
}

// Equal reports whether d and other represent the same date.
func (d Date) Equal(other Date) bool {
	return d.Time.Equal(other.Time)
}

// Compare compares the date d with other. If d is before other, it returns -1;
// if d is after other, it returns 1; if they're equal, it returns 0.
func (d Date) Compare(other Date) int {
	if d.Before(other) {
		return -1
	}
	if d.After(other) {
		return 1
	}
	return 0
}

// IsZero reports whether d represents the zero date (January 1, year 1, 00:00:00 UTC).
func (d Date) IsZero() bool {
	return d.Time.IsZero()
}
