// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pantheon

import (
	"encoding/json"
	"testing"
	"time"

	"gopkg.in/yaml.v2"
)

func TestNewDate(t *testing.T) {
	tests := []struct {
		name     string
		year     int
		month    time.Month
		day      int
		expected string
	}{
		{
			name:     "valid date",
			year:     2025,
			month:    time.July,
			day:      4,
			expected: "2025-07-04",
		},
		{
			name:     "leap year date",
			year:     2024,
			month:    time.February,
			day:      29,
			expected: "2024-02-29",
		},
		{
			name:     "new year date",
			year:     2023,
			month:    time.January,
			day:      1,
			expected: "2023-01-01",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			date := NewDate(tt.year, tt.month, tt.day)
			if date.String() != tt.expected {
				t.Errorf("NewDate() = %v, want %v", date.String(), tt.expected)
			}
		})
	}
}

func TestParseDate(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  string
		shouldErr bool
	}{
		{
			name:      "valid date",
			input:     "2025-07-04",
			expected:  "2025-07-04",
			shouldErr: false,
		},
		{
			name:      "valid date with whitespace",
			input:     "  2025-07-04  ",
			expected:  "2025-07-04",
			shouldErr: false,
		},
		{
			name:      "leap year date",
			input:     "2024-02-29",
			expected:  "2024-02-29",
			shouldErr: false,
		},
		{
			name:      "invalid format - wrong separator",
			input:     "2025/07/04",
			shouldErr: true,
		},
		{
			name:      "invalid format - missing day",
			input:     "2025-07",
			shouldErr: true,
		},
		{
			name:      "invalid date - February 30",
			input:     "2025-02-30",
			shouldErr: true,
		},
		{
			name:      "invalid date - month 13",
			input:     "2025-13-01",
			shouldErr: true,
		},
		{
			name:      "empty string",
			input:     "",
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			date, err := ParseDate(tt.input)
			if tt.shouldErr {
				if err == nil {
					t.Errorf("ParseDate() expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("ParseDate() unexpected error: %v", err)
				}
				if date.String() != tt.expected {
					t.Errorf("ParseDate() = %v, want %v", date.String(), tt.expected)
				}
			}
		})
	}
}

func TestDate_String(t *testing.T) {
	date := NewDate(2025, time.July, 4)
	expected := "2025-07-04"
	if date.String() != expected {
		t.Errorf("Date.String() = %v, want %v", date.String(), expected)
	}
}

func TestDate_MarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		date     Date
		expected string
	}{
		{
			name:     "valid date",
			date:     NewDate(2025, time.July, 4),
			expected: `"2025-07-04"`,
		},
		{
			name:     "leap year date",
			date:     NewDate(2024, time.February, 29),
			expected: `"2024-02-29"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.date)
			if err != nil {
				t.Errorf("Date.MarshalJSON() error = %v", err)
				return
			}
			if string(data) != tt.expected {
				t.Errorf("Date.MarshalJSON() = %v, want %v", string(data), tt.expected)
			}
		})
	}
}

func TestDate_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  string
		shouldErr bool
	}{
		{
			name:      "valid date",
			input:     `"2025-07-04"`,
			expected:  "2025-07-04",
			shouldErr: false,
		},
		{
			name:      "leap year date",
			input:     `"2024-02-29"`,
			expected:  "2024-02-29",
			shouldErr: false,
		},
		{
			name:      "invalid JSON",
			input:     `2025-07-04`,
			shouldErr: true,
		},
		{
			name:      "invalid date format",
			input:     `"2025/07/04"`,
			shouldErr: true,
		},
		{
			name:      "invalid date",
			input:     `"2025-02-30"`,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var date Date
			err := json.Unmarshal([]byte(tt.input), &date)
			if tt.shouldErr {
				if err == nil {
					t.Errorf("Date.UnmarshalJSON() expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Date.UnmarshalJSON() unexpected error: %v", err)
				}
				if date.String() != tt.expected {
					t.Errorf("Date.UnmarshalJSON() = %v, want %v", date.String(), tt.expected)
				}
			}
		})
	}
}

func TestDate_MarshalYAML(t *testing.T) {
	tests := []struct {
		name     string
		date     Date
		expected string
	}{
		{
			name:     "valid date",
			date:     NewDate(2025, time.July, 4),
			expected: "\"2025-07-04\"\n",
		},
		{
			name:     "leap year date",
			date:     NewDate(2024, time.February, 29),
			expected: "\"2024-02-29\"\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := yaml.Marshal(tt.date)
			if err != nil {
				t.Errorf("Date.MarshalYAML() error = %v", err)
				return
			}
			if string(data) != tt.expected {
				t.Errorf("Date.MarshalYAML() = %v, want %v", string(data), tt.expected)
			}
		})
	}
}

func TestDate_UnmarshalYAML(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  string
		shouldErr bool
	}{
		{
			name:      "valid date",
			input:     "2025-07-04",
			expected:  "2025-07-04",
			shouldErr: false,
		},
		{
			name:      "leap year date",
			input:     "2024-02-29",
			expected:  "2024-02-29",
			shouldErr: false,
		},
		{
			name:      "invalid date format",
			input:     "2025/07/04",
			shouldErr: true,
		},
		{
			name:      "invalid date",
			input:     "2025-02-30",
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var date Date
			err := yaml.Unmarshal([]byte(tt.input), &date)
			if tt.shouldErr {
				if err == nil {
					t.Errorf("Date.UnmarshalYAML() expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Date.UnmarshalYAML() unexpected error: %v", err)
				}
				if date.String() != tt.expected {
					t.Errorf("Date.UnmarshalYAML() = %v, want %v", date.String(), tt.expected)
				}
			}
		})
	}
}

func TestDate_JSONRoundTrip(t *testing.T) {
	original := NewDate(2025, time.July, 4)

	// Marshal to JSON
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	// Unmarshal from JSON
	var unmarshaled Date
	err = json.Unmarshal(data, &unmarshaled)
	if err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	// Check if they're equal
	if original.String() != unmarshaled.String() {
		t.Errorf("JSON round trip failed: original = %v, unmarshaled = %v", original.String(), unmarshaled.String())
	}
}

func TestDate_YAMLRoundTrip(t *testing.T) {
	original := NewDate(2025, time.July, 4)

	// Marshal to YAML
	data, err := yaml.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	// Unmarshal from YAML
	var unmarshaled Date
	err = yaml.Unmarshal(data, &unmarshaled)
	if err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	// Check if they're equal
	if original.String() != unmarshaled.String() {
		t.Errorf("YAML round trip failed: original = %v, unmarshaled = %v", original.String(), unmarshaled.String())
	}
}

func TestDate_TimeCompatibility(t *testing.T) {
	date1 := NewDate(2025, time.July, 4)
	date2 := NewDate(2025, time.July, 5)
	date3 := NewDate(2025, time.July, 4)

	// Test compatibility with underlying time.Time methods
	if !date1.Time.Before(date2.Time) {
		t.Errorf("Expected %v to be before %v", date1.String(), date2.String())
	}

	if !date2.Time.After(date1.Time) {
		t.Errorf("Expected %v to be after %v", date2.String(), date1.String())
	}

	if !date1.Time.Equal(date3.Time) {
		t.Errorf("Expected %v to be equal to %v", date1.String(), date3.String())
	}
}

func TestDate_DateComparison(t *testing.T) {
	tests := []struct {
		name    string
		date1   Date
		date2   Date
		before  bool
		after   bool
		equal   bool
		compare int
	}{
		{
			name:    "date1 before date2",
			date1:   NewDate(2025, time.July, 4),
			date2:   NewDate(2025, time.July, 5),
			before:  true,
			after:   false,
			equal:   false,
			compare: -1,
		},
		{
			name:    "date1 after date2",
			date1:   NewDate(2025, time.July, 5),
			date2:   NewDate(2025, time.July, 4),
			before:  false,
			after:   true,
			equal:   false,
			compare: 1,
		},
		{
			name:    "dates equal",
			date1:   NewDate(2025, time.July, 4),
			date2:   NewDate(2025, time.July, 4),
			before:  false,
			after:   false,
			equal:   true,
			compare: 0,
		},
		{
			name:    "different years",
			date1:   NewDate(2024, time.December, 31),
			date2:   NewDate(2025, time.January, 1),
			before:  true,
			after:   false,
			equal:   false,
			compare: -1,
		},
		{
			name:    "leap year comparison",
			date1:   NewDate(2024, time.February, 29),
			date2:   NewDate(2025, time.February, 28),
			before:  true,
			after:   false,
			equal:   false,
			compare: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test Before
			if got := tt.date1.Before(tt.date2); got != tt.before {
				t.Errorf("Before() = %v, want %v", got, tt.before)
			}

			// Test After
			if got := tt.date1.After(tt.date2); got != tt.after {
				t.Errorf("After() = %v, want %v", got, tt.after)
			}

			// Test Equal
			if got := tt.date1.Equal(tt.date2); got != tt.equal {
				t.Errorf("Equal() = %v, want %v", got, tt.equal)
			}

			// Test Compare
			if got := tt.date1.Compare(tt.date2); got != tt.compare {
				t.Errorf("Compare() = %v, want %v", got, tt.compare)
			}
		})
	}
}

func TestDate_IsZero(t *testing.T) {
	tests := []struct {
		name     string
		date     Date
		expected bool
	}{
		{
			name:     "zero date",
			date:     Date{},
			expected: true,
		},
		{
			name:     "non-zero date",
			date:     NewDate(2025, time.July, 4),
			expected: false,
		},
		{
			name:     "explicit zero time",
			date:     Date{time.Time{}},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.date.IsZero(); got != tt.expected {
				t.Errorf("IsZero() = %v, want %v", got, tt.expected)
			}
		})
	}
}
