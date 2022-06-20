// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package testutil

import "testing"

func TestContains(t *testing.T) {
	tests := map[string]struct {
		haystack    []string
		needle      []string
		shouldMatch bool
	}{
		"empty haystack": {
			haystack:    []string{},
			needle:      []string{"key1"},
			shouldMatch: false,
		},

		"empty needle": {
			haystack:    []string{"key1", "key2", "key3"},
			needle:      []string{},
			shouldMatch: false,
		},

		"single value needle": {
			haystack:    []string{"key1", "key2", "key3"},
			needle:      []string{"key1"},
			shouldMatch: true,
		},

		"multiple value needle": {
			haystack:    []string{"key1", "key2", "key3"},
			needle:      []string{"key1", "key2"},
			shouldMatch: true,
		},

		"same size needle as haystack": {
			haystack:    []string{"key1", "key2", "key3"},
			needle:      []string{"key1", "key2", "key3"},
			shouldMatch: true,
		},

		"larger needle than haystack": {
			haystack:    []string{"key1", "key2", "key3"},
			needle:      []string{"key1", "key2", "key3", "key4"},
			shouldMatch: false,
		},

		"needle not contained": {
			haystack:    []string{"key1", "key2", "key3"},
			needle:      []string{"key4"},
			shouldMatch: false,
		},

		"haystack ends before needle": {
			haystack:    []string{"key1", "key2", "key3"},
			needle:      []string{"key3", "key4"},
			shouldMatch: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			if testData.shouldMatch != contains(testData.haystack, testData.needle) {
				t.Fatalf("unexpected result testing contains() with %#v", testData)
			}
		})
	}
}
