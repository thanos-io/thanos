// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extannotations

import "testing"

func TestIsPromQLAnnotation(t *testing.T) {
	for _, tc := range []struct {
		name string
		s    string
		want bool
	}{
		{"info annotation", "PromQL info: metric might not be a counter", true},
		{"warning annotation", "PromQL warning: query timestamp out of bounds", true},
		{"unrelated annotation", "store warning: some store warning", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPromQLAnnotation(tc.s); got != tc.want {
				t.Errorf("IsPromQLAnnotation(%q) = %v, want %v", tc.s, got, tc.want)
			}
		})
	}
}

func TestIsPromQLInfoAnnotation(t *testing.T) {
	for _, tc := range []struct {
		name string
		s    string
		want bool
	}{
		{"info annotation", "PromQL info: metric might not be a counter", true},
		{"warning annotation", "PromQL warning: query timestamp out of bounds", false},
		{"unrelated annotation", "store warning: some store warning", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPromQLInfoAnnotation(tc.s); got != tc.want {
				t.Errorf("IsPromQLInfoAnnotation(%q) = %v, want %v", tc.s, got, tc.want)
			}
		})
	}
}
