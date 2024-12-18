// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Provenance-includes-location: https://github.com/golang/go/blob/f2d118fd5f7e872804a5825ce29797f81a28b0fa/src/strings/strings.go
// Provenance-includes-license: BSD-3-Clause
// Provenance-includes-copyright: Copyright The Go Authors.

package otlptranslator

import "strings"

// fieldsFunc is a copy of strings.FieldsFunc from the Go standard library,
// but it also returns the separators as part of the result.
func fieldsFunc(s string, f func(rune) bool) ([]string, []string) {
	// A span is used to record a slice of s of the form s[start:end].
	// The start index is inclusive and the end index is exclusive.
	type span struct {
		start int
		end   int
	}
	spans := make([]span, 0, 32)
	separators := make([]string, 0, 32)

	// Find the field start and end indices.
	// Doing this in a separate pass (rather than slicing the string s
	// and collecting the result substrings right away) is significantly
	// more efficient, possibly due to cache effects.
	start := -1 // valid span start if >= 0
	for end, rune := range s {
		if f(rune) {
			if start >= 0 {
				spans = append(spans, span{start, end})
				// Set start to a negative value.
				// Note: using -1 here consistently and reproducibly
				// slows down this code by a several percent on amd64.
				start = ^start
				separators = append(separators, string(s[end]))
			}
		} else {
			if start < 0 {
				start = end
			}
		}
	}

	// Last field might end at EOF.
	if start >= 0 {
		spans = append(spans, span{start, len(s)})
	}

	// Create strings from recorded field indices.
	a := make([]string, len(spans))
	for i, span := range spans {
		a[i] = s[span.start:span.end]
	}

	return a, separators
}

// join is a copy of strings.Join from the Go standard library,
// but it also accepts a slice of separators to join the elements with.
// If the slice of separators is shorter than the slice of elements, use a default value.
// We also don't check for integer overflow.
func join(elems []string, separators []string, def string) string {
	switch len(elems) {
	case 0:
		return ""
	case 1:
		return elems[0]
	}

	var n int
	var sep string
	sepLen := len(separators)
	for i, elem := range elems {
		if i >= sepLen {
			sep = def
		} else {
			sep = separators[i]
		}
		n += len(sep) + len(elem)
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteString(elems[0])
	for i, s := range elems[1:] {
		if i >= sepLen {
			sep = def
		} else {
			sep = separators[i]
		}
		b.WriteString(sep)
		b.WriteString(s)
	}
	return b.String()
}
