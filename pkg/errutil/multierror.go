// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package errutil

import (
	"bytes"
	"fmt"
	"sort"
)

// The MultiError type implements the error interface, and contains the
// Errors used to construct it.
type MultiError []error

// Add adds the error to the error list if it is not nil.
func (es *MultiError) Add(err error) {
	if err == nil {
		return
	}
	if merr, ok := err.(NonNilMultiError); ok {
		*es = append(*es, merr...)
	} else {
		*es = append(*es, err)
	}
}

// Err returns the error list as an error or nil if it is empty.
func (es MultiError) Err() error {
	if len(es) == 0 {
		return nil
	}
	return NonNilMultiError(es)
}

type NonNilMultiError MultiError

// Error returns a concatenated string of the contained errors.
func (es NonNilMultiError) Error() string {
	if len(es) == 0 {
		return ""
	}
	if len(es) == 1 {
		return es[0].Error()
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%d errors: ", len(es))

	for i, err := range es {
		if i != 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(err.Error())
	}
	return buf.String()
}

type SortableError interface {
	SortedError() string
}

var _ SortableError = NonNilMultiError(nil)

// SortedError returns sorted concatenated string of the contained errors.
func (es NonNilMultiError) SortedError() string {
	if len(es) == 0 {
		return ""
	}
	if len(es) == 1 {
		if sortable, ok := es[0].(SortableError); ok {
			return sortable.SortedError()
		}
		return es[0].Error()
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%d (sorted) errors: ", len(es))

	// Sort for stable output.
	s := make([]string, len(es))
	for i, err := range es {
		if sortable, ok := es[0].(SortableError); ok {
			s[i] = sortable.SortedError()
			continue
		}
		s[i] = err.Error()
	}
	sort.Strings(s)
	for i, err := range s {
		if i != 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(err)
	}
	return buf.String()
}
