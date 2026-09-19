// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package errutil

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/pkg/errors"
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

// SyncMultiError is a thread-safe implementation of MultiError.
type SyncMultiError struct {
	mtx sync.Mutex
	es  MultiError
}

// Add adds the error to the error list if it is not nil.
func (es *SyncMultiError) Add(err error) {
	if err == nil {
		return
	}
	es.mtx.Lock()
	defer es.mtx.Unlock()

	es.es.Add(err)
}

// Err returns the error list as an error or nil if it is empty.
func (es *SyncMultiError) Err() error {
	es.mtx.Lock()
	defer es.mtx.Unlock()

	return es.es.Err()
}

type NonNilMultiError MultiError

// Returns a concatenated string of the contained errors.
func (es NonNilMultiError) Error() string {
	return multiErrorString(es)
}

func (es NonNilMultiError) Cause() error {
	return es.getCause()
}

func (es NonNilMultiError) Is(target error) bool {
	for _, err := range es {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func (es NonNilMultiError) getCause() NonNilMultiRootError {
	var causes []error
	for _, err := range es {
		if multiErr, ok := errors.Cause(err).(NonNilMultiError); ok {
			causes = append(causes, multiErr.getCause()...)
		} else {
			causes = append(causes, errors.Cause(err))
		}
	}
	return causes
}

type NonNilMultiRootError MultiError

// Returns a concatenated string of the contained errors.
func (es NonNilMultiRootError) Error() string {
	return multiErrorString(es)
}

func multiErrorString(es []error) string {
	var buf bytes.Buffer

	if len(es) > 1 {
		fmt.Fprintf(&buf, "%d errors: ", len(es))
	}

	for i, err := range es {
		if i != 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(err.Error())
	}

	return buf.String()
}
