// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package route

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/errutil"
)

var (
	// errConflict is returned whenever an operation fails due to any conflict-type error.
	errConflict = errors.New("conflict")

	errBadReplica  = errors.New("replica count exceeds replication factor")
	errNotReady    = errors.New("target not ready")
	errUnavailable = errors.New("target not available")
)

type expectedErrors []*struct {
	err   error
	cause func(error) bool
	count int
}

func (a expectedErrors) Len() int           { return len(a) }
func (a expectedErrors) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a expectedErrors) Less(i, j int) bool { return a[i].count < a[j].count }

// determineWriteErrorCause extracts a sentinel error that has occurred more than the given threshold from a given fan-out error.
// It will inspect the error's cause if the error is a MultiError,
// It will return cause of each contained error but will not traverse any deeper.
func determineWriteErrorCause(err error, threshold int) error {
	if err == nil {
		return nil
	}

	unwrappedErr := errors.Cause(err)
	errs, ok := unwrappedErr.(errutil.MultiError)
	if !ok {
		errs = []error{unwrappedErr}
	}
	if len(errs) == 0 {
		return nil
	}

	if threshold < 1 {
		return err
	}

	expErrs := expectedErrors{
		{err: errConflict, cause: isConflict},
		{err: errNotReady, cause: isNotReady},
		{err: errUnavailable, cause: isUnavailable},
	}
	for _, exp := range expErrs {
		exp.count = 0
		for _, err := range errs {
			if exp.cause(errors.Cause(err)) {
				exp.count++
			}
		}
	}
	// Determine which error occurred most.
	sort.Sort(sort.Reverse(expErrs))
	if exp := expErrs[0]; exp.count >= threshold {
		return exp.err
	}

	return err
}
