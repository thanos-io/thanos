// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package errutil

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestMultiSyncErrorAdd(t *testing.T) {
	sme := &SyncMultiError{}
	sme.Add(fmt.Errorf("test"))
}

func TestNonNilMultiErrorCause_SingleCause(t *testing.T) {
	rootCause := fmt.Errorf("test root cause")
	me := MultiError{}
	me.Add(errors.Wrap(rootCause, "wrapped error"))
	causes, ok := errors.Cause(NonNilMultiError(me)).(NonNilMultiRootError)
	require.True(t, ok)
	require.Equal(t, 1, len(causes))
	require.Equal(t, rootCause, causes[0])
}

func TestNonNilMultiErrorCause_MultipleCauses(t *testing.T) {
	rootCause1 := fmt.Errorf("test root cause 1")
	rootCause2 := fmt.Errorf("test root cause 2")
	rootCause3 := fmt.Errorf("test root cause 3")
	me := MultiError{}
	me.Add(errors.Wrap(rootCause1, "wrapped error 1"))
	me.Add(errors.Wrap(errors.Wrap(rootCause2, "wrapped error 2"), "wrapped error 2 again"))
	me.Add(rootCause3)
	causes, ok := errors.Cause(NonNilMultiError(me)).(NonNilMultiRootError)
	require.True(t, ok)
	require.Equal(t, 3, len(causes))
	require.Contains(t, causes, rootCause1)
	require.Contains(t, causes, rootCause2)
	require.Contains(t, causes, rootCause3)
}

func TestNonNilMultiErrorCause_MultipleCausesWithNestedNonNilMultiError(t *testing.T) {
	rootCause1 := fmt.Errorf("test root cause 1")
	rootCause2 := fmt.Errorf("test root cause 2")
	rootCause3 := fmt.Errorf("test root cause 3")
	me1 := MultiError{}
	me1.Add(errors.Wrap(rootCause1, "wrapped error 1"))
	me1.Add(errors.Wrap(rootCause2, "wrapped error 2"))
	me := MultiError{}
	me.Add(errors.Wrap(rootCause3, "wrapped error 3"))
	me.Add(NonNilMultiError(me1))
	causes, ok := errors.Cause(NonNilMultiError(me)).(NonNilMultiRootError)
	require.True(t, ok)
	require.Equal(t, 3, len(causes))
	require.Contains(t, causes, rootCause1)
	require.Contains(t, causes, rootCause2)
	require.Contains(t, causes, rootCause3)
}
