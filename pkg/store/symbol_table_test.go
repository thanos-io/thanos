// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"math"
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestSymbolTableHandlesBigNumbersCorrectly(t *testing.T) {
	adjuster := newReferenceAdjusterFactory(2)
	adjust := adjuster(0)

	testutil.Assert(t, adjust(0) != adjust(math.MaxUint64/2))
}
