// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil/custom"
)

func TestMain(m *testing.M) {
	custom.TolerantVerifyLeakMain(m)
}
