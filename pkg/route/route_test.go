// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package route

import (
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMain(m *testing.M) {
	testutil.TolerantVerifyLeakMain(m)
}
