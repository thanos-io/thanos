// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package errutil

import (
	"fmt"
	"testing"
)

func TestMultiSyncErrorAdd(t *testing.T) {
	sme := &SyncMultiError{}
	sme.Add(fmt.Errorf("test"))
}
