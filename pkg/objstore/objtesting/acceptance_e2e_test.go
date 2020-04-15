// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objtesting

import (
	"testing"

	"github.com/thanos-io/thanos/pkg/objstore"
)

// TestObjStoreAcceptanceTest_e2e tests all known implementation against interface behaviour contract we agreed on.
// This ensures consistent behaviour across all implementations.
// NOTE: This test assumes strong consistency, but in the same way it does not guarantee that if it passes, the
// used object store is strongly consistent.
func TestObjStore_AcceptanceTest_e2e(t *testing.T) {
	ForeachStore(t, objstore.AcceptanceTest)
}
