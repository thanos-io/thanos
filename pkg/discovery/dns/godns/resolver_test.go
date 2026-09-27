// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package godns

import (
	"context"
	"net"
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestResolverLookupIPAddrByNetworkPreservesLiteralIPv6Zone(t *testing.T) {
	resolver := Resolver{Resolver: net.DefaultResolver}

	addrs, err := resolver.LookupIPAddrByNetwork(context.Background(), "ip6", "fe80::1%eth0")
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(addrs))
	testutil.Equals(t, "eth0", addrs[0].Zone)
}
