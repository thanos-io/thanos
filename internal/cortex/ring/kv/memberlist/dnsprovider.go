// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package memberlist

import (
	"context"
)

// DNSProvider supports storing or resolving a list of addresses.
type DNSProvider interface {
	// Resolve stores a list of provided addresses or their DNS records if requested.
	// Implementations may have specific ways of interpreting addresses.
	Resolve(ctx context.Context, addrs []string) error

	// Addresses returns the latest addresses present in the DNSProvider.
	Addresses() []string
}
