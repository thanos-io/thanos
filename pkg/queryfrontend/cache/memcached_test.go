// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"testing"

	"github.com/go-kit/kit/log"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestNewMemcachedCache(t *testing.T) {
	for _, tc := range []struct {
		name      string
		conf      string
		expectErr bool
	}{
		{
			name: "address and dns update interval",
			conf: `
addresses: ["localhost:11211"]
dns_provider_update_interval: 1s
`,
			expectErr: false,
		},
		{
			name: "validity",
			conf: `
addresses: ["localhost:11211"]
dns_provider_update_interval: 1s
validity: 6h
`,
			expectErr: false,
		},
		{
			name: "address not set",
			conf: `
dns_provider_update_interval: 1s
`,
			expectErr: true,
		},
		{
			name: "failed to unmarshal because of invalid field",
			conf: `
aaa: 111
dns_provider_update_interval: 1s
`,
			expectErr: true,
		},
	} {
		_, err := newMemcachedCache([]byte(tc.conf), log.NewNopLogger(), nil)
		if tc.expectErr {
			testutil.NotOk(t, err)
		} else {
			testutil.Ok(t, err)
		}
	}
}
