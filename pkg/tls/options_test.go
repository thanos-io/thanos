// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tls

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTlsOptions(t *testing.T) {
	var tests = []struct {
		input  string
		fail   bool
		result uint16
	}{
		{
			input: "",
			fail:  true,
		}, {
			input: "ab",
			fail:  true,
		}, {
			input: "1",
			fail:  true,
		}, {
			input:  "1.0",
			result: tls.VersionTLS10,
		},
		{
			input:  "1.1",
			result: tls.VersionTLS11,
		},
		{
			input:  "1.2",
			result: tls.VersionTLS12,
		},
		{
			input:  "1.3",
			result: tls.VersionTLS13,
		},
	}

	for _, test := range tests {
		minTlsVersion, err := getTlsVersion(test.input)

		if test.fail {
			require.Error(t, err)
			continue
		}

		require.NoError(t, err)
		assert.Equal(t, test.result, minTlsVersion)
	}
}
