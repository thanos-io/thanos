// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tls

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCipherSuiteIDs(t *testing.T) {
	supported := tls.CipherSuites()
	require.NotEmpty(t, supported, "tls.CipherSuites() must return at least one suite")

	first := supported[0]
	second := supported[1]

	tests := []struct {
		name        string
		input       []string
		wantIDs     []uint16
		wantErr     bool
		errContains string
	}{
		{
			name:    "empty input returns nil",
			input:   []string{},
			wantIDs: nil,
		},
		{
			name:    "nil input returns nil",
			input:   nil,
			wantIDs: nil,
		},
		{
			name:    "single valid cipher",
			input:   []string{first.Name},
			wantIDs: []uint16{first.ID},
		},
		{
			name:    "multiple valid ciphers preserves order",
			input:   []string{second.Name, first.Name},
			wantIDs: []uint16{second.ID, first.ID},
		},
		{
			name:    "duplicate cipher returns duplicate IDs",
			input:   []string{first.Name, first.Name},
			wantIDs: []uint16{first.ID, first.ID},
		},
		{
			name:        "unknown cipher returns error",
			input:       []string{"INVALID_CIPHER"},
			wantErr:     true,
			errContains: "INVALID_CIPHER",
		},
		{
			name:        "error message contains valid cipher names",
			input:       []string{"BAD_CIPHER"},
			wantErr:     true,
			errContains: first.Name,
		},
		{
			name:        "valid cipher followed by invalid returns error",
			input:       []string{first.Name, "UNKNOWN"},
			wantErr:     true,
			errContains: "UNKNOWN",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ids, err := getCipherSuiteIDs(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantIDs, ids)
		})
	}
}

func TestGetCurveIDs(t *testing.T) {
	tests := []struct {
		name        string
		input       []string
		wantIDs     []tls.CurveID
		wantErr     bool
		errContains string
	}{
		{
			name:    "empty input returns nil",
			input:   []string{},
			wantIDs: nil,
		},
		{
			name:    "nil input returns nil",
			input:   nil,
			wantIDs: nil,
		},
		{
			name:    "single valid curve",
			input:   []string{"X25519"},
			wantIDs: []tls.CurveID{tls.X25519},
		},
		{
			name:    "multiple valid curves preserves order",
			input:   []string{"CurveP384", "X25519"},
			wantIDs: []tls.CurveID{tls.CurveP384, tls.X25519},
		},
		{
			name:    "all valid curves",
			input:   []string{"CurveP256", "CurveP384", "CurveP521", "X25519"},
			wantIDs: []tls.CurveID{tls.CurveP256, tls.CurveP384, tls.CurveP521, tls.X25519},
		},
		{
			name:    "duplicate curve returns duplicate IDs",
			input:   []string{"CurveP256", "CurveP256"},
			wantIDs: []tls.CurveID{tls.CurveP256, tls.CurveP256},
		},
		{
			name:        "unknown curve returns error",
			input:       []string{"INVALID_CURVE"},
			wantErr:     true,
			errContains: "INVALID_CURVE",
		},
		{
			name:        "error message contains valid curve names",
			input:       []string{"BAD_CURVE"},
			wantErr:     true,
			errContains: "CurveP256",
		},
		{
			name:        "valid curve followed by invalid returns error",
			input:       []string{"X25519", "UNKNOWN"},
			wantErr:     true,
			errContains: "UNKNOWN",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ids, err := getCurveIDs(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantIDs, ids)
		})
	}
}

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
