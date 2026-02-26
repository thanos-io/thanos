// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func boolPtr(b bool) *bool    { return &b }
func strPtr(s string) *string { return &s }

func TestPerEndpointTLSConfig(t *testing.T) {
	globalTLSConfig := &tlsConfig{
		Enabled:  boolPtr(true),
		CertFile: strPtr("/mock-path.pem"),
		KeyFile:  strPtr("/mock-key-path.pem"),
		CAFile:   strPtr("/mock-ca-path.pem"),
	}

	var (
		useGlobalConfigCount = 0
		tlsEnabledNilCount   = 0
		tlsEnabledTrueCount  = 0
		tlsEnabledFalseCount = 0
	)

	var testCases = []struct {
		endpointConfig        endpointSettings
		testName              string
		endpointEnableChanged bool
	}{
		{
			testName: "Empty endpoint config uses global TLS settings",
			endpointConfig: endpointSettings{
				Address:      "store1:9091",
				ClientConfig: clientConfig{},
			},
		},
		{
			testName: "Enabled is Nil but CertFile provided so will",
			endpointConfig: endpointSettings{
				Address: "store2:9091",
				ClientConfig: clientConfig{
					TLSConfig: tlsConfig{CertFile: strPtr("/mock-endpoint-cert-path.pem")},
				},
			},
			endpointEnableChanged: true,
		},
		{
			testName: "Only MinVersion set, inherits global enabled",
			endpointConfig: endpointSettings{
				Address: "store3:9091",
				ClientConfig: clientConfig{
					TLSConfig: tlsConfig{MinVersion: strPtr("1.2")},
				},
			},
			endpointEnableChanged: true,
		},
		{
			testName: "Enabled is explicitly set to true use defaults",
			endpointConfig: endpointSettings{
				Address: "store4:9091",
				ClientConfig: clientConfig{
					TLSConfig: tlsConfig{Enabled: boolPtr(true)},
				},
			},
		},
		{
			testName: "Enabled explicitly set to false",
			endpointConfig: endpointSettings{
				Address: "store5:9091",
				ClientConfig: clientConfig{
					TLSConfig: tlsConfig{Enabled: boolPtr(false)},
				},
			},
		},
	}

	for _, tc := range testCases {
		ecfg := tc.endpointConfig
		tlsEnabled := ecfg.ClientConfig.TLSConfig.Enabled
		useGlobalConfig := ecfg.ClientConfig.UseGlobalTLSOpts()

		if useGlobalConfig {
			useGlobalConfigCount++
		} else if tlsEnabled == nil {
			ecfg.ClientConfig.TLSConfig.applyDefaults(globalTLSConfig)
			tlsEnabledNilCount++

			// only checking if we inherited globalTLSConfig.Enabled
			// we are testing only global as enabled so just checking enabled=true
			if tc.endpointEnableChanged {
				testutil.Assert(t, ecfg.ClientConfig.TLSConfig.Enabled != nil, "%s: enabled should not be nil after merge", tc.testName)
				testutil.Equals(t, true, *ecfg.ClientConfig.TLSConfig.Enabled, "%s: enabled should be inferred as true", tc.testName)
			}
		} else if *tlsEnabled {
			ecfg.ClientConfig.TLSConfig.applyDefaults(nil)
			tlsEnabledTrueCount++
		} else {
			tlsEnabledFalseCount++
		}
	}

	testutil.Equals(t, 1, useGlobalConfigCount)
	testutil.Equals(t, 2, tlsEnabledNilCount)
	testutil.Equals(t, 1, tlsEnabledTrueCount)
	testutil.Equals(t, 1, tlsEnabledFalseCount)
}
