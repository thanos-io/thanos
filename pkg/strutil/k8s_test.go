// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package strutil

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestExtractPodOrdinal(t *testing.T) {
	testCases := map[string]struct {
		inputDNS        string
		expectedOrdinal int
		expectError     bool
	}{
		"valid DNS without port": {
			inputDNS:        "thanos-receive-0.thanos-receive-headless.monitoring.svc.cluster.local",
			expectedOrdinal: 0,
			expectError:     false,
		},
		"valid DNS with port": {
			inputDNS:        "thanos-receive-10.thanos-receive-headless.monitoring.svc.cluster.local:10901",
			expectedOrdinal: 10,
			expectError:     false,
		},
		"valid DNS with multiple hyphens in name": {
			inputDNS:        "my-complex-statefulset-name-5.headless.prod.svc.cluster.local",
			expectedOrdinal: 5,
			expectError:     false,
		},
		"invalid DNS format - wrong domain": {
			inputDNS:        "thanos-receive-0.thanos-receive-headless.monitoring.svc.cluster.example.com",
			expectedOrdinal: -1,
			expectError:     true,
		},
		"invalid DNS format - missing service/namespace": {
			inputDNS:        "thanos-receive-0.svc.cluster.local",
			expectedOrdinal: -1,
			expectError:     true,
		},
		"invalid DNS format - no ordinal": {
			inputDNS:        "thanos-receive.thanos-receive-headless.monitoring.svc.cluster.local",
			expectedOrdinal: -1,
			expectError:     true,
		},
		"invalid DNS format - non-numeric ordinal": {
			inputDNS:        "thanos-receive-abc.thanos-receive-headless.monitoring.svc.cluster.local",
			expectedOrdinal: -1,
			expectError:     true,
		},
		"invalid DNS format - ordinal not at the end of pod name part": {
			inputDNS:        "thanos-receive-0-backup.thanos-receive-headless.monitoring.svc.cluster.local",
			expectedOrdinal: -1, // The regex expects \d+ followed by a dot.
			expectError:     true,
		},
		"empty input string": {
			inputDNS:        "",
			expectedOrdinal: -1,
			expectError:     true,
		},
		"just hostname": {
			inputDNS:        "my-hostname-1",
			expectedOrdinal: -1,
			expectError:     true,
		},
		"just hostname with port": {
			inputDNS:        "my-hostname-1:8080",
			expectedOrdinal: -1,
			expectError:     true,
		},
		"DNS like string but not matching pattern": {
			inputDNS:        "pod-1.service.namespace",
			expectedOrdinal: -1,
			expectError:     true,
		},
	}

	for tcName, tc := range testCases {
		t.Run(tcName, func(t *testing.T) {
			ordinal, err := ExtractPodOrdinal(tc.inputDNS)

			if tc.expectError {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, tc.expectedOrdinal, ordinal)
			}
		})
	}
}
