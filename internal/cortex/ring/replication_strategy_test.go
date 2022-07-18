// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package ring

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRingReplicationStrategy(t *testing.T) {
	for i, tc := range []struct {
		replicationFactor, liveIngesters, deadIngesters int
		expectedMaxFailure                              int
		expectedError                                   string
	}{
		// Ensure it works for a single ingester, for local testing.
		{
			replicationFactor:  1,
			liveIngesters:      1,
			expectedMaxFailure: 0,
		},

		{
			replicationFactor: 1,
			deadIngesters:     1,
			expectedError:     "at least 1 live replicas required, could only find 0 - unhealthy instances: dead1",
		},

		// Ensure it works for RF=3 and 2 ingesters.
		{
			replicationFactor:  3,
			liveIngesters:      2,
			expectedMaxFailure: 0,
		},

		// Ensure it works for the default production config.
		{
			replicationFactor:  3,
			liveIngesters:      3,
			expectedMaxFailure: 1,
		},

		{
			replicationFactor:  3,
			liveIngesters:      2,
			deadIngesters:      1,
			expectedMaxFailure: 0,
		},

		{
			replicationFactor: 3,
			liveIngesters:     1,
			deadIngesters:     2,
			expectedError:     "at least 2 live replicas required, could only find 1 - unhealthy instances: dead1,dead2",
		},

		// Ensure it works when adding / removing nodes.

		// A node is joining or leaving, replica set expands.
		{
			replicationFactor:  3,
			liveIngesters:      4,
			expectedMaxFailure: 1,
		},

		{
			replicationFactor:  3,
			liveIngesters:      3,
			deadIngesters:      1,
			expectedMaxFailure: 1,
		},

		{
			replicationFactor:  3,
			liveIngesters:      2,
			deadIngesters:      2,
			expectedMaxFailure: 0,
		},
	} {
		ingesters := []InstanceDesc{}
		for i := 0; i < tc.liveIngesters; i++ {
			ingesters = append(ingesters, InstanceDesc{
				Timestamp: time.Now().Unix(),
			})
		}
		for i := 0; i < tc.deadIngesters; i++ {
			ingesters = append(ingesters, InstanceDesc{Addr: fmt.Sprintf("dead%d", i+1)})
		}

		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			strategy := NewDefaultReplicationStrategy()
			liveIngesters, maxFailure, err := strategy.Filter(ingesters, Read, tc.replicationFactor, 100*time.Second, false)
			if tc.expectedError == "" {
				assert.NoError(t, err)
				assert.Equal(t, tc.liveIngesters, len(liveIngesters))
				assert.Equal(t, tc.expectedMaxFailure, maxFailure)
			} else {
				assert.EqualError(t, err, tc.expectedError)
			}
		})
	}
}

func TestIgnoreUnhealthyInstancesReplicationStrategy(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		liveIngesters, deadIngesters int
		expectedMaxFailure           int
		expectedError                string
	}{
		{
			name:               "with at least 1 healthy instance",
			liveIngesters:      1,
			expectedMaxFailure: 0,
		},
		{
			name:               "with more healthy instances than unhealthy",
			deadIngesters:      1,
			liveIngesters:      2,
			expectedMaxFailure: 1,
		},
		{
			name:               "with more unhealthy instances than healthy",
			deadIngesters:      1,
			liveIngesters:      2,
			expectedMaxFailure: 1,
		},
		{
			name:               "with equal number of healthy and unhealthy instances",
			deadIngesters:      2,
			liveIngesters:      2,
			expectedMaxFailure: 1,
		},
		{
			name:               "with no healthy instances",
			liveIngesters:      0,
			deadIngesters:      3,
			expectedMaxFailure: 0,
			expectedError:      "at least 1 healthy replica required, could only find 0 - unhealthy instances: dead1,dead2,dead3",
		},
	} {
		ingesters := []InstanceDesc{}
		for i := 0; i < tc.liveIngesters; i++ {
			ingesters = append(ingesters, InstanceDesc{
				Timestamp: time.Now().Unix(),
			})
		}
		for i := 0; i < tc.deadIngesters; i++ {
			ingesters = append(ingesters, InstanceDesc{Addr: fmt.Sprintf("dead%d", i+1)})
		}

		t.Run(tc.name, func(t *testing.T) {
			strategy := NewIgnoreUnhealthyInstancesReplicationStrategy()
			liveIngesters, maxFailure, err := strategy.Filter(ingesters, Read, 3, 100*time.Second, false)
			if tc.expectedError == "" {
				assert.NoError(t, err)
				assert.Equal(t, tc.liveIngesters, len(liveIngesters))
				assert.Equal(t, tc.expectedMaxFailure, maxFailure)
			} else {
				assert.EqualError(t, err, tc.expectedError)
			}
		})
	}
}
