// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package client

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/thanos-io/thanos/internal/cortex/ring"
)

func TestNewRingServiceDiscovery(t *testing.T) {
	tests := map[string]struct {
		ringReplicationSet ring.ReplicationSet
		ringErr            error
		expectedAddrs      []string
		expectedErr        error
	}{
		"discovery failure": {
			ringErr:     errors.New("mocked error"),
			expectedErr: errors.New("mocked error"),
		},
		"empty ring": {
			ringErr:       ring.ErrEmptyRing,
			expectedAddrs: nil,
		},
		"empty replication set": {
			ringReplicationSet: ring.ReplicationSet{
				Instances: []ring.InstanceDesc{},
			},
			expectedAddrs: nil,
		},
		"replication containing some endpoints": {
			ringReplicationSet: ring.ReplicationSet{
				Instances: []ring.InstanceDesc{
					{Addr: "1.1.1.1"},
					{Addr: "2.2.2.2"},
				},
			},
			expectedAddrs: []string{"1.1.1.1", "2.2.2.2"},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			r := &mockReadRing{}
			r.mockedReplicationSet = testData.ringReplicationSet
			r.mockedErr = testData.ringErr

			d := NewRingServiceDiscovery(r)
			addrs, err := d()
			assert.Equal(t, testData.expectedErr, err)
			assert.Equal(t, testData.expectedAddrs, addrs)
		})
	}
}

type mockReadRing struct {
	ring.ReadRing

	mockedReplicationSet ring.ReplicationSet
	mockedErr            error
}

func (m *mockReadRing) GetAllHealthy(_ ring.Operation) (ring.ReplicationSet, error) {
	return m.mockedReplicationSet, m.mockedErr
}
