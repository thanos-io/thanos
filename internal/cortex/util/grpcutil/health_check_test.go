// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package grpcutil

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/internal/cortex/util/services"
)

func TestHealthCheck_isHealthy(t *testing.T) {
	tests := map[string]struct {
		states   []services.State
		expected bool
	}{
		"all services are new": {
			states:   []services.State{services.New, services.New},
			expected: false,
		},
		"all services are starting": {
			states:   []services.State{services.Starting, services.Starting},
			expected: false,
		},
		"some services are starting and some running": {
			states:   []services.State{services.Starting, services.Running},
			expected: false,
		},
		"all services are running": {
			states:   []services.State{services.Running, services.Running},
			expected: true,
		},
		"some services are stopping": {
			states:   []services.State{services.Running, services.Stopping},
			expected: true,
		},
		"some services are terminated while others running": {
			states:   []services.State{services.Running, services.Terminated},
			expected: true,
		},
		"all services are stopping": {
			states:   []services.State{services.Stopping, services.Stopping},
			expected: true,
		},
		"some services are terminated while others stopping": {
			states:   []services.State{services.Stopping, services.Terminated},
			expected: true,
		},
		"a service has failed while others are running": {
			states:   []services.State{services.Running, services.Failed},
			expected: false,
		},
		"all services are terminated": {
			states:   []services.State{services.Terminated, services.Terminated},
			expected: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			var svcs []services.Service
			for range testData.states {
				svcs = append(svcs, &mockService{})
			}

			sm, err := services.NewManager(svcs...)
			require.NoError(t, err)

			// Switch the state of each mocked services.
			for i, s := range svcs {
				s.(*mockService).switchState(testData.states[i])
			}

			h := NewHealthCheck(sm)
			assert.Equal(t, testData.expected, h.isHealthy())
		})
	}
}

type mockService struct {
	services.Service
	state     services.State
	listeners []services.Listener
}

func (s *mockService) switchState(desiredState services.State) {
	// Simulate all the states between the current state and the desired one.
	orderedStates := []services.State{services.New, services.Starting, services.Running, services.Failed, services.Stopping, services.Terminated}
	simulationStarted := false

	for _, orderedState := range orderedStates {
		// Skip until we reach the current state.
		if !simulationStarted && orderedState != s.state {
			continue
		}

		// Start the simulation once we reach the current state.
		if orderedState == s.state {
			simulationStarted = true
			continue
		}

		// Skip the failed state, unless it's the desired one.
		if orderedState == services.Failed && desiredState != services.Failed {
			continue
		}

		s.state = orderedState

		// Synchronously call listeners to avoid flaky tests.
		for _, listener := range s.listeners {
			switch orderedState {
			case services.Starting:
				listener.Starting()
			case services.Running:
				listener.Running()
			case services.Stopping:
				listener.Stopping(services.Running)
			case services.Failed:
				listener.Failed(services.Running, errors.New("mocked error"))
			case services.Terminated:
				listener.Terminated(services.Stopping)
			}
		}

		if orderedState == desiredState {
			break
		}
	}
}

func (s *mockService) State() services.State {
	return s.state
}

func (s *mockService) AddListener(listener services.Listener) {
	s.listeners = append(s.listeners, listener)
}

func (s *mockService) StartAsync(_ context.Context) error      { return nil }
func (s *mockService) AwaitRunning(_ context.Context) error    { return nil }
func (s *mockService) StopAsync()                              {}
func (s *mockService) AwaitTerminated(_ context.Context) error { return nil }
func (s *mockService) FailureCase() error                      { return nil }
