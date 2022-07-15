// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package ring

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultResultTracker(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1"}
	instance2 := InstanceDesc{Addr: "127.0.0.2"}
	instance3 := InstanceDesc{Addr: "127.0.0.3"}
	instance4 := InstanceDesc{Addr: "127.0.0.4"}

	tests := map[string]struct {
		instances []InstanceDesc
		maxErrors int
		run       func(t *testing.T, tracker *defaultResultTracker)
	}{
		"should succeed on no instances to track": {
			instances: nil,
			maxErrors: 0,
			run: func(t *testing.T, tracker *defaultResultTracker) {
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())
			},
		},
		"should succeed once all instances succeed on max errors = 0": {
			instances: []InstanceDesc{instance1, instance2, instance3, instance4},
			maxErrors: 0,
			run: func(t *testing.T, tracker *defaultResultTracker) {
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance2, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance3, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance4, nil)
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())
			},
		},
		"should fail on 1st failing instance on max errors = 0": {
			instances: []InstanceDesc{instance1, instance2, instance3, instance4},
			maxErrors: 0,
			run: func(t *testing.T, tracker *defaultResultTracker) {
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance2, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.True(t, tracker.failed())
			},
		},
		"should fail on 2nd failing instance on max errors = 1": {
			instances: []InstanceDesc{instance1, instance2, instance3, instance4},
			maxErrors: 1,
			run: func(t *testing.T, tracker *defaultResultTracker) {
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance2, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance3, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.True(t, tracker.failed())
			},
		},
		"should fail on 3rd failing instance on max errors = 2": {
			instances: []InstanceDesc{instance1, instance2, instance3, instance4},
			maxErrors: 2,
			run: func(t *testing.T, tracker *defaultResultTracker) {
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance2, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance3, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance4, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.True(t, tracker.failed())
			},
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			testCase.run(t, newDefaultResultTracker(testCase.instances, testCase.maxErrors))
		})
	}
}

func TestZoneAwareResultTracker(t *testing.T) {
	instance1 := InstanceDesc{Addr: "127.0.0.1", Zone: "zone-a"}
	instance2 := InstanceDesc{Addr: "127.0.0.2", Zone: "zone-a"}
	instance3 := InstanceDesc{Addr: "127.0.0.3", Zone: "zone-b"}
	instance4 := InstanceDesc{Addr: "127.0.0.4", Zone: "zone-b"}
	instance5 := InstanceDesc{Addr: "127.0.0.5", Zone: "zone-c"}
	instance6 := InstanceDesc{Addr: "127.0.0.6", Zone: "zone-c"}

	tests := map[string]struct {
		instances           []InstanceDesc
		maxUnavailableZones int
		run                 func(t *testing.T, tracker *zoneAwareResultTracker)
	}{
		"should succeed on no instances to track": {
			instances:           nil,
			maxUnavailableZones: 0,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())
			},
		},
		"should succeed once all instances succeed on max unavailable zones = 0": {
			instances:           []InstanceDesc{instance1, instance2, instance3},
			maxUnavailableZones: 0,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance2, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance3, nil)
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())
			},
		},
		"should fail on 1st failing instance on max unavailable zones = 0": {
			instances:           []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6},
			maxUnavailableZones: 0,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance2, errors.New("test"))
				assert.False(t, tracker.succeeded())
				assert.True(t, tracker.failed())
			},
		},
		"should succeed on 2 failing instances within the same zone on max unavailable zones = 1": {
			instances:           []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6},
			maxUnavailableZones: 1,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				// Track failing instances.
				for _, instance := range []InstanceDesc{instance1, instance2} {
					tracker.done(&instance, errors.New("test"))
					assert.False(t, tracker.succeeded())
					assert.False(t, tracker.failed())
				}

				// Track successful instances.
				for _, instance := range []InstanceDesc{instance3, instance4, instance5} {
					tracker.done(&instance, nil)
					assert.False(t, tracker.succeeded())
					assert.False(t, tracker.failed())
				}

				tracker.done(&instance6, nil)
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())
			},
		},
		"should succeed as soon as the response has been successfully received from 'all zones - 1' on max unavailable zones = 1": {
			instances:           []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6},
			maxUnavailableZones: 1,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				// Track successful instances.
				for _, instance := range []InstanceDesc{instance1, instance2, instance3} {
					tracker.done(&instance, nil)
					assert.False(t, tracker.succeeded())
					assert.False(t, tracker.failed())
				}

				tracker.done(&instance4, nil)
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())
			},
		},
		"should succeed on failing instances within 2 zones on max unavailable zones = 2": {
			instances:           []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6},
			maxUnavailableZones: 2,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				// Track failing instances.
				for _, instance := range []InstanceDesc{instance1, instance2, instance3, instance4} {
					tracker.done(&instance, errors.New("test"))
					assert.False(t, tracker.succeeded())
					assert.False(t, tracker.failed())
				}

				// Track successful instances.
				tracker.done(&instance5, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				tracker.done(&instance6, nil)
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())
			},
		},
		"should succeed as soon as the response has been successfully received from 'all zones - 2' on max unavailable zones = 2": {
			instances:           []InstanceDesc{instance1, instance2, instance3, instance4, instance5, instance6},
			maxUnavailableZones: 2,
			run: func(t *testing.T, tracker *zoneAwareResultTracker) {
				// Zone-a
				tracker.done(&instance1, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				// Zone-b
				tracker.done(&instance3, nil)
				assert.False(t, tracker.succeeded())
				assert.False(t, tracker.failed())

				// Zone-a
				tracker.done(&instance2, nil)
				assert.True(t, tracker.succeeded())
				assert.False(t, tracker.failed())
			},
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			testCase.run(t, newZoneAwareResultTracker(testCase.instances, testCase.maxUnavailableZones))
		})
	}
}
