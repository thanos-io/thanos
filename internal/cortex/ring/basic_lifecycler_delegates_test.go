// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package ring

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/internal/cortex/util/concurrency"
	"github.com/thanos-io/thanos/internal/cortex/util/services"
	"github.com/thanos-io/thanos/internal/cortex/util/test"
)

func TestLeaveOnStoppingDelegate(t *testing.T) {
	onStoppingCalled := false

	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()

	testDelegate := &mockDelegate{
		onStopping: func(l *BasicLifecycler) {
			assert.Equal(t, LEAVING, l.GetState())
			onStoppingCalled = true
		},
	}

	leaveDelegate := NewLeaveOnStoppingDelegate(testDelegate, log.NewNopLogger())
	lifecycler, _, err := prepareBasicLifecyclerWithDelegate(t, cfg, leaveDelegate)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))

	assert.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))
	assert.True(t, onStoppingCalled)
}

func TestTokensPersistencyDelegate_ShouldSkipTokensLoadingIfFileDoesNotExist(t *testing.T) {
	// Create a temporary file and immediately delete it.
	tokensFile, err := os.CreateTemp("", "tokens-*")
	require.NoError(t, err)
	require.NoError(t, os.Remove(tokensFile.Name()))

	testDelegate := &mockDelegate{
		onRegister: func(lifecycler *BasicLifecycler, ringDesc Desc, instanceExists bool, instanceID string, instanceDesc InstanceDesc) (InstanceState, Tokens) {
			assert.False(t, instanceExists)
			return JOINING, Tokens{1, 2, 3, 4, 5}
		},
	}

	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)
	persistencyDelegate := NewTokensPersistencyDelegate(tokensFile.Name(), ACTIVE, testDelegate, logger)

	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()
	lifecycler, _, err := prepareBasicLifecyclerWithDelegate(t, cfg, persistencyDelegate)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, lifecycler) //nolint:errcheck

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
	assert.Equal(t, JOINING, lifecycler.GetState())
	assert.Equal(t, Tokens{1, 2, 3, 4, 5}, lifecycler.GetTokens())
	assert.True(t, lifecycler.IsRegistered())

	require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))

	// Ensure tokens have been stored.
	actualTokens, err := LoadTokensFromFile(tokensFile.Name())
	require.NoError(t, err)
	assert.Equal(t, Tokens{1, 2, 3, 4, 5}, actualTokens)

	// Ensure no error has been logged.
	assert.Empty(t, logs.String())
}

func TestTokensPersistencyDelegate_ShouldLoadTokensFromFileIfFileExist(t *testing.T) {
	tokensFile, err := os.CreateTemp("", "tokens-*")
	require.NoError(t, err)
	defer os.Remove(tokensFile.Name()) //nolint:errcheck

	// Store some tokens to the file.
	storedTokens := Tokens{6, 7, 8, 9, 10}
	require.NoError(t, storedTokens.StoreToFile(tokensFile.Name()))

	testDelegate := &mockDelegate{
		onRegister: func(lifecycler *BasicLifecycler, ringDesc Desc, instanceExists bool, instanceID string, instanceDesc InstanceDesc) (InstanceState, Tokens) {
			assert.True(t, instanceExists)
			assert.Equal(t, ACTIVE, instanceDesc.GetState())
			assert.Equal(t, storedTokens, Tokens(instanceDesc.GetTokens()))
			assert.True(t, instanceDesc.GetRegisteredAt().IsZero())

			return instanceDesc.GetState(), instanceDesc.GetTokens()
		},
	}

	persistencyDelegate := NewTokensPersistencyDelegate(tokensFile.Name(), ACTIVE, testDelegate, log.NewNopLogger())

	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()
	lifecycler, _, err := prepareBasicLifecyclerWithDelegate(t, cfg, persistencyDelegate)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
	assert.Equal(t, ACTIVE, lifecycler.GetState())
	assert.Equal(t, storedTokens, lifecycler.GetTokens())
	assert.True(t, lifecycler.IsRegistered())
	assert.InDelta(t, time.Now().Unix(), lifecycler.GetRegisteredAt().Unix(), 2)

	require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))

	// Ensure we can still read back the tokens file.
	actualTokens, err := LoadTokensFromFile(tokensFile.Name())
	require.NoError(t, err)
	assert.Equal(t, storedTokens, actualTokens)
}

func TestTokensPersistencyDelegate_ShouldHandleTheCaseTheInstanceIsAlreadyInTheRing(t *testing.T) {
	storedTokens := Tokens{6, 7, 8, 9, 10}
	differentTokens := Tokens{1, 2, 3, 4, 5}

	tests := map[string]struct {
		storedTokens   Tokens
		initialState   InstanceState
		initialTokens  Tokens
		expectedState  InstanceState
		expectedTokens Tokens
	}{
		"instance already registered in the ring without tokens": {
			initialState:   PENDING,
			initialTokens:  nil,
			expectedState:  ACTIVE,
			expectedTokens: storedTokens,
		},
		"instance already registered in the ring with tokens": {
			initialState:   JOINING,
			initialTokens:  differentTokens,
			expectedState:  JOINING,
			expectedTokens: differentTokens,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			tokensFile, err := os.CreateTemp("", "tokens-*")
			require.NoError(t, err)
			defer os.Remove(tokensFile.Name()) //nolint:errcheck

			// Store some tokens to the file.
			require.NoError(t, storedTokens.StoreToFile(tokensFile.Name()))

			// We assume is already registered to the ring.
			registeredAt := time.Now().Add(-time.Hour)

			testDelegate := &mockDelegate{
				onRegister: func(lifecycler *BasicLifecycler, ringDesc Desc, instanceExists bool, instanceID string, instanceDesc InstanceDesc) (InstanceState, Tokens) {
					return instanceDesc.GetState(), instanceDesc.GetTokens()
				},
			}

			persistencyDelegate := NewTokensPersistencyDelegate(tokensFile.Name(), ACTIVE, testDelegate, log.NewNopLogger())

			ctx := context.Background()
			cfg := prepareBasicLifecyclerConfig()
			lifecycler, store, err := prepareBasicLifecyclerWithDelegate(t, cfg, persistencyDelegate)
			require.NoError(t, err)
			defer services.StopAndAwaitTerminated(ctx, lifecycler) //nolint:errcheck

			// Add the instance to the ring.
			require.NoError(t, store.CAS(ctx, testRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
				ringDesc := NewDesc()
				ringDesc.AddIngester(cfg.ID, cfg.Addr, cfg.Zone, testData.initialTokens, testData.initialState, registeredAt)
				return ringDesc, true, nil
			}))

			require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
			assert.Equal(t, testData.expectedState, lifecycler.GetState())
			assert.Equal(t, testData.expectedTokens, lifecycler.GetTokens())
			assert.True(t, lifecycler.IsRegistered())
			assert.Equal(t, registeredAt.Unix(), lifecycler.GetRegisteredAt().Unix())
		})
	}
}

// TestDelegatesChain tests chaining all provided delegates together.
func TestDelegatesChain(t *testing.T) {
	onStoppingCalled := false

	// Create a temporary file and immediately delete it.
	tokensFile, err := os.CreateTemp("", "tokens-*")
	require.NoError(t, err)
	require.NoError(t, os.Remove(tokensFile.Name()))

	// Chain delegates together.
	var chain BasicLifecyclerDelegate
	chain = &mockDelegate{
		onRegister: func(lifecycler *BasicLifecycler, ringDesc Desc, instanceExists bool, instanceID string, instanceDesc InstanceDesc) (InstanceState, Tokens) {
			assert.False(t, instanceExists)
			return JOINING, Tokens{1, 2, 3, 4, 5}
		},
		onStopping: func(l *BasicLifecycler) {
			assert.Equal(t, LEAVING, l.GetState())
			onStoppingCalled = true
		},
	}

	chain = NewTokensPersistencyDelegate(tokensFile.Name(), ACTIVE, chain, log.NewNopLogger())
	chain = NewLeaveOnStoppingDelegate(chain, log.NewNopLogger())
	chain = NewAutoForgetDelegate(time.Minute, chain, log.NewNopLogger())

	ctx := context.Background()
	cfg := prepareBasicLifecyclerConfig()
	lifecycler, _, err := prepareBasicLifecyclerWithDelegate(t, cfg, chain)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, lifecycler) //nolint:errcheck

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
	assert.Equal(t, JOINING, lifecycler.GetState())
	assert.Equal(t, Tokens{1, 2, 3, 4, 5}, lifecycler.GetTokens())
	assert.True(t, lifecycler.IsRegistered())

	require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))
	assert.True(t, onStoppingCalled)

	// Ensure tokens have been stored.
	actualTokens, err := LoadTokensFromFile(tokensFile.Name())
	require.NoError(t, err)
	assert.Equal(t, Tokens{1, 2, 3, 4, 5}, actualTokens)
}

func TestAutoForgetDelegate(t *testing.T) {
	const forgetPeriod = time.Minute
	registeredAt := time.Now()

	tests := map[string]struct {
		setup             func(ringDesc *Desc)
		expectedInstances []string
	}{
		"no unhealthy instance in the ring": {
			setup: func(ringDesc *Desc) {
				ringDesc.AddIngester("instance-1", "1.1.1.1", "", nil, ACTIVE, registeredAt)
			},
			expectedInstances: []string{testInstanceID, "instance-1"},
		},
		"unhealthy instance in the ring that has NOTreached the forget period yet": {
			setup: func(ringDesc *Desc) {
				i := ringDesc.AddIngester("instance-1", "1.1.1.1", "", nil, ACTIVE, registeredAt)
				i.Timestamp = time.Now().Add(-forgetPeriod).Add(5 * time.Second).Unix()
				ringDesc.Ingesters["instance-1"] = i
			},
			expectedInstances: []string{testInstanceID, "instance-1"},
		},
		"unhealthy instance in the ring that has reached the forget period": {
			setup: func(ringDesc *Desc) {
				i := ringDesc.AddIngester("instance-1", "1.1.1.1", "", nil, ACTIVE, registeredAt)
				i.Timestamp = time.Now().Add(-forgetPeriod).Add(-5 * time.Second).Unix()
				ringDesc.Ingesters["instance-1"] = i
			},
			expectedInstances: []string{testInstanceID},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			cfg := prepareBasicLifecyclerConfig()
			cfg.HeartbeatPeriod = 100 * time.Millisecond

			testDelegate := &mockDelegate{}

			autoForgetDelegate := NewAutoForgetDelegate(forgetPeriod, testDelegate, log.NewNopLogger())
			lifecycler, store, err := prepareBasicLifecyclerWithDelegate(t, cfg, autoForgetDelegate)
			require.NoError(t, err)

			// Setup the initial state of the ring.
			require.NoError(t, store.CAS(ctx, testRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
				ringDesc := NewDesc()
				testData.setup(ringDesc)
				return ringDesc, true, nil
			}))

			// Start the lifecycler.
			require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
			defer services.StopAndAwaitTerminated(ctx, lifecycler) //nolint:errcheck

			// Wait until an heartbeat has been sent.
			test.Poll(t, time.Second, true, func() interface{} {
				return testutil.ToFloat64(lifecycler.metrics.heartbeats) > 0
			})

			// Read back the ring status from the store.
			v, err := store.Get(ctx, testRingKey)
			require.NoError(t, err)
			require.NotNil(t, v)

			var actualInstances []string
			for id := range GetOrCreateRingDesc(v).GetIngesters() {
				actualInstances = append(actualInstances, id)
			}

			assert.ElementsMatch(t, testData.expectedInstances, actualInstances)
		})
	}
}
