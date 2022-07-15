// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package ring

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/internal/cortex/ring/kv/consul"
	"github.com/thanos-io/thanos/internal/cortex/util/flagext"
	"github.com/thanos-io/thanos/internal/cortex/util/services"
	"github.com/thanos-io/thanos/internal/cortex/util/test"
)

const (
	// ring key used for testware
	ringKey = "ring"
)

func testLifecyclerConfig(ringConfig Config, id string) LifecyclerConfig {
	var lifecyclerConfig LifecyclerConfig
	flagext.DefaultValues(&lifecyclerConfig)
	lifecyclerConfig.Addr = "0.0.0.0"
	lifecyclerConfig.Port = 1
	lifecyclerConfig.ListenPort = 0
	lifecyclerConfig.RingConfig = ringConfig
	lifecyclerConfig.NumTokens = 1
	lifecyclerConfig.ID = id
	lifecyclerConfig.Zone = "zone1"
	lifecyclerConfig.FinalSleep = 0
	lifecyclerConfig.HeartbeatPeriod = 100 * time.Millisecond

	return lifecyclerConfig
}

func checkNormalised(d interface{}, id string) bool {
	desc, ok := d.(*Desc)
	return ok &&
		len(desc.Ingesters) == 1 &&
		desc.Ingesters[id].State == ACTIVE &&
		len(desc.Ingesters[id].Tokens) == 1
}

func TestLifecycler_HealthyInstancesCount(t *testing.T) {
	ringStore, closer := consul.NewInMemoryClient(GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = ringStore

	ctx := context.Background()

	// Add the first ingester to the ring
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig1.HeartbeatPeriod = 100 * time.Millisecond
	lifecyclerConfig1.JoinAfter = 100 * time.Millisecond

	lifecycler1, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{}, "ingester", ringKey, true, log.NewNopLogger(), nil)
	require.NoError(t, err)
	assert.Equal(t, 0, lifecycler1.HealthyInstancesCount())

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler1))
	defer services.StopAndAwaitTerminated(ctx, lifecycler1) // nolint:errcheck

	// Assert the first ingester joined the ring
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler1.HealthyInstancesCount() == 1
	})

	// Add the second ingester to the ring
	lifecyclerConfig2 := testLifecyclerConfig(ringConfig, "ing2")
	lifecyclerConfig2.HeartbeatPeriod = 100 * time.Millisecond
	lifecyclerConfig2.JoinAfter = 100 * time.Millisecond

	lifecycler2, err := NewLifecycler(lifecyclerConfig2, &nopFlushTransferer{}, "ingester", ringKey, true, log.NewNopLogger(), nil)
	require.NoError(t, err)
	assert.Equal(t, 0, lifecycler2.HealthyInstancesCount())

	require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler2))
	defer services.StopAndAwaitTerminated(ctx, lifecycler2) // nolint:errcheck

	// Assert the second ingester joined the ring
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler2.HealthyInstancesCount() == 2
	})

	// Assert the first ingester count is updated
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		return lifecycler1.HealthyInstancesCount() == 2
	})
}

func TestLifecycler_ZonesCount(t *testing.T) {
	ringStore, closer := consul.NewInMemoryClient(GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = ringStore

	events := []struct {
		zone          string
		expectedZones int
	}{
		{"zone-a", 1},
		{"zone-b", 2},
		{"zone-a", 2},
		{"zone-c", 3},
	}

	for idx, event := range events {
		ctx := context.Background()

		// Register an ingester to the ring.
		cfg := testLifecyclerConfig(ringConfig, fmt.Sprintf("instance-%d", idx))
		cfg.HeartbeatPeriod = 100 * time.Millisecond
		cfg.JoinAfter = 100 * time.Millisecond
		cfg.Zone = event.zone

		lifecycler, err := NewLifecycler(cfg, &nopFlushTransferer{}, "ingester", ringKey, true, log.NewNopLogger(), nil)
		require.NoError(t, err)
		assert.Equal(t, 0, lifecycler.ZonesCount())

		require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
		defer services.StopAndAwaitTerminated(ctx, lifecycler) // nolint:errcheck

		// Wait until joined.
		test.Poll(t, time.Second, idx+1, func() interface{} {
			return lifecycler.HealthyInstancesCount()
		})

		assert.Equal(t, event.expectedZones, lifecycler.ZonesCount())
	}
}

func TestLifecycler_NilFlushTransferer(t *testing.T) {
	ringStore, closer := consul.NewInMemoryClient(GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = ringStore
	lifecyclerConfig := testLifecyclerConfig(ringConfig, "ing1")

	// Create a lifecycler with nil FlushTransferer to make sure it operates correctly
	lifecycler, err := NewLifecycler(lifecyclerConfig, nil, "ingester", ringKey, true, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), lifecycler))

	// Ensure the lifecycler joined the ring
	test.Poll(t, time.Second, 1, func() interface{} {
		return lifecycler.HealthyInstancesCount()
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), lifecycler))

	assert.Equal(t, 0, lifecycler.HealthyInstancesCount())
}

func TestLifecycler_TwoRingsWithDifferentKeysOnTheSameKVStore(t *testing.T) {
	// Create a shared ring
	ringStore, closer := consul.NewInMemoryClient(GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = ringStore

	// Create two lifecyclers, each on a separate ring
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "instance-1")
	lifecyclerConfig2 := testLifecyclerConfig(ringConfig, "instance-2")

	lifecycler1, err := NewLifecycler(lifecyclerConfig1, nil, "service-1", "ring-1", true, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), lifecycler1))
	defer services.StopAndAwaitTerminated(context.Background(), lifecycler1) //nolint:errcheck

	lifecycler2, err := NewLifecycler(lifecyclerConfig2, nil, "service-2", "ring-2", true, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), lifecycler2))
	defer services.StopAndAwaitTerminated(context.Background(), lifecycler2) //nolint:errcheck

	// Ensure each lifecycler reports 1 healthy instance, because they're
	// in a different ring
	test.Poll(t, time.Second, 1, func() interface{} {
		return lifecycler1.HealthyInstancesCount()
	})

	test.Poll(t, time.Second, 1, func() interface{} {
		return lifecycler2.HealthyInstancesCount()
	})
}

type nopFlushTransferer struct{}

func (f *nopFlushTransferer) Flush() {}
func (f *nopFlushTransferer) TransferOut(_ context.Context) error {
	return nil
}

func TestLifecycler_ShouldHandleInstanceAbruptlyRestarted(t *testing.T) {
	ringStore, closer := consul.NewInMemoryClient(GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = ringStore

	r, err := New(ringConfig, "ingester", ringKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	// Add an 'ingester' with normalised tokens.
	lifecyclerConfig1 := testLifecyclerConfig(ringConfig, "ing1")
	l1, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{}, "ingester", ringKey, true, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l1))

	// Check this ingester joined, is active, and has one token.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ringKey)
		require.NoError(t, err)
		return checkNormalised(d, "ing1")
	})

	expectedTokens := l1.getTokens()
	expectedRegisteredAt := l1.getRegisteredAt()

	// Wait 1 second because the registered timestamp has second precision. Without waiting
	// we wouldn't have the guarantee the previous registered timestamp is preserved.
	time.Sleep(time.Second)

	// Add a second ingester with the same settings, so it will think it has restarted
	l2, err := NewLifecycler(lifecyclerConfig1, &nopFlushTransferer{}, "ingester", ringKey, true, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l2))

	// Check the new ingester picked up the same tokens and registered timestamp.
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ringKey)
		require.NoError(t, err)

		return checkNormalised(d, "ing1") &&
			expectedTokens.Equals(l2.getTokens()) &&
			expectedRegisteredAt.Unix() == l2.getRegisteredAt().Unix()
	})
}

type MockClient struct {
	ListFunc        func(ctx context.Context, prefix string) ([]string, error)
	GetFunc         func(ctx context.Context, key string) (interface{}, error)
	DeleteFunc      func(ctx context.Context, key string) error
	CASFunc         func(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error
	WatchKeyFunc    func(ctx context.Context, key string, f func(interface{}) bool)
	WatchPrefixFunc func(ctx context.Context, prefix string, f func(string, interface{}) bool)
}

func (m *MockClient) List(ctx context.Context, prefix string) ([]string, error) {
	if m.ListFunc != nil {
		return m.ListFunc(ctx, prefix)
	}

	return nil, nil
}

func (m *MockClient) Get(ctx context.Context, key string) (interface{}, error) {
	if m.GetFunc != nil {
		return m.GetFunc(ctx, key)
	}

	return nil, nil
}

func (m *MockClient) Delete(ctx context.Context, key string) error {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(ctx, key)
	}

	return nil
}

func (m *MockClient) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	if m.CASFunc != nil {
		return m.CASFunc(ctx, key, f)
	}

	return nil
}

func (m *MockClient) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	if m.WatchKeyFunc != nil {
		m.WatchKeyFunc(ctx, key, f)
	}
}

func (m *MockClient) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	if m.WatchPrefixFunc != nil {
		m.WatchPrefixFunc(ctx, prefix, f)
	}
}

// Ensure a check ready returns error when consul returns a nil key and the ingester already holds keys. This happens if the ring key gets deleted
func TestCheckReady_NoRingInKVStore(t *testing.T) {
	ctx := context.Background()

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = &MockClient{}

	r, err := New(ringConfig, "ingester", ringKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, r.StartAsync(ctx))
	// This is very atypical, but if we used AwaitRunning, that would fail, because of how quickly service terminates ...
	// by the time we check for Running state, it is already terminated, because mock ring has no WatchFunc, so it
	// will just exit.
	require.NoError(t, r.AwaitTerminated(ctx))

	cfg := testLifecyclerConfig(ringConfig, "ring1")
	cfg.MinReadyDuration = 1 * time.Nanosecond
	l1, err := NewLifecycler(cfg, &nopFlushTransferer{}, "ingester", ringKey, true, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, l1))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, l1))
	})

	l1.setTokens([]uint32{1})

	err = l1.CheckReady(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no ring returned from the KV store")
}

func TestCheckReady_MinReadyDuration(t *testing.T) {
	tests := map[string]struct {
		minReadyDuration time.Duration
		expectedMinDelay time.Duration
	}{
		"should immediately pass the check if the instance is ACTIVE and healthy and min ready duration is disabled": {
			minReadyDuration: 0,
			expectedMinDelay: 0,
		},
		"should wait min ready duration before passing the check after the instance is ACTIVE and healthy": {
			minReadyDuration: time.Second,
			expectedMinDelay: time.Second,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()

			ringStore, closer := consul.NewInMemoryClient(GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			var ringConfig Config
			flagext.DefaultValues(&ringConfig)
			ringConfig.KVStore.Mock = ringStore

			cfg := testLifecyclerConfig(ringConfig, "instance-1")
			cfg.ReadinessCheckRingHealth = false
			cfg.MinReadyDuration = testData.minReadyDuration

			l, err := NewLifecycler(cfg, &nopFlushTransferer{}, "ring", ringKey, true, log.NewNopLogger(), nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, l))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, l))
			})

			startTime := time.Now()

			// Wait until the instance is ACTIVE and healthy in the ring.
			waitRingInstance(t, 3*time.Second, l, func(instance InstanceDesc) error {
				return instance.IsReady(time.Now(), cfg.RingConfig.HeartbeatTimeout)
			})

			if testData.expectedMinDelay == 0 {
				// We expect it to be immediately ready.
				assert.NoError(t, l.CheckReady(ctx))
			} else {
				// Poll the readiness check until ready and measure how much time it takes.
				test.Poll(t, 3*time.Second, nil, func() interface{} {
					return l.CheckReady(ctx)
				})

				assert.GreaterOrEqual(t, time.Since(startTime), testData.expectedMinDelay)
			}
		})
	}
}

func TestCheckReady_CheckRingHealth(t *testing.T) {
	tests := map[string]struct {
		checkRingHealthEnabled bool
		firstJoinAfter         time.Duration
		secondJoinAfter        time.Duration
		expectedFirstMinReady  time.Duration
		expectedFirstMaxReady  time.Duration
	}{
		"should wait until the self instance is ACTIVE and healthy in the ring when 'check ring health' is disabled": {
			checkRingHealthEnabled: false,
			firstJoinAfter:         time.Second,
			secondJoinAfter:        3 * time.Second,
			expectedFirstMinReady:  time.Second,
			expectedFirstMaxReady:  2 * time.Second,
		},
		"should wait until all instances are ACTIVE and healthy in the ring when 'check ring health' is enabled": {
			checkRingHealthEnabled: true,
			firstJoinAfter:         time.Second,
			secondJoinAfter:        3 * time.Second,
			expectedFirstMinReady:  3 * time.Second,
			expectedFirstMaxReady:  4 * time.Second,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()

			ringStore, closer := consul.NewInMemoryClient(GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			var ringConfig Config
			flagext.DefaultValues(&ringConfig)
			ringConfig.KVStore.Mock = ringStore

			// Create lifecycler #1.
			cfg := testLifecyclerConfig(ringConfig, "instance-1")
			cfg.ReadinessCheckRingHealth = testData.checkRingHealthEnabled
			cfg.MinReadyDuration = 0
			cfg.JoinAfter = testData.firstJoinAfter

			l1, err := NewLifecycler(cfg, &nopFlushTransferer{}, "ring", ringKey, true, log.NewNopLogger(), nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, l1))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, l1))
			})

			// Create lifecycler #2.
			cfg = testLifecyclerConfig(ringConfig, "instance-2")
			cfg.ReadinessCheckRingHealth = testData.checkRingHealthEnabled
			cfg.MinReadyDuration = 0
			cfg.JoinAfter = testData.secondJoinAfter

			l2, err := NewLifecycler(cfg, &nopFlushTransferer{}, "ring", ringKey, true, log.NewNopLogger(), nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, l2))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, l2))
			})

			startTime := time.Now()

			// Wait until both instances are registered in the ring. We expect them to be registered
			// immediately and then switch to ACTIVE after the configured auto join delay.
			waitRingInstance(t, 3*time.Second, l1, func(instance InstanceDesc) error { return nil })
			waitRingInstance(t, 3*time.Second, l2, func(instance InstanceDesc) error { return nil })

			// Poll the readiness check until ready and measure how much time it takes.
			test.Poll(t, 5*time.Second, nil, func() interface{} {
				return l1.CheckReady(ctx)
			})

			assert.GreaterOrEqual(t, time.Since(startTime), testData.expectedFirstMinReady)
			assert.LessOrEqual(t, time.Since(startTime), testData.expectedFirstMaxReady)
		})
	}
}

type noopFlushTransferer struct {
}

func (f *noopFlushTransferer) Flush()                                {}
func (f *noopFlushTransferer) TransferOut(ctx context.Context) error { return nil }

func TestRestartIngester_DisabledHeartbeat_unregister_on_shutdown_false(t *testing.T) {
	ringStore, closer := consul.NewInMemoryClient(GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = ringStore

	r, err := New(ringConfig, "ingester", ringKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))

	// poll function waits for a condition and returning actual state of the ingesters after the condition succeed.
	poll := func(condition func(*Desc) bool) map[string]InstanceDesc {
		var ingesters map[string]InstanceDesc
		test.Poll(t, 5*time.Second, true, func() interface{} {
			d, err := r.KVClient.Get(context.Background(), ringKey)
			require.NoError(t, err)

			desc, ok := d.(*Desc)

			if ok {
				ingesters = desc.Ingesters
			}
			return ok && condition(desc)
		})

		return ingesters
	}

	// Starts Ingester and wait it to became active
	startIngesterAndWaitActive := func(ingId string) *Lifecycler {
		lifecyclerConfig := testLifecyclerConfig(ringConfig, ingId)
		// Disabling heartBeat and unregister_on_shutdown
		lifecyclerConfig.UnregisterOnShutdown = false
		lifecyclerConfig.HeartbeatPeriod = 0
		lifecycler, err := NewLifecycler(lifecyclerConfig, &noopFlushTransferer{}, "lifecycler", ringKey, true, log.NewNopLogger(), nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), lifecycler))
		poll(func(desc *Desc) bool {
			return desc.Ingesters[ingId].State == ACTIVE
		})
		return lifecycler
	}

	// We are going to create 2 fake ingester with disabled heart beat and `unregister_on_shutdown=false` then
	// test if the ingester 2 became active after:
	// * Clean Shutdown (LEAVING after shutdown)
	// * Crashes while in the PENDING or JOINING state
	l1 := startIngesterAndWaitActive("ing1")
	defer services.StopAndAwaitTerminated(context.Background(), l1) //nolint:errcheck

	l2 := startIngesterAndWaitActive("ing2")

	ingesters := poll(func(desc *Desc) bool {
		return len(desc.Ingesters) == 2 && desc.Ingesters["ing1"].State == ACTIVE && desc.Ingesters["ing2"].State == ACTIVE
	})

	// Both Ingester should be active and running
	assert.Equal(t, ACTIVE, ingesters["ing1"].State)
	assert.Equal(t, ACTIVE, ingesters["ing2"].State)

	// Stop One ingester gracefully should leave it on LEAVING STATE on the ring
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), l2))

	ingesters = poll(func(desc *Desc) bool {
		return len(desc.Ingesters) == 2 && desc.Ingesters["ing2"].State == LEAVING
	})
	assert.Equal(t, LEAVING, ingesters["ing2"].State)

	// Start Ingester2 again - Should flip back to ACTIVE in the ring
	l2 = startIngesterAndWaitActive("ing2")
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), l2))

	// Simulate ingester2 crash on startup and left the ring with JOINING state
	err = r.KVClient.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc, ok := in.(*Desc)
		require.Equal(t, true, ok)
		ingester2Desc := desc.Ingesters["ing2"]
		ingester2Desc.State = JOINING
		desc.Ingesters["ing2"] = ingester2Desc
		return desc, true, nil
	})
	require.NoError(t, err)

	l2 = startIngesterAndWaitActive("ing2")
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), l2))

	// Simulate ingester2 crash on startup and left the ring with PENDING state
	err = r.KVClient.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc, ok := in.(*Desc)
		require.Equal(t, true, ok)
		ingester2Desc := desc.Ingesters["ing2"]
		ingester2Desc.State = PENDING
		desc.Ingesters["ing2"] = ingester2Desc
		return desc, true, nil
	})
	require.NoError(t, err)

	l2 = startIngesterAndWaitActive("ing2")
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), l2))
}

func TestTokensOnDisk(t *testing.T) {
	ringStore, closer := consul.NewInMemoryClient(GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = ringStore

	r, err := New(ringConfig, "ingester", ringKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	tokenDir := t.TempDir()

	lifecyclerConfig := testLifecyclerConfig(ringConfig, "ing1")
	lifecyclerConfig.NumTokens = 512
	lifecyclerConfig.TokensFilePath = tokenDir + "/tokens"

	// Start first ingester.
	l1, err := NewLifecycler(lifecyclerConfig, &noopFlushTransferer{}, "ingester", ringKey, true, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l1))

	// Check this ingester joined, is active, and has 512 token.
	var expTokens []uint32
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ringKey)
		require.NoError(t, err)

		desc, ok := d.(*Desc)
		if ok {
			expTokens = desc.Ingesters["ing1"].Tokens
		}
		return ok &&
			len(desc.Ingesters) == 1 &&
			desc.Ingesters["ing1"].State == ACTIVE &&
			len(desc.Ingesters["ing1"].Tokens) == 512
	})

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), l1))

	// Start new ingester at same token directory.
	lifecyclerConfig.ID = "ing2"
	l2, err := NewLifecycler(lifecyclerConfig, &noopFlushTransferer{}, "ingester", ringKey, true, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l2))
	defer services.StopAndAwaitTerminated(context.Background(), l2) //nolint:errcheck

	// Check this ingester joined, is active, and has 512 token.
	var actTokens []uint32
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ringKey)
		require.NoError(t, err)
		desc, ok := d.(*Desc)
		if ok {
			actTokens = desc.Ingesters["ing2"].Tokens
		}
		return ok &&
			len(desc.Ingesters) == 1 &&
			desc.Ingesters["ing2"].State == ACTIVE &&
			len(desc.Ingesters["ing2"].Tokens) == 512
	})

	// Check for same tokens.
	sort.Slice(expTokens, func(i, j int) bool { return expTokens[i] < expTokens[j] })
	sort.Slice(actTokens, func(i, j int) bool { return actTokens[i] < actTokens[j] })
	for i := 0; i < 512; i++ {
		require.Equal(t, expTokens, actTokens)
	}
}

// JoinInLeavingState ensures that if the lifecycler starts up and the ring already has it in a LEAVING state that it still is able to auto join
func TestJoinInLeavingState(t *testing.T) {
	ringStore, closer := consul.NewInMemoryClient(GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = ringStore

	r, err := New(ringConfig, "ingester", ringKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	cfg := testLifecyclerConfig(ringConfig, "ing1")
	cfg.NumTokens = 2
	cfg.MinReadyDuration = 1 * time.Nanosecond

	// Set state as LEAVING
	err = r.KVClient.CAS(context.Background(), ringKey, func(in interface{}) (interface{}, bool, error) {
		r := &Desc{
			Ingesters: map[string]InstanceDesc{
				"ing1": {
					State:  LEAVING,
					Tokens: []uint32{1, 4},
				},
				"ing2": {
					Tokens: []uint32{2, 3},
				},
			},
		}

		return r, true, nil
	})
	require.NoError(t, err)

	l1, err := NewLifecycler(cfg, &nopFlushTransferer{}, "ingester", ringKey, true, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l1))

	// Check that the lifecycler was able to join after coming up in LEAVING
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ringKey)
		require.NoError(t, err)

		desc, ok := d.(*Desc)
		return ok &&
			len(desc.Ingesters) == 2 &&
			desc.Ingesters["ing1"].State == ACTIVE &&
			len(desc.Ingesters["ing1"].Tokens) == cfg.NumTokens &&
			len(desc.Ingesters["ing2"].Tokens) == 2
	})
}

// JoinInJoiningState ensures that if the lifecycler starts up and the ring already has it in a JOINING state that it still is able to auto join
func TestJoinInJoiningState(t *testing.T) {
	ringStore, closer := consul.NewInMemoryClient(GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = ringStore

	r, err := New(ringConfig, "ingester", ringKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	cfg := testLifecyclerConfig(ringConfig, "ing1")
	cfg.NumTokens = 2
	cfg.MinReadyDuration = 1 * time.Nanosecond
	instance1RegisteredAt := time.Now().Add(-1 * time.Hour)
	instance2RegisteredAt := time.Now().Add(-2 * time.Hour)

	// Set state as JOINING
	err = r.KVClient.CAS(context.Background(), ringKey, func(in interface{}) (interface{}, bool, error) {
		r := &Desc{
			Ingesters: map[string]InstanceDesc{
				"ing1": {
					State:               JOINING,
					Tokens:              []uint32{1, 4},
					RegisteredTimestamp: instance1RegisteredAt.Unix(),
				},
				"ing2": {
					Tokens:              []uint32{2, 3},
					RegisteredTimestamp: instance2RegisteredAt.Unix(),
				},
			},
		}

		return r, true, nil
	})
	require.NoError(t, err)

	l1, err := NewLifecycler(cfg, &nopFlushTransferer{}, "ingester", ringKey, true, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l1))

	// Check that the lifecycler was able to join after coming up in JOINING
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ringKey)
		require.NoError(t, err)

		desc, ok := d.(*Desc)
		return ok &&
			len(desc.Ingesters) == 2 &&
			desc.Ingesters["ing1"].State == ACTIVE &&
			len(desc.Ingesters["ing1"].Tokens) == cfg.NumTokens &&
			len(desc.Ingesters["ing2"].Tokens) == 2 &&
			desc.Ingesters["ing1"].RegisteredTimestamp == instance1RegisteredAt.Unix() &&
			desc.Ingesters["ing2"].RegisteredTimestamp == instance2RegisteredAt.Unix()
	})
}

func TestRestoreOfZoneWhenOverwritten(t *testing.T) {
	// This test is simulating a case during upgrade of pre 1.0 cortex where
	// older ingesters do not have the zone field in their ring structs
	// so it gets removed. The current version of the lifecylcer should
	// write it back on update during its next heartbeat.

	ringStore, closer := consul.NewInMemoryClient(GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	var ringConfig Config
	flagext.DefaultValues(&ringConfig)
	ringConfig.KVStore.Mock = ringStore

	r, err := New(ringConfig, "ingester", ringKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	cfg := testLifecyclerConfig(ringConfig, "ing1")

	// Set ing1 to not have a zone
	err = r.KVClient.CAS(context.Background(), ringKey, func(in interface{}) (interface{}, bool, error) {
		r := &Desc{
			Ingesters: map[string]InstanceDesc{
				"ing1": {
					State:  ACTIVE,
					Addr:   "0.0.0.0",
					Tokens: []uint32{1, 4},
				},
				"ing2": {
					Tokens: []uint32{2, 3},
				},
			},
		}

		return r, true, nil
	})
	require.NoError(t, err)

	l1, err := NewLifecycler(cfg, &nopFlushTransferer{}, "ingester", ringKey, true, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), l1))

	// Check that the lifecycler was able to reset the zone value to the expected setting
	test.Poll(t, 1000*time.Millisecond, true, func() interface{} {
		d, err := r.KVClient.Get(context.Background(), ringKey)
		require.NoError(t, err)
		desc, ok := d.(*Desc)
		return ok &&
			len(desc.Ingesters) == 2 &&
			desc.Ingesters["ing1"].Zone == l1.Zone &&
			desc.Ingesters["ing2"].Zone == ""

	})
}

func waitRingInstance(t *testing.T, timeout time.Duration, l *Lifecycler, check func(instance InstanceDesc) error) {
	test.Poll(t, timeout, nil, func() interface{} {
		desc, err := l.KVStore.Get(context.Background(), l.RingKey)
		if err != nil {
			return err
		}

		ringDesc, ok := desc.(*Desc)
		if !ok || ringDesc == nil {
			return errors.New("empty ring")
		}

		instance, ok := ringDesc.Ingesters[l.ID]
		if !ok {
			return errors.New("no instance in the ring")
		}

		return check(instance)
	})
}
