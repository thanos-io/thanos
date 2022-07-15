// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package querier

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/internal/cortex/ring"
	"github.com/thanos-io/thanos/internal/cortex/ring/kv/consul"
	cortex_tsdb "github.com/thanos-io/thanos/internal/cortex/storage/tsdb"
	"github.com/thanos-io/thanos/internal/cortex/util"
	"github.com/thanos-io/thanos/internal/cortex/util/flagext"
	"github.com/thanos-io/thanos/internal/cortex/util/services"
	"github.com/thanos-io/thanos/internal/cortex/util/test"
)

func TestBlocksStoreReplicationSet_GetClientsFor(t *testing.T) {
	// The following block IDs have been picked to have increasing hash values
	// in order to simplify the tests.
	block1 := ulid.MustNew(1, nil) // hash: 283204220
	block2 := ulid.MustNew(2, nil) // hash: 444110359
	block3 := ulid.MustNew(5, nil) // hash: 2931974232
	block4 := ulid.MustNew(6, nil) // hash: 3092880371

	block1Hash := cortex_tsdb.HashBlockID(block1)
	block2Hash := cortex_tsdb.HashBlockID(block2)
	block3Hash := cortex_tsdb.HashBlockID(block3)
	block4Hash := cortex_tsdb.HashBlockID(block4)

	userID := "user-A"
	registeredAt := time.Now()

	tests := map[string]struct {
		shardingStrategy  string
		tenantShardSize   int
		replicationFactor int
		setup             func(*ring.Desc)
		queryBlocks       []ulid.ULID
		exclude           map[ulid.ULID][]string
		expectedClients   map[string][]ulid.ULID
		expectedErr       error
	}{
		//
		// Sharding strategy: default
		//
		"default sharding, single instance in the ring with RF = 1": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"default sharding, single instance in the ring with RF = 1 but excluded": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", block1.String()),
		},
		"default sharding, single instance in the ring with RF = 1 but excluded for non queried block": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block3: {"127.0.0.1"},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"default sharding, single instance in the ring with RF = 2": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"default sharding, multiple instances in the ring with each requested block belonging to a different store-gateway and RF = 1": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block3, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1},
				"127.0.0.3": {block3},
				"127.0.0.4": {block4},
			},
		},
		"default sharding, multiple instances in the ring with each requested block belonging to a different store-gateway and RF = 1 but excluded": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block3, block4},
			exclude: map[ulid.ULID][]string{
				block3: {"127.0.0.3"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", block3.String()),
		},
		"default sharding, multiple instances in the ring with each requested block belonging to a different store-gateway and RF = 2": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block3, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1},
				"127.0.0.3": {block3},
				"127.0.0.4": {block4},
			},
		},
		"default sharding, multiple instances in the ring with multiple requested blocks belonging to the same store-gateway and RF = 2": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2, block3, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block4},
				"127.0.0.2": {block2, block3},
			},
		},
		"default sharding, multiple instances in the ring with each requested block belonging to a different store-gateway and RF = 2 and some blocks excluded but with replacement available": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block3, block4},
			exclude: map[ulid.ULID][]string{
				block3: {"127.0.0.3"},
				block1: {"127.0.0.1"},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.2": {block1},
				"127.0.0.4": {block3, block4},
			},
		},
		"default sharding, multiple instances in the ring are JOINING, the requested block + its replicas only belongs to JOINING instances": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.JOINING, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.JOINING, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.JOINING, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.4": {block1},
			},
		},
		//
		// Sharding strategy: shuffle sharding
		//
		"shuffle sharding, single instance in the ring with RF = 1, SS = 1": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   1,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"shuffle sharding, single instance in the ring with RF = 1, SS = 1 but excluded": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   1,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", block1.String()),
		},
		"shuffle sharding, single instance in the ring with RF = 2, SS = 2": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   2,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 1, SS = 1": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   1,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2, block4},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 1, SS = 2": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   2,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block4},
				"127.0.0.3": {block2},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 1, SS = 4": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   4,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1},
				"127.0.0.2": {block2},
				"127.0.0.4": {block4},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 2, SS = 2 with excluded blocks but some replacement available": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   2,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1"},
				block2: {"127.0.0.1"},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.3": {block1, block2},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 2, SS = 2 with excluded blocks and no replacement available": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   2,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1", "127.0.0.3"},
				block2: {"127.0.0.1"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", block1.String()),
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			// Setup the ring state.
			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			require.NoError(t, ringStore.CAS(ctx, "test", func(in interface{}) (interface{}, bool, error) {
				d := ring.NewDesc()
				testData.setup(d)
				return d, true, nil
			}))

			ringCfg := ring.Config{}
			flagext.DefaultValues(&ringCfg)
			ringCfg.ReplicationFactor = testData.replicationFactor

			r, err := ring.NewWithStoreClientAndStrategy(ringCfg, "test", "test", ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, nil)
			require.NoError(t, err)

			limits := &blocksStoreLimitsMock{
				storeGatewayTenantShardSize: testData.tenantShardSize,
			}

			reg := prometheus.NewPedanticRegistry()
			s, err := newBlocksStoreReplicationSet(r, testData.shardingStrategy, noLoadBalancing, limits, ClientConfig{}, log.NewNopLogger(), reg)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, s))
			defer services.StopAndAwaitTerminated(ctx, s) //nolint:errcheck

			// Wait until the ring client has initialised the state.
			test.Poll(t, time.Second, true, func() interface{} {
				all, err := r.GetAllHealthy(ring.Read)
				return err == nil && len(all.Instances) > 0
			})

			clients, err := s.GetClientsFor(userID, testData.queryBlocks, testData.exclude)
			assert.Equal(t, testData.expectedErr, err)

			if testData.expectedErr == nil {
				assert.Equal(t, testData.expectedClients, getStoreGatewayClientAddrs(clients))

				assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_storegateway_clients The current number of store-gateway clients in the pool.
					# TYPE cortex_storegateway_clients gauge
					cortex_storegateway_clients{client="querier"} %d
				`, len(testData.expectedClients))), "cortex_storegateway_clients"))
			}
		})
	}
}

func TestBlocksStoreReplicationSet_GetClientsFor_ShouldSupportRandomLoadBalancingStrategy(t *testing.T) {
	const (
		numRuns      = 1000
		numInstances = 3
	)

	ctx := context.Background()
	userID := "user-A"
	registeredAt := time.Now()
	block1 := ulid.MustNew(1, nil)

	// Create a ring.
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	require.NoError(t, ringStore.CAS(ctx, "test", func(in interface{}) (interface{}, bool, error) {
		d := ring.NewDesc()
		for n := 1; n <= numInstances; n++ {
			d.AddIngester(fmt.Sprintf("instance-%d", n), fmt.Sprintf("127.0.0.%d", n), "", []uint32{uint32(n)}, ring.ACTIVE, registeredAt)
		}
		return d, true, nil
	}))

	// Configure a replication factor equal to the number of instances, so that every store-gateway gets all blocks.
	ringCfg := ring.Config{}
	flagext.DefaultValues(&ringCfg)
	ringCfg.ReplicationFactor = numInstances

	r, err := ring.NewWithStoreClientAndStrategy(ringCfg, "test", "test", ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, nil)
	require.NoError(t, err)

	limits := &blocksStoreLimitsMock{}
	reg := prometheus.NewPedanticRegistry()
	s, err := newBlocksStoreReplicationSet(r, util.ShardingStrategyDefault, randomLoadBalancing, limits, ClientConfig{}, log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, s))
	defer services.StopAndAwaitTerminated(ctx, s) //nolint:errcheck

	// Wait until the ring client has initialised the state.
	test.Poll(t, time.Second, true, func() interface{} {
		all, err := r.GetAllHealthy(ring.Read)
		return err == nil && len(all.Instances) > 0
	})

	// Request the same block multiple times and ensure the distribution of
	// requests across store-gateways is balanced.
	distribution := map[string]int{}

	for n := 0; n < numRuns; n++ {
		clients, err := s.GetClientsFor(userID, []ulid.ULID{block1}, nil)
		require.NoError(t, err)
		require.Len(t, clients, 1)

		for addr := range getStoreGatewayClientAddrs(clients) {
			distribution[addr]++
		}
	}

	assert.Len(t, distribution, numInstances)
	for addr, count := range distribution {
		// Ensure that the number of times each client is returned is above
		// the 80% of the perfect even distribution.
		assert.Greaterf(t, float64(count), (float64(numRuns)/float64(numInstances))*0.8, "store-gateway address: %s", addr)
	}
}

func getStoreGatewayClientAddrs(clients map[BlocksStoreClient][]ulid.ULID) map[string][]ulid.ULID {
	addrs := map[string][]ulid.ULID{}
	for c, blockIDs := range clients {
		addrs[c.RemoteAddress()] = blockIDs
	}
	return addrs
}
