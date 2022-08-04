// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package querier

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/internal/cortex/util/services"
)

func TestBlocksStoreBalancedSet_GetClientsFor(t *testing.T) {
	const numGets = 1000
	serviceAddrs := []string{"127.0.0.1", "127.0.0.2"}
	block1 := ulid.MustNew(1, nil)

	ctx := context.Background()
	reg := prometheus.NewPedanticRegistry()
	s := newBlocksStoreBalancedSet(serviceAddrs, ClientConfig{}, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, s))
	defer services.StopAndAwaitTerminated(ctx, s) //nolint:errcheck

	// Call the GetClientsFor() many times to measure the distribution
	// of returned clients (we expect an even distribution).
	clientsCount := map[string]int{}

	for i := 0; i < numGets; i++ {
		clients, err := s.GetClientsFor("", []ulid.ULID{block1}, map[ulid.ULID][]string{})
		require.NoError(t, err)
		require.Len(t, clients, 1)

		var clientAddr string
		for c := range clients {
			clientAddr = c.RemoteAddress()
		}

		clientsCount[clientAddr] = clientsCount[clientAddr] + 1
	}

	assert.Len(t, clientsCount, len(serviceAddrs))
	for addr, count := range clientsCount {
		// Ensure that the number of times each client is returned is above
		// the 80% of the perfect even distribution.
		assert.Greaterf(t, count, int((float64(numGets)/float64(len(serviceAddrs)))*0.8), "service address: %s", addr)
	}

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_storegateway_client_dns_failures_total The number of DNS lookup failures
		# TYPE cortex_storegateway_client_dns_failures_total counter
		cortex_storegateway_client_dns_failures_total 0
		# HELP cortex_storegateway_client_dns_lookups_total The number of DNS lookups resolutions attempts
		# TYPE cortex_storegateway_client_dns_lookups_total counter
		cortex_storegateway_client_dns_lookups_total 0
		# HELP cortex_storegateway_client_dns_provider_results The number of resolved endpoints for each configured address
		# TYPE cortex_storegateway_client_dns_provider_results gauge
		cortex_storegateway_client_dns_provider_results{addr="127.0.0.1"} 1
		cortex_storegateway_client_dns_provider_results{addr="127.0.0.2"} 1
		# HELP cortex_storegateway_clients The current number of store-gateway clients in the pool.
		# TYPE cortex_storegateway_clients gauge
		cortex_storegateway_clients{client="querier"} 2
	`)))
}

func TestBlocksStoreBalancedSet_GetClientsFor_Exclude(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	tests := map[string]struct {
		serviceAddrs    []string
		queryBlocks     []ulid.ULID
		exclude         map[ulid.ULID][]string
		expectedClients map[string][]ulid.ULID
		expectedErr     error
	}{
		"no exclude": {
			serviceAddrs: []string{"127.0.0.1"},
			queryBlocks:  []ulid.ULID{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"single instance available and excluded for a non-queried block": {
			serviceAddrs: []string{"127.0.0.1"},
			queryBlocks:  []ulid.ULID{block1},
			exclude: map[ulid.ULID][]string{
				block2: {"127.0.0.1"},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1},
			},
		},
		"single instance available and excluded for the queried block": {
			serviceAddrs: []string{"127.0.0.1"},
			queryBlocks:  []ulid.ULID{block1},
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after filtering out excluded instances for block %s", block1.String()),
		},
		"multiple instances available and one is excluded for the queried blocks": {
			serviceAddrs: []string{"127.0.0.1", "127.0.0.2"},
			queryBlocks:  []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1"},
				block2: {"127.0.0.2"},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block2},
				"127.0.0.2": {block1},
			},
		},
		"multiple instances available and all are excluded for the queried block": {
			serviceAddrs: []string{"127.0.0.1", "127.0.0.2"},
			queryBlocks:  []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1", "127.0.0.2"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after filtering out excluded instances for block %s", block1.String()),
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			s := newBlocksStoreBalancedSet(testData.serviceAddrs, ClientConfig{}, log.NewNopLogger(), nil)
			require.NoError(t, services.StartAndAwaitRunning(ctx, s))
			defer services.StopAndAwaitTerminated(ctx, s) //nolint:errcheck

			clients, err := s.GetClientsFor("", testData.queryBlocks, testData.exclude)
			assert.Equal(t, testData.expectedErr, err)

			if testData.expectedErr == nil {
				assert.Equal(t, testData.expectedClients, getStoreGatewayClientAddrs(clients))
			}
		})
	}
}
