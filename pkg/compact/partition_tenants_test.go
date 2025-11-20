// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/thanos-io/objstore"
)

func validateTenantAssignments(t *testing.T, numShards int, tenantWeights []TenantWeight, buckets map[int][]string, weights map[int]int) {
	t.Helper()

	// Verify all shards exist in the maps
	if len(buckets) != numShards {
		t.Errorf("expected %d buckets, got %d", numShards, len(buckets))
	}
	if len(weights) != numShards {
		t.Errorf("expected %d weight entries, got %d", numShards, len(weights))
	}

	totalAssigned := 0
	assignedTenants := make(map[string]int) // tenant -> shard mapping

	for i := 0; i < numShards; i++ {
		if _, ok := buckets[i]; !ok {
			t.Errorf("bucket %d not found in assignments", i)
			continue
		}
		totalAssigned += len(buckets[i])

		for _, tenant := range buckets[i] {
			if prevShard, exists := assignedTenants[tenant]; exists {
				t.Errorf("tenant %s assigned to multiple buckets (shard %d and %d)", tenant, prevShard, i)
			}
			assignedTenants[tenant] = i
		}
	}

	// Verify all tenants are assigned exactly once
	if totalAssigned != len(tenantWeights) {
		t.Errorf("expected %d tenants to be assigned, got %d", len(tenantWeights), totalAssigned)
	}

	for _, tw := range tenantWeights {
		if _, ok := assignedTenants[tw.TenantName]; !ok {
			t.Errorf("tenant %s was not assigned to any bucket", tw.TenantName)
		}
	}

	// Verify weights are calculated correctly
	for i := 0; i < numShards; i++ {
		calculatedWeight := 0
		for _, tenantName := range buckets[i] {
			for _, tw := range tenantWeights {
				if tw.TenantName == tenantName {
					calculatedWeight += tw.Weight
					break
				}
			}
		}
		if weights[i] != calculatedWeight {
			t.Errorf("bucket %d: expected weight %d, got %d", i, calculatedWeight, weights[i])
		}
	}
}

func TestComputeTenantAssignments(t *testing.T) {
	for _, tcase := range []struct {
		name          string
		numShards     int
		tenantWeights []TenantWeight
	}{
		{
			name:      "basic distribution",
			numShards: 3,
			tenantWeights: []TenantWeight{
				{TenantName: "tenant1", Weight: 10},
				{TenantName: "tenant2", Weight: 8},
				{TenantName: "tenant3", Weight: 5},
				{TenantName: "tenant4", Weight: 3},
			},
		},
		{
			name:      "single shard",
			numShards: 1,
			tenantWeights: []TenantWeight{
				{TenantName: "tenant1", Weight: 10},
				{TenantName: "tenant2", Weight: 20},
			},
		},
		{
			name:      "more shards than tenants",
			numShards: 5,
			tenantWeights: []TenantWeight{
				{TenantName: "tenant1", Weight: 10},
				{TenantName: "tenant2", Weight: 20},
			},
		},
		{
			name:          "empty tenant list",
			numShards:     3,
			tenantWeights: []TenantWeight{},
		},
		{
			name:      "equal weights",
			numShards: 2,
			tenantWeights: []TenantWeight{
				{TenantName: "tenant1", Weight: 10},
				{TenantName: "tenant2", Weight: 10},
				{TenantName: "tenant3", Weight: 10},
				{TenantName: "tenant4", Weight: 10},
			},
		},
		{
			name:      "single tenant",
			numShards: 3,
			tenantWeights: []TenantWeight{
				{TenantName: "only-tenant", Weight: 100},
			},
		},
		{
			name:      "alphabetical sorting with same weights",
			numShards: 2,
			tenantWeights: []TenantWeight{
				{TenantName: "zebra", Weight: 10},
				{TenantName: "alpha", Weight: 10},
				{TenantName: "beta", Weight: 10},
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			buckets, weights := computeTenantAssignments(tcase.numShards, tcase.tenantWeights)
			validateTenantAssignments(t, tcase.numShards, tcase.tenantWeights, buckets, weights)
		})
	}
}

func TestComputeTenantAssignments_DistributionFairness(t *testing.T) {
	for _, tcase := range []struct {
		name             string
		numShards        int
		tenantWeights    []TenantWeight
		maxWeightDelta   int    // Maximum allowed difference between heaviest and lightest shard
		expectSoleTenant bool   // Whether we expect a tenant to be alone in a shard
		soleTenant       string // Which tenant should be alone (if expectSoleTenant is true)
	}{
		{
			name:      "balanced distribution",
			numShards: 3,
			tenantWeights: []TenantWeight{
				{TenantName: "tenant1", Weight: 100},
				{TenantName: "tenant2", Weight: 90},
				{TenantName: "tenant3", Weight: 80},
				{TenantName: "tenant4", Weight: 70},
				{TenantName: "tenant5", Weight: 60},
				{TenantName: "tenant6", Weight: 50},
			},
			maxWeightDelta: 30,
		},
		{
			name:      "very imbalanced - one large tenant",
			numShards: 3,
			tenantWeights: []TenantWeight{
				{TenantName: "huge-tenant", Weight: 1000},
				{TenantName: "small-1", Weight: 10},
				{TenantName: "small-2", Weight: 10},
				{TenantName: "small-3", Weight: 10},
				{TenantName: "small-4", Weight: 10},
				{TenantName: "small-5", Weight: 10},
			},
			expectSoleTenant: true,
			soleTenant:       "huge-tenant",
		},
		{
			name:      "very imbalanced - multiple large and small tenants",
			numShards: 4,
			tenantWeights: []TenantWeight{
				{TenantName: "large-1", Weight: 500},
				{TenantName: "large-2", Weight: 480},
				{TenantName: "medium", Weight: 50},
				{TenantName: "small-1", Weight: 5},
				{TenantName: "small-2", Weight: 5},
				{TenantName: "small-3", Weight: 5},
				{TenantName: "small-4", Weight: 5},
			},
			expectSoleTenant: true,
			soleTenant:       "large-1",
		},
		{
			name:      "extreme imbalance - one tenant much larger than sum of others",
			numShards: 2,
			tenantWeights: []TenantWeight{
				{TenantName: "massive", Weight: 10000},
				{TenantName: "tiny-1", Weight: 1},
				{TenantName: "tiny-2", Weight: 1},
				{TenantName: "tiny-3", Weight: 1},
			},
			expectSoleTenant: true,
			soleTenant:       "massive",
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			buckets, weights := computeTenantAssignments(tcase.numShards, tcase.tenantWeights)

			validateTenantAssignments(t, tcase.numShards, tcase.tenantWeights, buckets, weights)

			minWeight := weights[0]
			maxWeight := weights[0]
			for i := 1; i < tcase.numShards; i++ {
				if weights[i] < minWeight {
					minWeight = weights[i]
				}
				if weights[i] > maxWeight {
					maxWeight = weights[i]
				}
			}

			weightDelta := maxWeight - minWeight
			t.Logf("Weight distribution - min: %d, max: %d, delta: %d", minWeight, maxWeight, weightDelta)

			if tcase.maxWeightDelta > 0 && weightDelta > tcase.maxWeightDelta {
				t.Errorf("weight delta %d exceeds maximum allowed %d", weightDelta, tcase.maxWeightDelta)
			}

			// Check if large tenant is alone in a shard when expected
			if tcase.expectSoleTenant {
				found := false
				for i := 0; i < tcase.numShards; i++ {
					if len(buckets[i]) == 1 && buckets[i][0] == tcase.soleTenant {
						found = true
						t.Logf("Large tenant %s correctly assigned alone to shard %d with weight %d",
							tcase.soleTenant, i, weights[i])
						break
					}
				}
				if !found {
					t.Errorf("expected tenant %s to be the sole tenant in a shard, but it shares or is not found",
						tcase.soleTenant)
					for i := 0; i < tcase.numShards; i++ {
						t.Logf("Shard %d: tenants=%v, weight=%d", i, buckets[i], weights[i])
					}
				}
			}

			for i := 0; i < tcase.numShards; i++ {
				t.Logf("Shard %d: %d tenants, weight=%d, tenants=%v",
					i, len(buckets[i]), weights[i], buckets[i])
			}
		})
	}
}

func TestReadTenantWeights(t *testing.T) {
	for _, tcase := range []struct {
		name         string
		fileContent  string
		expectedData map[string]int
		expectError  bool
	}{
		{
			name:        "valid JSON with multiple tenants",
			fileContent: `{"tenant1": 10, "tenant2": 20, "tenant3": 30}`,
			expectedData: map[string]int{
				"tenant1": 10,
				"tenant2": 20,
				"tenant3": 30,
			},
			expectError: false,
		},
		{
			name:        "single tenant",
			fileContent: `{"single": 100}`,
			expectedData: map[string]int{
				"single": 100,
			},
			expectError: false,
		},
		{
			name:         "empty JSON object",
			fileContent:  `{}`,
			expectedData: map[string]int{},
			expectError:  false,
		},
		{
			name:        "invalid JSON",
			fileContent: `invalid json content`,
			expectError: true,
		},
		{
			name:        "malformed JSON - missing brace",
			fileContent: `{"tenant1": 10`,
			expectError: true,
		},
		{
			name:        "wrong type - array instead of object",
			fileContent: `["tenant1", "tenant2"]`,
			expectError: true,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			tempFile, err := os.CreateTemp("", "tenant_weights_*.json")
			if err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}
			defer tempFile.Close()
			defer os.Remove(tempFile.Name())

			if _, err := tempFile.Write([]byte(tcase.fileContent)); err != nil {
				t.Fatalf("failed to write test data: %v", err)
			}

			weights, err := readTenantWeights(tempFile.Name())

			if tcase.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(weights) != len(tcase.expectedData) {
				t.Errorf("expected %d tenants, got %d", len(tcase.expectedData), len(weights))
			}

			foundTenants := make(map[string]int)
			for _, tw := range weights {
				foundTenants[tw.TenantName] = tw.Weight
			}

			for name, expectedWeight := range tcase.expectedData {
				if weight, ok := foundTenants[name]; !ok {
					t.Errorf("tenant %s not found in results", name)
				} else if weight != expectedWeight {
					t.Errorf("tenant %s: expected weight %d, got %d", name, expectedWeight, weight)
				}
			}

			for name := range foundTenants {
				if _, ok := tcase.expectedData[name]; !ok {
					t.Errorf("unexpected tenant %s in results", name)
				}
			}
		})
	}
}

func TestReadTenantWeights_NonexistentFile(t *testing.T) {
	_, err := readTenantWeights("/nonexistent/path/to/file.json")
	if err == nil {
		t.Error("expected error for nonexistent file, got nil")
	}
}

func TestTenantToShard(t *testing.T) {
	for _, tcase := range []struct {
		name       string
		tenantName string
		numShards  int
		// We don't specify expected shard since hash output is implementation-dependent,
		// but we validate properties
	}{
		{
			name:       "simple tenant with 3 shards",
			tenantName: "tenant1",
			numShards:  3,
		},
		{
			name:       "simple tenant with 5 shards",
			tenantName: "tenant1",
			numShards:  5,
		},
		{
			name:       "tenant with special characters",
			tenantName: "tenant-with-dashes",
			numShards:  4,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			shard := tenantToShard(tcase.tenantName, tcase.numShards)

			// Verify shard is within valid range
			if shard < 0 || shard >= tcase.numShards {
				t.Errorf("shard %d out of range [0, %d)", shard, tcase.numShards)
			}
		})
	}
}

func TestTenantToShard_Consistency(t *testing.T) {
	// Test that same tenant always maps to same shard
	tenantName := "consistent-tenant"
	numShards := 10

	firstShard := tenantToShard(tenantName, numShards)

	// Call multiple times and verify consistency
	for i := 0; i < 100; i++ {
		shard := tenantToShard(tenantName, numShards)
		if shard != firstShard {
			t.Errorf("inconsistent shard assignment: got %d, expected %d on iteration %d", shard, firstShard, i)
		}
	}
}

func TestTenantToShard_Distribution(t *testing.T) {
	// Test that distribution is reasonably uniform across shards
	numShards := 5
	numTenants := 1000

	shardCounts := make(map[int]int)
	for i := 0; i < numTenants; i++ {
		tenantName := "tenant-" + string(rune(i))
		shard := tenantToShard(tenantName, numShards)
		shardCounts[shard]++
	}

	// Check that all shards got some tenants
	for i := 0; i < numShards; i++ {
		if shardCounts[i] == 0 {
			t.Errorf("shard %d received no tenants", i)
		}
	}

	// Log distribution for visibility
	for i := 0; i < numShards; i++ {
		t.Logf("Shard %d: %d tenants (%.1f%%)", i, shardCounts[i], float64(shardCounts[i])/float64(numTenants)*100)
	}
}

func TestDiscoverTenantsFromBucket(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()

	for _, tcase := range []struct {
		name             string
		setupBucket      func(bkt objstore.Bucket) error
		commonPathPrefix string
		knownTenants     []TenantWeight
		expectedTenants  []string
		expectError      bool
	}{
		{
			name: "discover multiple new tenants",
			setupBucket: func(bkt objstore.Bucket) error {
				if err := bkt.Upload(ctx, "v1/raw/tenant-a/.keep", strings.NewReader("")); err != nil {
					return err
				}
				if err := bkt.Upload(ctx, "v1/raw/tenant-b/.keep", strings.NewReader("")); err != nil {
					return err
				}
				if err := bkt.Upload(ctx, "v1/raw/tenant-c/.keep", strings.NewReader("")); err != nil {
					return err
				}
				return nil
			},
			commonPathPrefix: "v1/raw/",
			knownTenants:     []TenantWeight{},
			expectedTenants:  []string{"tenant-a", "tenant-b", "tenant-c"},
			expectError:      false,
		},
		{
			name: "skip known tenants",
			setupBucket: func(bkt objstore.Bucket) error {
				if err := bkt.Upload(ctx, "v1/raw/known-tenant/.keep", strings.NewReader("")); err != nil {
					return err
				}
				if err := bkt.Upload(ctx, "v1/raw/new-tenant/.keep", strings.NewReader("")); err != nil {
					return err
				}
				return nil
			},
			commonPathPrefix: "v1/raw/",
			knownTenants: []TenantWeight{
				{TenantName: "known-tenant", Weight: 100},
			},
			expectedTenants: []string{"new-tenant"},
			expectError:     false,
		},
		{
			name: "empty bucket",
			setupBucket: func(bkt objstore.Bucket) error {
				return nil
			},
			commonPathPrefix: "v1/raw/",
			knownTenants:     []TenantWeight{},
			expectedTenants:  []string{},
			expectError:      false,
		},
		{
			name: "all tenants already known",
			setupBucket: func(bkt objstore.Bucket) error {
				if err := bkt.Upload(ctx, "v1/raw/tenant-1/.keep", strings.NewReader("")); err != nil {
					return err
				}
				if err := bkt.Upload(ctx, "v1/raw/tenant-2/.keep", strings.NewReader("")); err != nil {
					return err
				}
				return nil
			},
			commonPathPrefix: "v1/raw/",
			knownTenants: []TenantWeight{
				{TenantName: "tenant-1", Weight: 50},
				{TenantName: "tenant-2", Weight: 50},
			},
			expectedTenants: []string{},
			expectError:     false,
		},
		{
			name: "mixed known and new tenants",
			setupBucket: func(bkt objstore.Bucket) error {
				tenants := []string{"tenant-a", "tenant-b", "tenant-c", "tenant-d", "tenant-e"}
				for _, tenant := range tenants {
					if err := bkt.Upload(ctx, "v1/raw/"+tenant+"/.keep", strings.NewReader("")); err != nil {
						return err
					}
				}
				return nil
			},
			commonPathPrefix: "v1/raw/",
			knownTenants: []TenantWeight{
				{TenantName: "tenant-a", Weight: 100},
				{TenantName: "tenant-c", Weight: 80},
			},
			expectedTenants: []string{"tenant-b", "tenant-d", "tenant-e"},
			expectError:     false,
		},
		{
			name: "different path prefix",
			setupBucket: func(bkt objstore.Bucket) error {
				if err := bkt.Upload(ctx, "data/metrics/tenant-x/.keep", strings.NewReader("")); err != nil {
					return err
				}
				if err := bkt.Upload(ctx, "data/metrics/tenant-y/.keep", strings.NewReader("")); err != nil {
					return err
				}
				return nil
			},
			commonPathPrefix: "data/metrics/",
			knownTenants:     []TenantWeight{},
			expectedTenants:  []string{"tenant-x", "tenant-y"},
			expectError:      false,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()

			// Setup bucket state
			if err := tcase.setupBucket(bkt); err != nil {
				t.Fatalf("failed to setup bucket: %v", err)
			}

			// Discover tenants
			discovered, err := discoverTenantsFromBucket(ctx, bkt, logger, tcase.commonPathPrefix, tcase.knownTenants)

			if tcase.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Sort both slices for comparison
			sort.Strings(discovered)
			expectedSorted := make([]string, len(tcase.expectedTenants))
			copy(expectedSorted, tcase.expectedTenants)
			sort.Strings(expectedSorted)

			// Compare discovered tenants
			if len(discovered) != len(expectedSorted) {
				t.Errorf("expected %d discovered tenants, got %d", len(expectedSorted), len(discovered))
				t.Logf("Expected: %v", expectedSorted)
				t.Logf("Got: %v", discovered)
				return
			}

			for i, tenant := range discovered {
				if tenant != expectedSorted[i] {
					t.Errorf("at index %d: expected tenant %s, got %s", i, expectedSorted[i], tenant)
				}
			}
		})
	}
}

func encodeTempFile(t *testing.T, tempFile *os.File, config interface{}) (string, error) {
	t.Helper()

	if err := json.NewEncoder(tempFile).Encode(config); err != nil {
		return "", err
	}
	return tempFile.Name(), nil
}

func TestSetupTenantPartitioning(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()

	for _, tcase := range []struct {
		name                      string
		setupBucket               func(bkt objstore.Bucket) error
		setupConfigFile           func() (string, error)
		commonPathPrefix          string
		numShards                 int
		expectedActiveTenants     int
		expectedDiscoveredTenants int
		expectError               bool
	}{
		{
			name: "active and discovered tenants",
			setupBucket: func(bkt objstore.Bucket) error {
				// Create some discovered tenants
				for _, tenant := range []string{"discovered-1", "discovered-2"} {
					if err := bkt.Upload(ctx, "v1/raw/"+tenant+"/.keep", strings.NewReader("")); err != nil {
						return err
					}
				}
				return nil
			},
			setupConfigFile: func() (string, error) {
				tempFile, err := os.CreateTemp("", "tenant_config_*.json")
				if err != nil {
					return "", err
				}
				defer tempFile.Close()
				config := map[string]int{
					"active-1": 100,
					"active-2": 80,
				}
				return encodeTempFile(t, tempFile, config)
			},
			commonPathPrefix:          "v1/raw/",
			numShards:                 2,
			expectedActiveTenants:     2,
			expectedDiscoveredTenants: 2,
			expectError:               false,
		},
		{
			name: "only active tenants",
			setupBucket: func(bkt objstore.Bucket) error {
				return nil // Empty bucket
			},
			setupConfigFile: func() (string, error) {
				tempFile, err := os.CreateTemp("", "tenant_config_*.json")
				if err != nil {
					return "", err
				}
				defer tempFile.Close()
				config := map[string]int{
					"tenant-a": 50,
					"tenant-b": 50,
					"tenant-c": 50,
				}
				return encodeTempFile(t, tempFile, config)
			},
			commonPathPrefix:          "v1/raw/",
			numShards:                 2,
			expectedActiveTenants:     3,
			expectedDiscoveredTenants: 0,
			expectError:               false,
		},
		{
			name: "only discovered tenants",
			setupBucket: func(bkt objstore.Bucket) error {
				for _, tenant := range []string{"tenant-1", "tenant-2", "tenant-3", "tenant-4"} {
					if err := bkt.Upload(ctx, "v1/raw/"+tenant+"/.keep", strings.NewReader("")); err != nil {
						return err
					}
				}
				return nil
			},
			setupConfigFile: func() (string, error) {
				tempFile, err := os.CreateTemp("", "tenant_config_*.json")
				if err != nil {
					return "", err
				}
				defer tempFile.Close()
				config := map[string]int{} // Empty config
				return encodeTempFile(t, tempFile, config)
			},
			commonPathPrefix:          "v1/raw/",
			numShards:                 2,
			expectedActiveTenants:     0,
			expectedDiscoveredTenants: 4,
			expectError:               false,
		},
		{
			name: "large mixed setup",
			setupBucket: func(bkt objstore.Bucket) error {
				// Create many discovered tenants
				for i := 1; i <= 20; i++ {
					tenant := "discovered-" + string(rune('a'+i-1))
					if err := bkt.Upload(ctx, "v1/raw/"+tenant+"/.keep", strings.NewReader("")); err != nil {
						return err
					}
				}
				return nil
			},
			setupConfigFile: func() (string, error) {
				tempFile, err := os.CreateTemp("", "tenant_config_*.json")
				if err != nil {
					return "", err
				}
				defer tempFile.Close()
				config := map[string]int{
					"active-large":   1000,
					"active-medium":  500,
					"active-small-1": 10,
					"active-small-2": 10,
					"active-small-3": 10,
				}
				return encodeTempFile(t, tempFile, config)
			},
			commonPathPrefix:          "v1/raw/",
			numShards:                 3,
			expectedActiveTenants:     5,
			expectedDiscoveredTenants: 20,
			expectError:               false,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()

			if err := tcase.setupBucket(bkt); err != nil {
				t.Fatalf("failed to setup bucket: %v", err)
			}

			configPath, err := tcase.setupConfigFile()
			if err != nil {
				t.Fatalf("failed to setup config file: %v", err)
			}
			defer os.Remove(configPath)

			tenantAssignments, err := SetupTenantPartitioning(ctx, bkt, logger, configPath, tcase.commonPathPrefix, tcase.numShards)

			if tcase.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			t.Logf("tenant assignments: %v", tenantAssignments)
		})
	}
}

func TestSetupTenantPartitioning_InvalidConfig(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()
	bkt := objstore.NewInMemBucket()

	_, err := SetupTenantPartitioning(ctx, bkt, logger, "/nonexistent/config.json", "v1/raw/", 3)
	if err == nil {
		t.Error("expected error for nonexistent config file, got nil")
	}
}
