// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
)

func TestMain(m *testing.M) {
	custom.TolerantVerifyLeakMain(m)
}

func TestAddingExternalLabelsForTenants(t *testing.T) {
	if testing.
		Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()

	for _, tc := range []struct {
		name                      string
		cfg                       []HashringConfig
		expectedExternalLabelSets []labels.Labels
	}{
		{
			name: "One tenant - No labels",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1"},
				},
			},
			expectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("replica", "test", "tenant_id", "tenant1"),
			},
		},
		{
			name: "One tenant - One label",
			cfg: []HashringConfig{
				{
					Endpoints:      []Endpoint{{Address: "node1"}},
					Tenants:        []string{"tenant1"},
					ExternalLabels: labels.FromStrings("name1", "value1"),
				},
			},
			expectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("name1", "value1", "replica", "test", "tenant_id", "tenant1"),
			},
		},
		{
			name: "One tenant - Multiple labels",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name1": "value1",
						"name2": "value2",
						"name3": "value3",
					}),
				},
			},
			expectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant1"),
			},
		},
		{
			name: "Multiple tenants - No labels",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1", "tenant2", "tenant3"},
				},
			},
			expectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("replica", "test", "tenant_id", "tenant1"),
				labels.FromStrings("replica", "test", "tenant_id", "tenant2"),
				labels.FromStrings("replica", "test", "tenant_id", "tenant3"),
			},
		},
		{
			name: "Multiple tenants - One label",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1", "tenant2", "tenant3"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name1": "value1",
					}),
				},
			},
			expectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("name1", "value1", "replica", "test", "tenant_id", "tenant1"),
				labels.FromStrings("name1", "value1", "replica", "test", "tenant_id", "tenant2"),
				labels.FromStrings("name1", "value1", "replica", "test", "tenant_id", "tenant3"),
			},
		},
		{
			name: "Multiple tenants - Multiple labels",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1", "tenant2", "tenant3"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name3": "value3",
						"name2": "value2",
						"name1": "value1",
					}),
				},
			},
			expectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant1"),
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant2"),
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant3"),
			},
		},
		{
			name: "Multiple hashrings - No repeated tenants",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1", "tenant2", "tenant3"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name1": "value1",
						"name2": "value2",
						"name3": "value3",
					}),
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant6"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name6": "value6",
						"name5": "value5",
						"name4": "value4",
					}),
				},
			},
			expectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant1"),
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant2"),
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant3"),
				labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6",
					"replica", "test", "tenant_id", "tenant4"),
				labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6",
					"replica", "test", "tenant_id", "tenant5"),
				labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6",
					"replica", "test", "tenant_id", "tenant6"),
			},
		},
		{
			name: "Multiple hashrings - One repeated tenant",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1", "tenant2", "tenant3"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name3": "value3",
						"name2": "value2",
						"name1": "value1",
					}),
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant1"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name4": "value4",
						"name5": "value5",
						"name6": "value6",
					}),
				},
			},
			expectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant1"),
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant2"),
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant3"),
				labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6",
					"replica", "test", "tenant_id", "tenant4"),
				labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6",
					"replica", "test", "tenant_id", "tenant5"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := initializeMultiTSDB(t.TempDir())

			err := m.SetHashringConfig(tc.cfg)
			require.NoError(t, err)

			for _, c := range tc.cfg {
				for _, tenantId := range c.Tenants {
					if m.testGetTenant(tenantId) == nil {
						err = appendSample(m, tenantId, time.Now())
						require.NoError(t, err)
					}
				}
			}

			err = m.Open()
			require.NoError(t, err)

			storeClients := m.TSDBLocalClients()
			require.Equal(t, len(tc.expectedExternalLabelSets), len(storeClients))

			setOfExpectedClientLabelSets, setOfActualClientLabelSets := setupSetsOfExpectedAndActualStoreClientLabelSets(
				tc.expectedExternalLabelSets, storeClients)

			for _, cls := range setOfActualClientLabelSets {
				require.Contains(t, setOfExpectedClientLabelSets, cls)
			}

			err = m.Flush()
			require.NoError(t, err)

			err = m.Close()
			require.NoError(t, err)
		})
	}
}

func TestLabelSetsOfTenantsWhenAddingTenants(t *testing.T) {
	t.Parallel()

	initialConfig := []HashringConfig{
		{
			Endpoints: []Endpoint{{Address: "node1"}},
			Tenants:   []string{"tenant1", "tenant2", "tenant3"},
			ExternalLabels: labels.FromMap(map[string]string{
				"name1": "value1",
				"name2": "value2",
				"name3": "value3",
			}),
		},
	}
	initialExpectedExternalLabelSets := []labels.Labels{
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant1"),
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant2"),
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant3"),
	}

	changedConfig := []HashringConfig{
		{
			Endpoints: []Endpoint{{Address: "node1"}},
			Tenants:   []string{"tenant1", "tenant2", "tenant3", "tenant4", "tenant5"},
			ExternalLabels: labels.FromMap(map[string]string{
				"name1": "value1",
				"name2": "value2",
				"name3": "value3",
			}),
		},
	}
	changedExpectedExternalLabelSets := []labels.Labels{
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant1"),
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant2"),
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant3"),
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant4"),
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant5"),
	}

	t.Run("Adding tenants", func(t *testing.T) {
		m := initializeMultiTSDB(t.TempDir())

		err := m.SetHashringConfig(initialConfig)
		require.NoError(t, err)

		for _, c := range initialConfig {
			for _, tenantId := range c.Tenants {
				if m.testGetTenant(tenantId) == nil {
					err = appendSample(m, tenantId, time.Now())
					require.NoError(t, err)
				}
			}
		}

		err = m.Open()
		require.NoError(t, err)

		initialStoreClients := m.TSDBLocalClients()
		require.Equal(t, len(initialExpectedExternalLabelSets), len(initialStoreClients))

		initialSetOfExpectedClientLabelSets, initialSetOfActualClientLabelSets := setupSetsOfExpectedAndActualStoreClientLabelSets(
			initialExpectedExternalLabelSets, initialStoreClients)

		for _, cls := range initialSetOfActualClientLabelSets {
			require.Contains(t, initialSetOfExpectedClientLabelSets, cls)
		}

		err = m.SetHashringConfig(changedConfig)
		require.NoError(t, err)

		for _, c := range changedConfig {
			for _, tenantId := range c.Tenants {
				if m.testGetTenant(tenantId) == nil {
					err = appendSample(m, tenantId, time.Now())
					require.NoError(t, err)
				}
			}
		}

		err = m.Flush()
		require.NoError(t, err)

		err = m.Open()
		require.NoError(t, err)

		changedStoreClients := m.TSDBLocalClients()
		require.Equal(t, len(changedExpectedExternalLabelSets), len(changedStoreClients))

		changedSetOfExpectedClientLabelSets, changedSetOfActualClientLabelSets := setupSetsOfExpectedAndActualStoreClientLabelSets(
			changedExpectedExternalLabelSets, changedStoreClients)

		for _, cls := range changedSetOfActualClientLabelSets {
			require.Contains(t, changedSetOfExpectedClientLabelSets, cls)
		}

		err = m.Flush()
		require.NoError(t, err)

		err = m.Close()
		require.NoError(t, err)
	})
}

func TestLabelSetsOfTenantsWhenChangingLabels(t *testing.T) {
	if testing.
		Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()

	initialConfig := []HashringConfig{
		{
			Endpoints: []Endpoint{{Address: "node1"}},
			Tenants:   []string{"tenant1", "tenant2", "tenant3"},
			ExternalLabels: labels.FromMap(map[string]string{
				"name1": "value1",
				"name2": "value2",
				"name3": "value3",
			}),
		},
		{
			Endpoints: []Endpoint{{Address: "node2"}},
			Tenants:   []string{"tenant4", "tenant5", "tenant6"},
			ExternalLabels: labels.FromMap(map[string]string{
				"name6": "value6",
				"name5": "value5",
				"name4": "value4",
			}),
		},
	}
	initialExpectedExternalLabelSets := []labels.Labels{
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant1"),
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant2"),
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant3"),
		labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6",
			"replica", "test", "tenant_id", "tenant4"),
		labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6",
			"replica", "test", "tenant_id", "tenant5"),
		labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6",
			"replica", "test", "tenant_id", "tenant6"),
	}

	for _, tc := range []struct {
		name                             string
		changedConfig                    []HashringConfig
		changedExpectedExternalLabelSets []labels.Labels
	}{
		{
			name: "Adding labels",
			changedConfig: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1", "tenant2", "tenant3"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name1": "value1",
						"name2": "value2",
						"name3": "value3",
						"name4": "value4",
					}),
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant6"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name4": "value4",
						"name5": "value5",
						"name6": "value6",
						"name7": "value7",
					}),
				},
			},
			changedExpectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3", "name4", "value4",
					"replica", "test", "tenant_id", "tenant1"),
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3", "name4", "value4",
					"replica", "test", "tenant_id", "tenant2"),
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3", "name4", "value4",
					"replica", "test", "tenant_id", "tenant3"),
				labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6", "name7", "value7",
					"replica", "test", "tenant_id", "tenant4"),
				labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6", "name7", "value7",
					"replica", "test", "tenant_id", "tenant5"),
				labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6", "name7", "value7",
					"replica", "test", "tenant_id", "tenant6"),
			},
		},
		{
			name: "Deleting some labels",
			changedConfig: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1", "tenant2", "tenant3"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name1": "value1",
						"name2": "value2",
					}),
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant6"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name4": "value4",
						"name5": "value5",
					}),
				},
			},
			changedExpectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("name1", "value1", "name2", "value2",
					"replica", "test", "tenant_id", "tenant1"),
				labels.FromStrings("name1", "value1", "name2", "value2",
					"replica", "test", "tenant_id", "tenant2"),
				labels.FromStrings("name1", "value1", "name2", "value2",
					"replica", "test", "tenant_id", "tenant3"),
				labels.FromStrings("name4", "value4", "name5", "value5",
					"replica", "test", "tenant_id", "tenant4"),
				labels.FromStrings("name4", "value4", "name5", "value5",
					"replica", "test", "tenant_id", "tenant5"),
				labels.FromStrings("name4", "value4", "name5", "value5",
					"replica", "test", "tenant_id", "tenant6"),
			},
		},
		{
			name: "Deleting all labels",
			changedConfig: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1", "tenant2", "tenant3"},
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant6"},
				},
			},
			changedExpectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("replica", "test", "tenant_id", "tenant1"),
				labels.FromStrings("replica", "test", "tenant_id", "tenant2"),
				labels.FromStrings("replica", "test", "tenant_id", "tenant3"),
				labels.FromStrings("replica", "test", "tenant_id", "tenant4"),
				labels.FromStrings("replica", "test", "tenant_id", "tenant5"),
				labels.FromStrings("replica", "test", "tenant_id", "tenant6"),
			},
		},
		{
			name: "Changing values of some labels",
			changedConfig: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1", "tenant2", "tenant3"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name1": "value3",
						"name2": "value2",
						"name3": "value3",
					}),
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant6"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name4": "value6",
						"name5": "value5",
						"name6": "value6",
					}),
				},
			},
			changedExpectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("name1", "value3", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant1"),
				labels.FromStrings("name1", "value3", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant2"),
				labels.FromStrings("name1", "value3", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant3"),
				labels.FromStrings("name4", "value6", "name5", "value5", "name6", "value6",
					"replica", "test", "tenant_id", "tenant4"),
				labels.FromStrings("name4", "value6", "name5", "value5", "name6", "value6",
					"replica", "test", "tenant_id", "tenant5"),
				labels.FromStrings("name4", "value6", "name5", "value5", "name6", "value6",
					"replica", "test", "tenant_id", "tenant6"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := initializeMultiTSDB(t.TempDir())

			err := m.SetHashringConfig(initialConfig)
			require.NoError(t, err)

			for _, c := range initialConfig {
				for _, tenantId := range c.Tenants {
					if m.testGetTenant(tenantId) == nil {
						err = appendSample(m, tenantId, time.Now())
						require.NoError(t, err)
					}
				}
			}

			err = m.Open()
			require.NoError(t, err)

			initialStoreClients := m.TSDBLocalClients()
			require.Equal(t, len(initialExpectedExternalLabelSets), len(initialStoreClients))

			initialSetOfExpectedClientLabelSets, initialSetOfActualClientLabelSets := setupSetsOfExpectedAndActualStoreClientLabelSets(
				initialExpectedExternalLabelSets, initialStoreClients)

			for _, cls := range initialSetOfActualClientLabelSets {
				require.Contains(t, initialSetOfExpectedClientLabelSets, cls)
			}

			err = m.SetHashringConfig(tc.changedConfig)
			require.NoError(t, err)

			err = m.Flush()
			require.NoError(t, err)

			err = m.Open()
			require.NoError(t, err)

			changedStoreClients := m.TSDBLocalClients()
			require.Equal(t, len(tc.changedExpectedExternalLabelSets), len(changedStoreClients))

			changedSetOfExpectedClientLabelSets, changedSetOfActualClientLabelSets := setupSetsOfExpectedAndActualStoreClientLabelSets(
				tc.changedExpectedExternalLabelSets, changedStoreClients)

			for _, cls := range changedSetOfActualClientLabelSets {
				require.Contains(t, changedSetOfExpectedClientLabelSets, cls)
			}

			err = m.Flush()
			require.NoError(t, err)

			err = m.Close()
			require.NoError(t, err)
		})
	}
}

func TestAddingLabelsWhenTenantAppearsInMultipleHashrings(t *testing.T) {
	if testing.
		Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()

	initialConfig := []HashringConfig{
		{
			Endpoints: []Endpoint{{Address: "node1"}},
			Tenants:   []string{"tenant1", "tenant2", "tenant3"},
			ExternalLabels: labels.FromMap(map[string]string{
				"name3": "value3",
				"name2": "value2",
				"name1": "value1",
			}),
		},
		{
			Endpoints: []Endpoint{{Address: "node2"}},
			Tenants:   []string{"tenant4", "tenant5", "tenant1"},
			ExternalLabels: labels.FromMap(map[string]string{
				"name4": "value4",
				"name5": "value5",
				"name6": "value6",
			}),
		},
	}
	initialExpectedExternalLabelSets := []labels.Labels{
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant1"),
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant2"),
		labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
			"replica", "test", "tenant_id", "tenant3"),
		labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6",
			"replica", "test", "tenant_id", "tenant4"),
		labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6",
			"replica", "test", "tenant_id", "tenant5"),
	}

	for _, tc := range []struct {
		name                             string
		changedConfig                    []HashringConfig
		changedExpectedExternalLabelSets []labels.Labels
	}{
		{
			name: "Adding labels in first hashring that tenant appears",
			changedConfig: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1", "tenant2", "tenant3"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name1": "value1",
						"name2": "value2",
						"name3": "value3",
						"name4": "value4",
					}),
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant1"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name4": "value4",
						"name5": "value5",
						"name6": "value6",
					}),
				},
			},
			changedExpectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3", "name4", "value4",
					"replica", "test", "tenant_id", "tenant1"),
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3", "name4", "value4",
					"replica", "test", "tenant_id", "tenant2"),
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3", "name4", "value4",
					"replica", "test", "tenant_id", "tenant3"),
				labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6",
					"replica", "test", "tenant_id", "tenant4"),
				labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6",
					"replica", "test", "tenant_id", "tenant5"),
			},
		},
		{
			name: "Adding labels in second hashring that tenant appears",
			changedConfig: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1", "tenant2", "tenant3"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name1": "value1",
						"name2": "value2",
						"name3": "value3",
					}),
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant1"},
					ExternalLabels: labels.FromMap(map[string]string{
						"name4": "value4",
						"name5": "value5",
						"name6": "value6",
						"name7": "value7",
					}),
				},
			},
			changedExpectedExternalLabelSets: []labels.Labels{
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant1"),
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant2"),
				labels.FromStrings("name1", "value1", "name2", "value2", "name3", "value3",
					"replica", "test", "tenant_id", "tenant3"),
				labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6", "name7", "value7",
					"replica", "test", "tenant_id", "tenant4"),
				labels.FromStrings("name4", "value4", "name5", "value5", "name6", "value6", "name7", "value7",
					"replica", "test", "tenant_id", "tenant5"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := initializeMultiTSDB(t.TempDir())

			err := m.SetHashringConfig(initialConfig)
			require.NoError(t, err)

			for _, c := range initialConfig {
				for _, tenantId := range c.Tenants {
					if m.testGetTenant(tenantId) == nil {
						err = appendSample(m, tenantId, time.Now())
						require.NoError(t, err)
					}
				}
			}

			err = m.Open()
			require.NoError(t, err)

			initialStoreClients := m.TSDBLocalClients()
			require.Equal(t, len(initialExpectedExternalLabelSets), len(initialStoreClients))

			initialSetOfExpectedClientLabelSets, initialSetOfActualClientLabelSets := setupSetsOfExpectedAndActualStoreClientLabelSets(
				initialExpectedExternalLabelSets, initialStoreClients)

			for _, cls := range initialSetOfActualClientLabelSets {
				require.Contains(t, initialSetOfExpectedClientLabelSets, cls)
			}

			err = m.SetHashringConfig(tc.changedConfig)
			require.NoError(t, err)

			err = m.Flush()
			require.NoError(t, err)

			err = m.Open()
			require.NoError(t, err)

			changedStoreClients := m.TSDBLocalClients()
			require.Equal(t, len(tc.changedExpectedExternalLabelSets), len(changedStoreClients))

			changedSetOfExpectedClientLabelSets, changedSetOfActualClientLabelSets := setupSetsOfExpectedAndActualStoreClientLabelSets(
				tc.changedExpectedExternalLabelSets, changedStoreClients)

			for _, cls := range changedSetOfActualClientLabelSets {
				require.Contains(t, changedSetOfExpectedClientLabelSets, cls)
			}

			err = m.Flush()
			require.NoError(t, err)

			err = m.Close()
			require.NoError(t, err)
		})
	}
}

func TestReceiverLabelsNotOverwrittenByExternalLabels(t *testing.T) {
	t.Parallel()

	cfg := []HashringConfig{
		{
			Endpoints: []Endpoint{{Address: "node1"}},
			Tenants:   []string{"tenant1"},
			ExternalLabels: labels.FromMap(map[string]string{
				"replica":   "0",
				"tenant_id": "tenant2",
				"name3":     "value3",
			}),
		},
	}
	expectedExternalLabelSets := []labels.Labels{
		labels.FromStrings("name3", "value3", "replica", "test", "tenant_id", "tenant1"),
	}

	t.Run("Receiver's labels not overwritten by external labels", func(t *testing.T) {
		m := initializeMultiTSDB(t.TempDir())

		err := m.SetHashringConfig(cfg)
		require.NoError(t, err)

		for _, c := range cfg {
			for _, tenantId := range c.Tenants {
				if m.testGetTenant(tenantId) == nil {
					err = appendSample(m, tenantId, time.Now())
					require.NoError(t, err)
				}
			}
		}

		err = m.Open()
		require.NoError(t, err)

		storeClients := m.TSDBLocalClients()
		require.Equal(t, len(expectedExternalLabelSets), len(storeClients))

		setOfExpectedClientLabelSets, setOfActualClientLabelSets := setupSetsOfExpectedAndActualStoreClientLabelSets(
			expectedExternalLabelSets, storeClients)

		for _, cls := range setOfActualClientLabelSets {
			require.Contains(t, setOfExpectedClientLabelSets, cls)
		}

		err = m.Flush()
		require.NoError(t, err)

		err = m.Close()
		require.NoError(t, err)
	})
}

func initializeMultiTSDB(dir string) *MultiTSDB {
	var bucket objstore.Bucket

	m := NewMultiTSDB(dir, log.NewNopLogger(), prometheus.NewRegistry(),
		&tsdb.Options{
			MinBlockDuration:  (2 * time.Hour).Milliseconds(),
			MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
			RetentionDuration: (6 * time.Hour).Milliseconds(),
		},
		labels.FromStrings("replica", "test"),
		"tenant_id",
		bucket,
		false,
		false,
		metadata.NoneFunc,
	)

	return m
}

// Set up expected set of label sets of store clients from expected external label sets of all tenants
// and set up actual set of label sets from actual store clients.
func setupSetsOfExpectedAndActualStoreClientLabelSets(
	expectedExternalLabelSets []labels.Labels, actualStoreClients []store.Client) ([][]labels.Labels, [][]labels.Labels) {
	setOfExpectedClientLabelSets := make([][]labels.Labels, len(expectedExternalLabelSets))
	setOfActualClientLabelSets := make([][]labels.Labels, len(actualStoreClients))

	for i := range actualStoreClients {
		testStore := store.TSDBStore{}
		testStore.SetExtLset(expectedExternalLabelSets[i])

		expectedClientLabelSets := labelpb.ZLabelSetsToPromLabelSets(testStore.LabelSet()...)
		setOfExpectedClientLabelSets = append(setOfExpectedClientLabelSets, expectedClientLabelSets)

		actualClientLabelSets := actualStoreClients[i].LabelSets()
		setOfActualClientLabelSets = append(setOfActualClientLabelSets, actualClientLabelSets)
	}

	return setOfExpectedClientLabelSets, setOfActualClientLabelSets
}

func mustNewLimiter(t *testing.T) *Limiter {
	t.Helper()

	l, err := NewLimiter(extkingpin.NewNopConfig(), nil, RouterIngestor, log.NewNopLogger(), time.Second)
	testutil.Ok(t, err)
	return l
}

func TestSendRemoteWriteMarksPeerUnavailableOnAnyError(t *testing.T) {
	t.Parallel()

	for _, sendUnavailable := range []bool{true, false} {
		t.Run(fmt.Sprintf("sendUnavailable=%v", sendUnavailable), func(t *testing.T) {
			h := NewHandler(log.NewNopLogger(), &Options{
				Writer:            NewWriter(log.NewNopLogger(), newFakeTenantAppendable(&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}), &WriterOptions{}),
				ForwardTimeout:    time.Second,
				ReplicationFactor: 1,
				Limiter:           mustNewLimiter(t),
			})

			endpoint := Endpoint{Address: "addr-a", CapNProtoAddress: "addr-a"}
			cl := &stubAsyncClient{err: context.DeadlineExceeded}
			stubPeers := &stubPeersGroup{
				client: cl,
			}

			h.peers = stubPeers

			responses := make(chan writeResponse, 1)
			var wg sync.WaitGroup
			wg.Add(1)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cl.sendUnavailable = sendUnavailable

			h.sendRemoteWrite(ctx, "tenant-a", endpointReplica{
				endpoint: endpoint,
				replica:  0,
			}, trackedSeries{
				seriesIDs:  []int{0},
				timeSeries: []prompb.TimeSeries{{}},
			}, false, responses, &wg)

			wg.Wait()
			close(responses)

			if sendUnavailable {
				testutil.Equals(t, []Endpoint{endpoint}, stubPeers.markUnavailable)
				testutil.Equals(t, []Endpoint(nil), stubPeers.closed)
			} else {
				testutil.Equals(t, []Endpoint(nil), stubPeers.markUnavailable)
				testutil.Equals(t, []Endpoint(nil), stubPeers.closed)
			}

		})
	}
}

type stubPeersGroup struct {
	client WriteableStoreAsyncClient

	markUnavailable []Endpoint
	closed          []Endpoint
}

func (s *stubPeersGroup) Close() error { return nil }

func (s *stubPeersGroup) markPeerUnavailable(endpoint Endpoint) {
	s.markUnavailable = append(s.markUnavailable, endpoint)
}

func (s *stubPeersGroup) markPeerAvailable(Endpoint) {}

func (s *stubPeersGroup) reset() {}

func (s *stubPeersGroup) close(endpoint Endpoint) error {
	s.closed = append(s.closed, endpoint)
	return nil
}

func (s *stubPeersGroup) getConnection(context.Context, Endpoint) (WriteableStoreAsyncClient, error) {
	return s.client, nil
}

type stubAsyncClient struct {
	err error

	sendUnavailable bool
}

func (s *stubAsyncClient) RemoteWrite(ctx context.Context, in *storepb.WriteRequest, opts ...grpc.CallOption) (*storepb.WriteResponse, error) {
	return &storepb.WriteResponse{}, nil
}

func (s *stubAsyncClient) RemoteWriteAsync(ctx context.Context, in *storepb.WriteRequest, er endpointReplica, seriesIDs []int, responses chan writeResponse, cb func(error)) {
	err := errors.Wrapf(s.err, "forwarding request to endpoint %v", er.endpoint)
	if s.sendUnavailable {
		err = status.Error(codes.Unavailable, err.Error())
	}
	responses <- newWriteResponse(seriesIDs, err, er)

	cb(err)
}

func (s *stubAsyncClient) Close() error { return nil }
