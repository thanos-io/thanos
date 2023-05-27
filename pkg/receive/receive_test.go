// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"testing"
	"time"

	"github.com/go-kit/log"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
)

func TestMain(m *testing.M) {
	custom.TolerantVerifyLeakMain(m)
}

func TestAddingExternalLabelsForTenants(t *testing.T) {
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
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1"},
					ExternalLabels: map[string]string{
						"name1": "value1",
					},
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
					ExternalLabels: map[string]string{
						"name1": "value1",
						"name2": "value2",
						"name3": "value3",
					},
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
					ExternalLabels: map[string]string{
						"name1": "value1",
					},
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
					ExternalLabels: map[string]string{
						"name3": "value3",
						"name2": "value2",
						"name1": "value1",
					},
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
					ExternalLabels: map[string]string{
						"name1": "value1",
						"name2": "value2",
						"name3": "value3",
					},
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant6"},
					ExternalLabels: map[string]string{
						"name6": "value6",
						"name5": "value5",
						"name4": "value4",
					},
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
					ExternalLabels: map[string]string{
						"name3": "value3",
						"name2": "value2",
						"name1": "value1",
					},
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant1"},
					ExternalLabels: map[string]string{
						"name4": "value4",
						"name5": "value5",
						"name6": "value6",
					},
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
					if m.tenants[tenantId] == nil {
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
	initialConfig := []HashringConfig{
		{
			Endpoints: []Endpoint{{Address: "node1"}},
			Tenants:   []string{"tenant1", "tenant2", "tenant3"},
			ExternalLabels: map[string]string{
				"name1": "value1",
				"name2": "value2",
				"name3": "value3",
			},
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
			ExternalLabels: map[string]string{
				"name1": "value1",
				"name2": "value2",
				"name3": "value3",
			},
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
				if m.tenants[tenantId] == nil {
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
				if m.tenants[tenantId] == nil {
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
	initialConfig := []HashringConfig{
		{
			Endpoints: []Endpoint{{Address: "node1"}},
			Tenants:   []string{"tenant1", "tenant2", "tenant3"},
			ExternalLabels: map[string]string{
				"name1": "value1",
				"name2": "value2",
				"name3": "value3",
			},
		},
		{
			Endpoints: []Endpoint{{Address: "node2"}},
			Tenants:   []string{"tenant4", "tenant5", "tenant6"},
			ExternalLabels: map[string]string{
				"name6": "value6",
				"name5": "value5",
				"name4": "value4",
			},
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
					ExternalLabels: map[string]string{
						"name1": "value1",
						"name2": "value2",
						"name3": "value3",
						"name4": "value4",
					},
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant6"},
					ExternalLabels: map[string]string{
						"name4": "value4",
						"name5": "value5",
						"name6": "value6",
						"name7": "value7",
					},
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
					ExternalLabels: map[string]string{
						"name1": "value1",
						"name2": "value2",
					},
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant6"},
					ExternalLabels: map[string]string{
						"name4": "value4",
						"name5": "value5",
					},
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
					ExternalLabels: map[string]string{
						"name1": "value3",
						"name2": "value2",
						"name3": "value3",
					},
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant6"},
					ExternalLabels: map[string]string{
						"name4": "value6",
						"name5": "value5",
						"name6": "value6",
					},
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
					if m.tenants[tenantId] == nil {
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
	initialConfig := []HashringConfig{
		{
			Endpoints: []Endpoint{{Address: "node1"}},
			Tenants:   []string{"tenant1", "tenant2", "tenant3"},
			ExternalLabels: map[string]string{
				"name3": "value3",
				"name2": "value2",
				"name1": "value1",
			},
		},
		{
			Endpoints: []Endpoint{{Address: "node2"}},
			Tenants:   []string{"tenant4", "tenant5", "tenant1"},
			ExternalLabels: map[string]string{
				"name4": "value4",
				"name5": "value5",
				"name6": "value6",
			},
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
					ExternalLabels: map[string]string{
						"name1": "value1",
						"name2": "value2",
						"name3": "value3",
						"name4": "value4",
					},
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant1"},
					ExternalLabels: map[string]string{
						"name4": "value4",
						"name5": "value5",
						"name6": "value6",
					},
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
					ExternalLabels: map[string]string{
						"name1": "value1",
						"name2": "value2",
						"name3": "value3",
					},
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant4", "tenant5", "tenant1"},
					ExternalLabels: map[string]string{
						"name4": "value4",
						"name5": "value5",
						"name6": "value6",
						"name7": "value7",
					},
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
					if m.tenants[tenantId] == nil {
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
	cfg := []HashringConfig{
		{
			Endpoints: []Endpoint{{Address: "node1"}},
			Tenants:   []string{"tenant1"},
			ExternalLabels: map[string]string{
				"replica":   "0",
				"tenant_id": "tenant2",
				"name3":     "value3",
			},
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
				if m.tenants[tenantId] == nil {
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

	for i := 0; i < len(actualStoreClients); i++ {
		testStore := store.TSDBStore{}
		testStore.SetExtLset(expectedExternalLabelSets[i])

		expectedClientLabelSets := labelpb.ZLabelSetsToPromLabelSets(testStore.LabelSet()...)
		setOfExpectedClientLabelSets = append(setOfExpectedClientLabelSets, expectedClientLabelSets)

		actualClientLabelSets := actualStoreClients[i].LabelSets()
		setOfActualClientLabelSets = append(setOfActualClientLabelSets, actualClientLabelSets)
	}

	return setOfExpectedClientLabelSets, setOfActualClientLabelSets
}
