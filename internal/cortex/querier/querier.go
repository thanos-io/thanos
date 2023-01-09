// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package querier

import (
	"time"

	"github.com/thanos-io/thanos/internal/cortex/util/flagext"
)

// Config contains the configuration require to create a querier
type Config struct {
	MaxConcurrent             int           `yaml:"max_concurrent"`
	Timeout                   time.Duration `yaml:"timeout"`
	Iterators                 bool          `yaml:"iterators"`
	BatchIterators            bool          `yaml:"batch_iterators"`
	IngesterStreaming         bool          `yaml:"ingester_streaming"`
	IngesterMetadataStreaming bool          `yaml:"ingester_metadata_streaming"`
	MaxSamples                int           `yaml:"max_samples"`
	QueryIngestersWithin      time.Duration `yaml:"query_ingesters_within"`
	QueryStoreForLabels       bool          `yaml:"query_store_for_labels_enabled"`
	AtModifierEnabled         bool          `yaml:"at_modifier_enabled"`
	EnablePerStepStats        bool          `yaml:"per_step_stats_enabled"`

	// QueryStoreAfter the time after which queries should also be sent to the store and not just ingesters.
	QueryStoreAfter    time.Duration `yaml:"query_store_after"`
	MaxQueryIntoFuture time.Duration `yaml:"max_query_into_future"`

	// The default evaluation interval for the promql engine.
	// Needs to be configured for subqueries to work as it is the default
	// step if not specified.
	DefaultEvaluationInterval time.Duration `yaml:"default_evaluation_interval"`

	// Directory for ActiveQueryTracker. If empty, ActiveQueryTracker will be disabled and MaxConcurrent will not be applied (!).
	// ActiveQueryTracker logs queries that were active during the last crash, but logs them on the next startup.
	// However, we need to use active query tracker, otherwise we cannot limit Max Concurrent queries in the PromQL
	// engine.
	ActiveQueryTrackerDir string `yaml:"active_query_tracker_dir"`
	// LookbackDelta determines the time since the last sample after which a time
	// series is considered stale.
	LookbackDelta time.Duration `yaml:"lookback_delta"`

	// Blocks storage only.
	StoreGatewayAddresses string       `yaml:"store_gateway_addresses"`
	StoreGatewayClient    ClientConfig `yaml:"store_gateway_client"`

	SecondStoreEngine        string       `yaml:"second_store_engine"`
	UseSecondStoreBeforeTime flagext.Time `yaml:"use_second_store_before_time"`

	ShuffleShardingIngestersLookbackPeriod time.Duration `yaml:"shuffle_sharding_ingesters_lookback_period"`
}
