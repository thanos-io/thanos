// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package chunk

import (
	"time"

	"github.com/prometheus/common/model"
)

// PeriodConfig defines the schema and tables to use for a period of time
type PeriodConfig struct {
	From        DayTime             `yaml:"from"`         // used when working with config
	IndexType   string              `yaml:"store"`        // type of index client to use.
	ObjectType  string              `yaml:"object_store"` // type of object client to use; if omitted, defaults to store.
	Schema      string              `yaml:"schema"`
	IndexTables PeriodicTableConfig `yaml:"index"`
	ChunkTables PeriodicTableConfig `yaml:"chunks"`
	RowShards   uint32              `yaml:"row_shards"`
}

// DayTime is a model.Time what holds day-aligned values, and marshals to/from
// YAML in YYYY-MM-DD format.
type DayTime struct {
	model.Time
}

// SchemaConfig contains the config for our chunk index schemas
type SchemaConfig struct {
	Configs []PeriodConfig `yaml:"configs"`

	fileName string
}

// PeriodicTableConfig is configuration for a set of time-sharded tables.
type PeriodicTableConfig struct {
	Prefix string
	Period time.Duration
	Tags   Tags
}
