// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storetestutil

import (
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type TestClient struct {
	storepb.StoreClient

	Name string

	ExtLset                     []labels.Labels
	MinTime, MaxTime            int64
	Shardable                   bool
	WithoutReplicaLabelsEnabled bool
	IsLocalStore                bool
	StoreTSDBInfos              []infopb.TSDBInfo
	StoreFilterNotMatches       bool
}

func (c TestClient) LabelSets() []labels.Labels             { return c.ExtLset }
func (c TestClient) TimeRange() (mint, maxt int64)          { return c.MinTime, c.MaxTime }
func (c TestClient) TSDBInfos() []infopb.TSDBInfo           { return c.StoreTSDBInfos }
func (c TestClient) SupportsSharding() bool                 { return c.Shardable }
func (c TestClient) SupportsWithoutReplicaLabels() bool     { return c.WithoutReplicaLabelsEnabled }
func (c TestClient) String() string                         { return c.Name }
func (c TestClient) Addr() (string, bool)                   { return c.Name, c.IsLocalStore }
func (c TestClient) Matches(matches []*labels.Matcher) bool { return !c.StoreFilterNotMatches }
