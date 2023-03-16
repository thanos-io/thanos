// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	cmodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/model"
)

type noopMetaFilter struct{}

func (f *noopMetaFilter) Filter(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, synced block.GaugeVec, modified block.GaugeVec) error {
	return nil
}

func TestShardingHandlesDedupCorrectly(t *testing.T) {
	const minTime = `0000-01-01T00:00:00Z`
	const maxTime = `9999-12-31T23:59:59Z`

	mdvMinTime := model.TimeOrDurationValue{}
	testutil.Ok(t, mdvMinTime.Set(minTime))

	mdvMaxTime := model.TimeOrDurationValue{}
	testutil.Ok(t, mdvMaxTime.Set(maxTime))

	monotonicReader := ulid.Monotonic(rand.Reader, 1)

	// Generate a bunch of blocks and then there will be one with all children.
	parentSources := []ulid.ULID{}
	metas := map[ulid.ULID]metadata.Meta{}

	for i := 0; i < 100; i++ {
		ul := ulid.MustNew(0, monotonicReader)

		metas[ul] = metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID: ul,
				Compaction: tsdb.BlockMetaCompaction{
					Sources: []ulid.ULID{ul},
				},
			},
		}

		parentSources = append(parentSources, ul)
	}
	ulidParent := ulid.MustNew(0, monotonicReader)
	metas[ulidParent] = metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID: ulidParent,
			Compaction: tsdb.BlockMetaCompaction{
				Sources: parentSources,
			},
		},
	}

	metaCopy := make(map[ulid.ULID]*metadata.Meta)
	for k, v := range metas {
		v := v
		metaCopy[k] = &v
	}

	// There must be exactly one block in total of both shards.
	filters := metadataFilterFactory(mdvMinTime,
		mdvMaxTime,
		[]*relabel.Config{
			{
				Action:       relabel.HashMod,
				SourceLabels: cmodel.LabelNames{"__block_id"},
				TargetLabel:  "shard",
				Modulus:      2,
			},
			{
				Action:       relabel.Keep,
				SourceLabels: cmodel.LabelNames{"shard"},
				Regex:        relabel.MustNewRegexp("1"),
			},
		},
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		&noopMetaFilter{},
		1, 0,
	)

	shard1 := applyFilters(t, metaCopy, filters)

	metaCopy = make(map[ulid.ULID]*metadata.Meta)
	for k, v := range metas {
		v := v
		metaCopy[k] = &v
	}

	filters = metadataFilterFactory(mdvMinTime,
		mdvMaxTime,
		[]*relabel.Config{
			{
				Action:       relabel.HashMod,
				SourceLabels: cmodel.LabelNames{"__block_id"},
				TargetLabel:  "shard",
				Modulus:      2,
			},
			{
				Action:       relabel.Keep,
				SourceLabels: cmodel.LabelNames{"shard"},
				Regex:        relabel.MustNewRegexp("0"),
			},
		},
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		&noopMetaFilter{},
		1, 0,
	)
	shard2 := applyFilters(t, metaCopy, filters)

	testutil.Equals(t, 1, len(shard1)+len(shard2))
}

func applyFilters(t *testing.T, blocks map[ulid.ULID]*metadata.Meta, filters []block.MetadataFilter) map[ulid.ULID]*metadata.Meta {
	t.Helper()

	r := prometheus.NewRegistry()

	synced := promauto.With(r).NewGaugeVec(prometheus.GaugeOpts{
		Name: "random_metric_name",
	}, []string{"duplicate"})

	modified := promauto.With(r).NewGaugeVec(prometheus.GaugeOpts{
		Name: "not_a_random_metric_name",
	}, []string{"duplicate"})

	for _, f := range filters {
		testutil.Ok(t, f.Filter(context.TODO(), blocks, synced, modified))
	}

	return blocks
}
