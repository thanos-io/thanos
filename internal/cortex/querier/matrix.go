// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package querier

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/thanos/internal/cortex/chunk"
	"github.com/thanos-io/thanos/internal/cortex/querier/series"
	"github.com/thanos-io/thanos/internal/cortex/util"
)

func mergeChunks(chunks []chunk.Chunk, from, through model.Time) chunkenc.Iterator {
	samples := make([][]model.SamplePair, 0, len(chunks))
	for _, c := range chunks {
		ss, err := c.Samples(from, through)
		if err != nil {
			return series.NewErrIterator(err)
		}

		samples = append(samples, ss)
	}

	merged := util.MergeNSampleSets(samples...)
	return series.NewConcreteSeriesIterator(series.NewConcreteSeries(nil, merged))
}
