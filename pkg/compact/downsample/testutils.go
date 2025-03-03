// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func GetMetaLabelsAndChunks(t *testing.T, dir string, id ulid.ULID) (*metadata.Meta, []labels.Labels, [][]chunks.Meta) {
	meta, err := metadata.ReadFromDir(filepath.Join(dir, id.String()))
	testutil.Ok(t, err)

	indexr, err := index.NewFileReader(filepath.Join(dir, id.String(), block.IndexFilename), index.DecodePostingsRaw)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, indexr.Close()) }()

	allPostingsK, allPostingsV := index.AllPostingsKey()
	pall, err := indexr.Postings(context.Background(), allPostingsK, allPostingsV)
	testutil.Ok(t, err)

	var series []storage.SeriesRef
	for pall.Next() {
		series = append(series, pall.At())
	}
	testutil.Ok(t, pall.Err())

	var (
		lbls []labels.Labels
		chks [][]chunks.Meta
	)
	for _, s := range series {
		var builder labels.ScratchBuilder
		var seriesChks []chunks.Meta
		testutil.Ok(t, indexr.Series(s, &builder, &seriesChks))
		lbls = append(lbls, builder.Labels())
		chks = append(chks, seriesChks)
	}

	return meta, lbls, chks
}

func GetAggregateFromChunk(t *testing.T, chunkr *chunks.Reader, c chunks.Meta, aggrType AggrType) []sample {
	chk, _, err := chunkr.ChunkOrIterable(c)
	testutil.Ok(t, err)

	ac, ok := chk.(*AggrChunk)
	testutil.Assert(t, ok)

	var samples []sample

	subChunk, err := ac.Get(aggrType)
	testutil.Ok(t, err)
	it := subChunk.Iterator(nil)
	for valueType := it.Next(); valueType != chunkenc.ValNone; valueType = it.Next() {
		switch valueType {
		case chunkenc.ValFloat:
			t, v := it.At()
			samples = append(samples, sample{t: t, v: v})
		case chunkenc.ValFloatHistogram:
			t, fh := it.AtFloatHistogram(nil)
			samples = append(samples, sample{t: t, fh: fh})
		default:
			t.Fatalf("unexpected value type %v", valueType)
		}
	}

	return samples
}
