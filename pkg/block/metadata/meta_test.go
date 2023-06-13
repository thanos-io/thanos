// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"bytes"
	"io"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
)

func TestMeta_ReadWrite(t *testing.T) {
	t.Run("empty write/read/write", func(t *testing.T) {
		b := bytes.Buffer{}
		testutil.Ok(t, Meta{}.Write(&b))
		testutil.Equals(t, `{
	"ulid": "00000000000000000000000000",
	"minTime": 0,
	"maxTime": 0,
	"stats": {},
	"compaction": {
		"level": 0
	},
	"version": 0,
	"thanos": {
		"labels": null,
		"downsample": {
			"resolution": 0
		},
		"source": "",
		"index_stats": {}
	}
}
`, b.String())
		_, err := Read(io.NopCloser(&b))
		testutil.NotOk(t, err)
		testutil.Equals(t, "unexpected meta file version 0", err.Error())
	})

	t.Run("real write/read/write", func(t *testing.T) {
		b := bytes.Buffer{}
		m1 := Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID:    ulid.MustNew(5, nil),
				MinTime: 2424,
				MaxTime: 134,
				Version: 1,
				Compaction: tsdb.BlockMetaCompaction{
					Sources: []ulid.ULID{ulid.MustNew(1, nil), ulid.MustNew(2, nil)},
					Parents: []tsdb.BlockDesc{
						{
							ULID:    ulid.MustNew(3, nil),
							MinTime: 442,
							MaxTime: 24225,
						},
					},
					Level: 123,
				},
				Stats: tsdb.BlockStats{NumChunks: 14, NumSamples: 245, NumSeries: 4},
			},
			Thanos: Thanos{
				Version: 1,
				Labels:  map[string]string{"ext": "lset1"},
				Source:  ReceiveSource,
				Files: []File{
					{RelPath: "chunks/000001", SizeBytes: 3751},
					{RelPath: "index", SizeBytes: 401},
					{RelPath: "meta.json"},
				},
				Downsample: ThanosDownsample{
					Resolution: 123144,
				},
				IndexStats: IndexStats{
					SeriesMaxSize: 2000,
					ChunkMaxSize:  1000,
				},
			},
		}
		testutil.Ok(t, m1.Write(&b))
		testutil.Equals(t, `{
	"ulid": "00000000050000000000000000",
	"minTime": 2424,
	"maxTime": 134,
	"stats": {
		"numSamples": 245,
		"numSeries": 4,
		"numChunks": 14
	},
	"compaction": {
		"level": 123,
		"sources": [
			"00000000010000000000000000",
			"00000000020000000000000000"
		],
		"parents": [
			{
				"ulid": "00000000030000000000000000",
				"minTime": 442,
				"maxTime": 24225
			}
		]
	},
	"version": 1,
	"thanos": {
		"version": 1,
		"labels": {
			"ext": "lset1"
		},
		"downsample": {
			"resolution": 123144
		},
		"source": "receive",
		"files": [
			{
				"rel_path": "chunks/000001",
				"size_bytes": 3751
			},
			{
				"rel_path": "index",
				"size_bytes": 401
			},
			{
				"rel_path": "meta.json"
			}
		],
		"index_stats": {
			"series_max_size": 2000,
			"chunk_max_size": 1000
		}
	}
}
`, b.String())
		retMeta, err := Read(io.NopCloser(&b))
		testutil.Ok(t, err)
		testutil.Equals(t, m1, *retMeta)
	})

	t.Run("missing external labels write/read/write", func(t *testing.T) {
		b := bytes.Buffer{}
		m1 := Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID:    ulid.MustNew(5, nil),
				MinTime: 2424,
				MaxTime: 134,
				Version: 1,
				Compaction: tsdb.BlockMetaCompaction{
					Sources: []ulid.ULID{ulid.MustNew(1, nil), ulid.MustNew(2, nil)},
					Parents: []tsdb.BlockDesc{
						{
							ULID:    ulid.MustNew(3, nil),
							MinTime: 442,
							MaxTime: 24225,
						},
					},
					Level: 123,
				},
				Stats: tsdb.BlockStats{NumChunks: 14, NumSamples: 245, NumSeries: 4},
			},
			Thanos: Thanos{
				Version: 1,
				Source:  ReceiveSource,
				Files: []File{
					{RelPath: "index", SizeBytes: 1313},
				},
				Downsample: ThanosDownsample{
					Resolution: 123144,
				},
			},
		}
		testutil.Ok(t, m1.Write(&b))
		testutil.Equals(t, `{
	"ulid": "00000000050000000000000000",
	"minTime": 2424,
	"maxTime": 134,
	"stats": {
		"numSamples": 245,
		"numSeries": 4,
		"numChunks": 14
	},
	"compaction": {
		"level": 123,
		"sources": [
			"00000000010000000000000000",
			"00000000020000000000000000"
		],
		"parents": [
			{
				"ulid": "00000000030000000000000000",
				"minTime": 442,
				"maxTime": 24225
			}
		]
	},
	"version": 1,
	"thanos": {
		"version": 1,
		"labels": null,
		"downsample": {
			"resolution": 123144
		},
		"source": "receive",
		"files": [
			{
				"rel_path": "index",
				"size_bytes": 1313
			}
		],
		"index_stats": {}
	}
}
`, b.String())
		retMeta, err := Read(io.NopCloser(&b))
		testutil.Ok(t, err)

		// We expect map to be empty but allocated.
		testutil.Equals(t, map[string]string(nil), m1.Thanos.Labels)
		m1.Thanos.Labels = map[string]string{}
		testutil.Equals(t, m1, *retMeta)
	})
}
