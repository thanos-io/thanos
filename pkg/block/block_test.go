// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/extprom"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestIsBlockDir(t *testing.T) {
	for _, tc := range []struct {
		input string
		id    ulid.ULID
		bdir  bool
	}{
		{
			input: "",
			bdir:  false,
		},
		{
			input: "something",
			bdir:  false,
		},
		{
			id:    ulid.MustNew(1, nil),
			input: ulid.MustNew(1, nil).String(),
			bdir:  true,
		},
		{
			id:    ulid.MustNew(2, nil),
			input: "/" + ulid.MustNew(2, nil).String(),
			bdir:  true,
		},
		{
			id:    ulid.MustNew(3, nil),
			input: "some/path/" + ulid.MustNew(3, nil).String(),
			bdir:  true,
		},
		{
			input: ulid.MustNew(4, nil).String() + "/something",
			bdir:  false,
		},
	} {
		t.Run(tc.input, func(t *testing.T) {
			id, ok := IsBlockDir(tc.input)
			testutil.Equals(t, tc.bdir, ok)

			if id.Compare(tc.id) != 0 {
				t.Errorf("expected %s got %s", tc.id, id)
				t.FailNow()
			}
		})
	}
}

func TestUpload(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt := objstore.NewInMemBucket()
	b1, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "b", Value: "1"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "val1"}}, 124, metadata.NoneFunc)
	testutil.Ok(t, err)
	testutil.Ok(t, os.MkdirAll(path.Join(tmpDir, "test", b1.String()), os.ModePerm))

	{
		// Wrong dir.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "not-existing"), metadata.NoneFunc)
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.HasSuffix(err.Error(), "/not-existing: no such file or directory"), "")
	}
	{
		// Wrong existing dir (not a block).
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test"), metadata.NoneFunc)
		testutil.NotOk(t, err)
		testutil.Equals(t, "not a block dir: ulid: bad data size when unmarshaling", err.Error())
	}
	{
		// Empty block dir.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), metadata.NoneFunc)
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.HasSuffix(err.Error(), "/meta.json: no such file or directory"), "")
	}
	e2eutil.Copy(t, path.Join(tmpDir, b1.String(), MetaFilename), path.Join(tmpDir, "test", b1.String(), MetaFilename))
	{
		// Missing chunks.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), metadata.NoneFunc)
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.HasSuffix(err.Error(), "/chunks: no such file or directory"), err.Error())
	}
	testutil.Ok(t, os.MkdirAll(path.Join(tmpDir, "test", b1.String(), ChunksDirname), os.ModePerm))
	e2eutil.Copy(t, path.Join(tmpDir, b1.String(), ChunksDirname, "000001"), path.Join(tmpDir, "test", b1.String(), ChunksDirname, "000001"))
	{
		// Missing index file.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), metadata.NoneFunc)
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.HasSuffix(err.Error(), "/index: no such file or directory"), "")
	}
	e2eutil.Copy(t, path.Join(tmpDir, b1.String(), IndexFilename), path.Join(tmpDir, "test", b1.String(), IndexFilename))
	testutil.Ok(t, os.Remove(path.Join(tmpDir, "test", b1.String(), MetaFilename)))
	{
		// Missing meta.json file.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), metadata.NoneFunc)
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.HasSuffix(err.Error(), "/meta.json: no such file or directory"), "")
	}
	e2eutil.Copy(t, path.Join(tmpDir, b1.String(), MetaFilename), path.Join(tmpDir, "test", b1.String(), MetaFilename))
	{
		// Full block.
		testutil.Ok(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), metadata.NoneFunc))
		testutil.Equals(t, 3, len(bkt.Objects()))
		testutil.Equals(t, 3727, len(bkt.Objects()[path.Join(b1.String(), ChunksDirname, "000001")]))
		testutil.Equals(t, 401, len(bkt.Objects()[path.Join(b1.String(), IndexFilename)]))
		testutil.Equals(t, 595, len(bkt.Objects()[path.Join(b1.String(), MetaFilename)]))

		// File stats are gathered.
		testutil.Equals(t, fmt.Sprintf(`{
	"ulid": "%s",
	"minTime": 0,
	"maxTime": 1000,
	"stats": {
		"numSamples": 500,
		"numSeries": 5,
		"numChunks": 5
	},
	"compaction": {
		"level": 1,
		"sources": [
			"%s"
		]
	},
	"version": 1,
	"thanos": {
		"labels": {
			"ext1": "val1"
		},
		"downsample": {
			"resolution": 124
		},
		"source": "test",
		"files": [
			{
				"rel_path": "chunks/000001",
				"size_bytes": 3727
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
			"series_max_size": 16
		}
	}
}
`, b1.String(), b1.String()), string(bkt.Objects()[path.Join(b1.String(), MetaFilename)]))
	}
	{
		// Test Upload is idempotent.
		testutil.Ok(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), metadata.NoneFunc))
		testutil.Equals(t, 3, len(bkt.Objects()))
		testutil.Equals(t, 3727, len(bkt.Objects()[path.Join(b1.String(), ChunksDirname, "000001")]))
		testutil.Equals(t, 401, len(bkt.Objects()[path.Join(b1.String(), IndexFilename)]))
		testutil.Equals(t, 595, len(bkt.Objects()[path.Join(b1.String(), MetaFilename)]))
	}
	{
		// Upload with no external labels should be blocked.
		b2, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
			{{Name: "b", Value: "1"}},
		}, 100, 0, 1000, nil, 124, metadata.NoneFunc)
		testutil.Ok(t, err)
		err = Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b2.String()), metadata.NoneFunc)
		testutil.NotOk(t, err)
		testutil.Equals(t, "empty external labels are not allowed for Thanos block.", err.Error())
		testutil.Equals(t, 3, len(bkt.Objects()))
	}
	{
		// No external labels with UploadPromBlocks.
		b2, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
			{{Name: "b", Value: "1"}},
		}, 100, 0, 1000, nil, 124, metadata.NoneFunc)
		testutil.Ok(t, err)
		err = UploadPromBlock(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b2.String()), metadata.NoneFunc)
		testutil.Ok(t, err)
		testutil.Equals(t, 6, len(bkt.Objects()))
		testutil.Equals(t, 3727, len(bkt.Objects()[path.Join(b2.String(), ChunksDirname, "000001")]))
		testutil.Equals(t, 401, len(bkt.Objects()[path.Join(b2.String(), IndexFilename)]))
		testutil.Equals(t, 574, len(bkt.Objects()[path.Join(b2.String(), MetaFilename)]))
	}
}

func TestDelete(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)
	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt := objstore.NewInMemBucket()
	{
		b1, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
			{{Name: "b", Value: "1"}},
		}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "val1"}}, 124, metadata.NoneFunc)
		testutil.Ok(t, err)
		testutil.Ok(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b1.String()), metadata.NoneFunc))
		testutil.Equals(t, 3, len(bkt.Objects()))

		markedForDeletion := promauto.With(prometheus.NewRegistry()).NewCounter(prometheus.CounterOpts{Name: "test"})
		testutil.Ok(t, MarkForDeletion(ctx, log.NewNopLogger(), bkt, b1, "", markedForDeletion))

		// Full delete.
		testutil.Ok(t, Delete(ctx, log.NewNopLogger(), bkt, b1))
		testutil.Equals(t, 0, len(bkt.Objects()))
	}
	{
		b2, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
			{{Name: "b", Value: "1"}},
		}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "val1"}}, 124, metadata.NoneFunc)
		testutil.Ok(t, err)
		testutil.Ok(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b2.String()), metadata.NoneFunc))
		testutil.Equals(t, 3, len(bkt.Objects()))

		// Remove meta.json and check if delete can delete it.
		testutil.Ok(t, bkt.Delete(ctx, path.Join(b2.String(), MetaFilename)))
		testutil.Ok(t, Delete(ctx, log.NewNopLogger(), bkt, b2))
		testutil.Equals(t, 0, len(bkt.Objects()))
	}
}

func TestMarkForDeletion(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)
	ctx := context.Background()

	tmpDir := t.TempDir()

	for _, tcase := range []struct {
		name      string
		preUpload func(t testing.TB, id ulid.ULID, bkt objstore.Bucket)

		blocksMarked int
	}{
		{
			name:         "block marked for deletion",
			preUpload:    func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {},
			blocksMarked: 1,
		},
		{
			name: "block with deletion mark already, expected log and no metric increment",
			preUpload: func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {
				deletionMark, err := json.Marshal(metadata.DeletionMark{
					ID:           id,
					DeletionTime: time.Now().Unix(),
					Version:      metadata.DeletionMarkVersion1,
				})
				testutil.Ok(t, err)
				testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), metadata.DeletionMarkFilename), bytes.NewReader(deletionMark)))
			},
			blocksMarked: 0,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			id, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
				{{Name: "a", Value: "1"}},
				{{Name: "a", Value: "2"}},
				{{Name: "a", Value: "3"}},
				{{Name: "a", Value: "4"}},
				{{Name: "b", Value: "1"}},
			}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "val1"}}, 124, metadata.NoneFunc)
			testutil.Ok(t, err)

			tcase.preUpload(t, id, bkt)

			testutil.Ok(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, id.String()), metadata.NoneFunc))

			c := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			err = MarkForDeletion(ctx, log.NewNopLogger(), bkt, id, "", c)
			testutil.Ok(t, err)
			testutil.Equals(t, float64(tcase.blocksMarked), promtest.ToFloat64(c))
		})
	}
}

func TestMarkForNoCompact(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)
	ctx := context.Background()

	tmpDir := t.TempDir()

	for _, tcase := range []struct {
		name      string
		preUpload func(t testing.TB, id ulid.ULID, bkt objstore.Bucket)

		blocksMarked int
	}{
		{
			name:         "block marked",
			preUpload:    func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {},
			blocksMarked: 1,
		},
		{
			name: "block with no-compact mark already, expected log and no metric increment",
			preUpload: func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {
				m, err := json.Marshal(metadata.NoCompactMark{
					ID:            id,
					NoCompactTime: time.Now().Unix(),
					Version:       metadata.NoCompactMarkVersion1,
				})
				testutil.Ok(t, err)
				testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), metadata.NoCompactMarkFilename), bytes.NewReader(m)))
			},
			blocksMarked: 0,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			id, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
				{{Name: "a", Value: "1"}},
				{{Name: "a", Value: "2"}},
				{{Name: "a", Value: "3"}},
				{{Name: "a", Value: "4"}},
				{{Name: "b", Value: "1"}},
			}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "val1"}}, 124, metadata.NoneFunc)
			testutil.Ok(t, err)

			tcase.preUpload(t, id, bkt)

			testutil.Ok(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, id.String()), metadata.NoneFunc))

			c := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			err = MarkForNoCompact(ctx, log.NewNopLogger(), bkt, id, metadata.ManualNoCompactReason, "", c)
			testutil.Ok(t, err)
			testutil.Equals(t, float64(tcase.blocksMarked), promtest.ToFloat64(c))
		})
	}
}

func TestMarkForNoDownsample(t *testing.T) {

	defer custom.TolerantVerifyLeak(t)
	ctx := context.Background()

	tmpDir := t.TempDir()

	for _, tcase := range []struct {
		name      string
		preUpload func(t testing.TB, id ulid.ULID, bkt objstore.Bucket)

		blocksMarked int
	}{
		{
			name:         "block marked",
			preUpload:    func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {},
			blocksMarked: 1,
		},
		{
			name: "block with no-downsample mark already, expected log and no metric increment",
			preUpload: func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {
				m, err := json.Marshal(metadata.NoDownsampleMark{
					ID:               id,
					NoDownsampleTime: time.Now().Unix(),
					Version:          metadata.NoDownsampleMarkVersion1,
				})
				testutil.Ok(t, err)
				testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), metadata.NoDownsampleMarkFilename), bytes.NewReader(m)))
			},
			blocksMarked: 0,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			id, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
				{{Name: "a", Value: "1"}},
				{{Name: "a", Value: "2"}},
				{{Name: "a", Value: "3"}},
				{{Name: "a", Value: "4"}},
				{{Name: "b", Value: "1"}},
			}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "val1"}}, 124, metadata.NoneFunc)
			testutil.Ok(t, err)

			tcase.preUpload(t, id, bkt)

			testutil.Ok(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, id.String()), metadata.NoneFunc))

			c := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			err = MarkForNoDownsample(ctx, log.NewNopLogger(), bkt, id, metadata.ManualNoDownsampleReason, "", c)
			testutil.Ok(t, err)
			testutil.Equals(t, float64(tcase.blocksMarked), promtest.ToFloat64(c))
		})
	}
}

// TestHashDownload uploads an empty block to in-memory storage
// and tries to download it to the same dir. It should not try
// to download twice.
func TestHashDownload(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt := objstore.NewInMemBucket()
	r := prometheus.NewRegistry()
	instrumentedBkt := objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", r), "test")

	b1, err := e2eutil.CreateBlockWithTombstone(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "val1"}}, 42, metadata.SHA256Func)
	testutil.Ok(t, err)

	testutil.Ok(t, Upload(ctx, log.NewNopLogger(), instrumentedBkt, path.Join(tmpDir, b1.String()), metadata.SHA256Func))
	testutil.Equals(t, 3, len(bkt.Objects()))

	m, err := DownloadMeta(ctx, log.NewNopLogger(), bkt, b1)
	testutil.Ok(t, err)

	for _, fl := range m.Thanos.Files {
		if fl.RelPath == MetaFilename {
			continue
		}
		testutil.Assert(t, fl.Hash != nil, "expected a hash for %s but got nil", fl.RelPath)
	}

	// Remove the hash from one file to check if we always download it.
	m.Thanos.Files[1].Hash = nil

	metaEncoded := strings.Builder{}
	testutil.Ok(t, m.Write(&metaEncoded))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(b1.String(), MetaFilename), strings.NewReader(metaEncoded.String())))

	// Only downloads MetaFile and IndexFile.
	{
		err = Download(ctx, log.NewNopLogger(), instrumentedBkt, m.ULID, path.Join(tmpDir, b1.String()))
		testutil.Ok(t, err)
		testutil.Ok(t, promtest.GatherAndCompare(r, strings.NewReader(`
		# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
        # TYPE thanos_objstore_bucket_operations_total counter
        thanos_objstore_bucket_operations_total{bucket="test",operation="attributes"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="delete"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="exists"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="get"} 2
        thanos_objstore_bucket_operations_total{bucket="test",operation="get_range"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="iter"} 2
        thanos_objstore_bucket_operations_total{bucket="test",operation="upload"} 3
		`), `thanos_objstore_bucket_operations_total`))
	}

	// Ensures that we always download MetaFile.
	{
		testutil.Ok(t, os.Remove(path.Join(tmpDir, b1.String(), MetaFilename)))
		err = Download(ctx, log.NewNopLogger(), instrumentedBkt, m.ULID, path.Join(tmpDir, b1.String()))
		testutil.Ok(t, err)
		testutil.Ok(t, promtest.GatherAndCompare(r, strings.NewReader(`
		# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
        # TYPE thanos_objstore_bucket_operations_total counter
        thanos_objstore_bucket_operations_total{bucket="test",operation="attributes"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="delete"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="exists"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="get"} 4
        thanos_objstore_bucket_operations_total{bucket="test",operation="get_range"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="iter"} 4
        thanos_objstore_bucket_operations_total{bucket="test",operation="upload"} 3
		`), `thanos_objstore_bucket_operations_total`))
	}

	// Remove chunks => gets redownloaded.
	// Always downloads MetaFile.
	// Finally, downloads the IndexFile since we have removed its hash.
	{
		testutil.Ok(t, os.RemoveAll(path.Join(tmpDir, b1.String(), ChunksDirname)))
		err = Download(ctx, log.NewNopLogger(), instrumentedBkt, m.ULID, path.Join(tmpDir, b1.String()))
		testutil.Ok(t, err)
		testutil.Ok(t, promtest.GatherAndCompare(r, strings.NewReader(`
			# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
			# TYPE thanos_objstore_bucket_operations_total counter
			thanos_objstore_bucket_operations_total{bucket="test",operation="attributes"} 0
			thanos_objstore_bucket_operations_total{bucket="test",operation="delete"} 0
			thanos_objstore_bucket_operations_total{bucket="test",operation="exists"} 0
			thanos_objstore_bucket_operations_total{bucket="test",operation="get"} 7
			thanos_objstore_bucket_operations_total{bucket="test",operation="get_range"} 0
			thanos_objstore_bucket_operations_total{bucket="test",operation="iter"} 6
			thanos_objstore_bucket_operations_total{bucket="test",operation="upload"} 3
			`), `thanos_objstore_bucket_operations_total`))
	}
}

func TestUploadCleanup(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt := objstore.NewInMemBucket()
	b1, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "b", Value: "1"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "val1"}}, 124, metadata.NoneFunc)
	testutil.Ok(t, err)

	{
		errBkt := errBucket{Bucket: bkt, failSuffix: "/index"}

		uploadErr := Upload(ctx, log.NewNopLogger(), errBkt, path.Join(tmpDir, b1.String()), metadata.NoneFunc)
		testutil.Assert(t, errors.Is(uploadErr, errUploadFailed))

		// If upload of index fails, block is deleted.
		testutil.Equals(t, 0, len(bkt.Objects()))
		testutil.Assert(t, len(bkt.Objects()[path.Join(DebugMetas, fmt.Sprintf("%s.json", b1.String()))]) == 0)
	}

	{
		errBkt := errBucket{Bucket: bkt, failSuffix: "/meta.json"}

		uploadErr := Upload(ctx, log.NewNopLogger(), errBkt, path.Join(tmpDir, b1.String()), metadata.NoneFunc)
		testutil.Assert(t, errors.Is(uploadErr, errUploadFailed))

		// If upload of meta.json fails, nothing is cleaned up.
		testutil.Equals(t, 3, len(bkt.Objects()))
		testutil.Assert(t, len(bkt.Objects()[path.Join(b1.String(), ChunksDirname, "000001")]) > 0)
		testutil.Assert(t, len(bkt.Objects()[path.Join(b1.String(), IndexFilename)]) > 0)
		testutil.Assert(t, len(bkt.Objects()[path.Join(b1.String(), MetaFilename)]) > 0)
		testutil.Assert(t, len(bkt.Objects()[path.Join(DebugMetas, fmt.Sprintf("%s.json", b1.String()))]) == 0)
	}
}

var errUploadFailed = errors.New("upload failed")

type errBucket struct {
	objstore.Bucket

	failSuffix string
}

func (eb errBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	err := eb.Bucket.Upload(ctx, name, r)
	if err != nil {
		return err
	}

	if strings.HasSuffix(name, eb.failSuffix) {
		return errUploadFailed
	}
	return nil
}

func TestRemoveMarkForDeletion(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)
	ctx := context.Background()
	tmpDir := t.TempDir()
	for _, testcases := range []struct {
		name           string
		preDelete      func(t testing.TB, id ulid.ULID, bkt objstore.Bucket)
		blocksUnmarked int
	}{
		{
			name: "unmarked block for deletion",
			preDelete: func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {
				deletionMark, err := json.Marshal(metadata.DeletionMark{
					ID:           id,
					DeletionTime: time.Now().Unix(),
					Version:      metadata.DeletionMarkVersion1,
				})
				testutil.Ok(t, err)
				testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), metadata.DeletionMarkFilename), bytes.NewReader(deletionMark)))
			},
			blocksUnmarked: 1,
		},
		{
			name:           "block not marked for deletion, message logged and metric not incremented",
			preDelete:      func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {},
			blocksUnmarked: 0,
		},
	} {
		t.Run(testcases.name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			id, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
				{{Name: "cluster-eu1", Value: "service-1"}},
				{{Name: "cluster-eu1", Value: "service-2"}},
				{{Name: "cluster-eu1", Value: "service-3"}},
				{{Name: "cluster-us1", Value: "service-1"}},
				{{Name: "cluster-us1", Value: "service-2"}},
			}, 100, 0, 1000, labels.Labels{{Name: "region-1", Value: "eu-west"}}, 124, metadata.NoneFunc)
			testutil.Ok(t, err)
			testcases.preDelete(t, id, bkt)
			counter := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			err = RemoveMark(ctx, log.NewNopLogger(), bkt, id, counter, metadata.DeletionMarkFilename)
			testutil.Ok(t, err)
			testutil.Equals(t, float64(testcases.blocksUnmarked), promtest.ToFloat64(counter))
		})
	}
}

func TestRemoveMarkForNoCompact(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)
	ctx := context.Background()
	tmpDir := t.TempDir()
	for _, testCases := range []struct {
		name           string
		preDelete      func(t testing.TB, id ulid.ULID, bkt objstore.Bucket)
		blocksUnmarked int
	}{
		{
			name: "unmarked block for no-compact",
			preDelete: func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {
				m, err := json.Marshal(metadata.NoCompactMark{
					ID:            id,
					NoCompactTime: time.Now().Unix(),
					Version:       metadata.NoCompactMarkVersion1,
				})
				testutil.Ok(t, err)
				testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), metadata.NoCompactMarkFilename), bytes.NewReader(m)))
			},
			blocksUnmarked: 1,
		},
		{
			name:           "block not marked for no-compact, message logged and metric not incremented",
			preDelete:      func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {},
			blocksUnmarked: 0,
		},
	} {
		t.Run(testCases.name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			id, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
				{{Name: "cluster-eu1", Value: "service-1"}},
				{{Name: "cluster-eu1", Value: "service-2"}},
				{{Name: "cluster-eu1", Value: "service-3"}},
				{{Name: "cluster-us1", Value: "service-1"}},
				{{Name: "cluster-us1", Value: "service-2"}},
			}, 100, 0, 1000, labels.Labels{{Name: "region-1", Value: "eu-west"}}, 124, metadata.NoneFunc)
			testutil.Ok(t, err)
			testCases.preDelete(t, id, bkt)
			counter := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			err = RemoveMark(ctx, log.NewNopLogger(), bkt, id, counter, metadata.NoCompactMarkFilename)
			testutil.Ok(t, err)
			testutil.Equals(t, float64(testCases.blocksUnmarked), promtest.ToFloat64(counter))
		})
	}
}

func TestRemoveMmarkForNoDownsample(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)
	ctx := context.Background()
	tmpDir := t.TempDir()
	for _, testCases := range []struct {
		name           string
		preDelete      func(t testing.TB, id ulid.ULID, bkt objstore.Bucket)
		blocksUnmarked int
	}{
		{
			name: "unmarked block for no-downsample",
			preDelete: func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {
				m, err := json.Marshal(metadata.NoDownsampleMark{
					ID:               id,
					NoDownsampleTime: time.Now().Unix(),
					Version:          metadata.NoDownsampleMarkVersion1,
				})
				testutil.Ok(t, err)
				testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), metadata.NoDownsampleMarkFilename), bytes.NewReader(m)))
			},
			blocksUnmarked: 1,
		},
		{
			name:           "block not marked for no-downsample, message logged and metric not incremented",
			preDelete:      func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {},
			blocksUnmarked: 0,
		},
	} {
		t.Run(testCases.name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			id, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
				{{Name: "cluster-eu1", Value: "service-1"}},
				{{Name: "cluster-eu1", Value: "service-2"}},
				{{Name: "cluster-eu1", Value: "service-3"}},
				{{Name: "cluster-us1", Value: "service-1"}},
				{{Name: "cluster-us1", Value: "service-2"}},
			}, 100, 0, 1000, labels.Labels{{Name: "region-1", Value: "eu-west"}}, 124, metadata.NoneFunc)
			testutil.Ok(t, err)
			testCases.preDelete(t, id, bkt)
			counter := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			err = RemoveMark(ctx, log.NewNopLogger(), bkt, id, counter, metadata.NoDownsampleMarkFilename)
			testutil.Ok(t, err)
			testutil.Equals(t, float64(testCases.blocksUnmarked), promtest.ToFloat64(counter))
		})
	}
}
