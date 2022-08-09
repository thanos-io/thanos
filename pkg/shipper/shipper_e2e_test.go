// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package shipper

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/objtesting"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestShipper_SyncBlocks_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t *testing.T, bkt objstore.Bucket) {
		// TODO(GiedriusS): consider switching to BucketWithMetrics() everywhere?
		metrics := prometheus.NewRegistry()
		metricsBucket := objstore.BucketWithMetrics("test", bkt, metrics)

		dir := t.TempDir()

		extLset := labels.FromStrings("prometheus", "prom-1")
		shipper := New(log.NewLogfmtLogger(os.Stderr), nil, dir, metricsBucket, func() labels.Labels { return extLset }, metadata.TestSource, false, false, metadata.NoneFunc)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create 10 new blocks. 9 of them (non compacted) should be actually uploaded.
		var (
			expBlocks    = map[ulid.ULID]struct{}{}
			expFiles     = map[string][]byte{}
			randr        = rand.New(rand.NewSource(0))
			now          = time.Now()
			ids          = []ulid.ULID{}
			maxSyncSoFar int64
		)
		for i := 0; i < 10; i++ {
			id := ulid.MustNew(uint64(i), randr)

			bdir := filepath.Join(dir, id.String())
			tmp := bdir + ".tmp"

			testutil.Ok(t, os.Mkdir(tmp, 0777))

			meta := metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					Version: 1,
					ULID:    id,
					Stats: tsdb.BlockStats{
						NumSamples: 1,
					},
					MinTime: timestamp.FromTime(now.Add(time.Duration(i) * time.Hour)),
					MaxTime: timestamp.FromTime(now.Add((time.Duration(i) * time.Hour) + 1)),

					Compaction: tsdb.BlockMetaCompaction{
						Level: 1,
					},
				},
				Thanos: metadata.Thanos{
					Source: metadata.TestSource,
				},
			}

			// Sixth block is compacted one.
			if i == 5 {
				meta.Compaction.Level = 2
			}

			metab, err := json.Marshal(&meta)
			testutil.Ok(t, err)

			testutil.Ok(t, os.WriteFile(tmp+"/meta.json", metab, 0666))
			testutil.Ok(t, os.WriteFile(tmp+"/index", []byte("indexcontents"), 0666))

			// Running shipper while a block is being written to temp dir should not trigger uploads.
			b, err := shipper.Sync(ctx)
			testutil.Ok(t, err)
			testutil.Equals(t, 0, b)

			shipMeta, err := ReadMetaFile(dir)
			testutil.Ok(t, err)
			if len(shipMeta.Uploaded) == 0 {
				shipMeta.Uploaded = []ulid.ULID{}
			}
			testutil.Equals(t, &Meta{Version: MetaVersion1, Uploaded: ids}, shipMeta)

			testutil.Ok(t, os.MkdirAll(tmp+"/chunks", 0777))
			testutil.Ok(t, os.WriteFile(tmp+"/chunks/0001", []byte("chunkcontents1"), 0666))
			testutil.Ok(t, os.WriteFile(tmp+"/chunks/0002", []byte("chunkcontents2"), 0666))

			testutil.Ok(t, os.Rename(tmp, bdir))

			// After rename sync should upload the block.
			b, err = shipper.Sync(ctx)
			testutil.Ok(t, err)

			if i != 5 {
				ids = append(ids, id)
				maxSyncSoFar = meta.MaxTime
				testutil.Equals(t, 1, b)
			} else {
				// 5 blocks uploaded so far - 5 existence checks & 25 uploads (5 files each).
				testutil.Ok(t, promtest.GatherAndCompare(metrics, strings.NewReader(`
				# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
				# TYPE thanos_objstore_bucket_operations_total counter
				thanos_objstore_bucket_operations_total{bucket="test",operation="attributes"} 0
				thanos_objstore_bucket_operations_total{bucket="test",operation="delete"} 0
				thanos_objstore_bucket_operations_total{bucket="test",operation="exists"} 5
				thanos_objstore_bucket_operations_total{bucket="test",operation="get"} 0
				thanos_objstore_bucket_operations_total{bucket="test",operation="get_range"} 0
				thanos_objstore_bucket_operations_total{bucket="test",operation="iter"} 0
				thanos_objstore_bucket_operations_total{bucket="test",operation="upload"} 20
				`), `thanos_objstore_bucket_operations_total`))
				testutil.Equals(t, 0, b)
			}

			// The external labels must be attached to the meta file on upload.
			meta.Thanos.Labels = extLset.Map()
			meta.Thanos.SegmentFiles = []string{"0001", "0002"}
			meta.Thanos.Files = []metadata.File{
				{RelPath: "chunks/0001", SizeBytes: 14},
				{RelPath: "chunks/0002", SizeBytes: 14},
				{RelPath: "index", SizeBytes: 13},
				{RelPath: "meta.json"},
			}

			buf := bytes.Buffer{}
			testutil.Ok(t, meta.Write(&buf))

			// We will delete the fifth block and do not expect it to be re-uploaded later.
			if i != 4 && i != 5 {
				expBlocks[id] = struct{}{}

				expFiles[id.String()+"/meta.json"] = buf.Bytes()
				expFiles[id.String()+"/index"] = []byte("indexcontents")
				expFiles[id.String()+"/chunks/0001"] = []byte("chunkcontents1")
				expFiles[id.String()+"/chunks/0002"] = []byte("chunkcontents2")
			}
			if i == 4 {
				testutil.Ok(t, block.Delete(ctx, log.NewNopLogger(), bkt, ids[4]))
			}
			// The shipper meta file should show all blocks as uploaded except the compacted one.
			shipMeta, err = ReadMetaFile(dir)
			testutil.Ok(t, err)
			testutil.Equals(t, &Meta{Version: MetaVersion1, Uploaded: ids}, shipMeta)

			// Verify timestamps were updated correctly.
			minTotal, maxSync, err := shipper.Timestamps()
			testutil.Ok(t, err)
			testutil.Equals(t, timestamp.FromTime(now), minTotal)
			testutil.Equals(t, maxSyncSoFar, maxSync)
		}

		for id := range expBlocks {
			ok, _ := bkt.Exists(ctx, path.Join(id.String(), block.MetaFilename))
			testutil.Assert(t, ok, "block %s was not uploaded", id)
		}
		for fn, exp := range expFiles {
			rc, err := bkt.Get(ctx, fn)
			testutil.Ok(t, err)
			act, err := io.ReadAll(rc)
			testutil.Ok(t, err)
			testutil.Ok(t, rc.Close())
			testutil.Equals(t, string(exp), string(act))
		}
		// Verify the fifth block is still deleted by the end.
		ok, err := bkt.Exists(ctx, ids[4].String()+"/meta.json")
		testutil.Ok(t, err)
		testutil.Assert(t, ok == false, "fifth block was reuploaded")
	})
}

func TestShipper_SyncBlocksWithMigrating_e2e(t *testing.T) {
	e2eutil.ForeachPrometheus(t, func(t testing.TB, p *e2eutil.Prometheus) {
		dir := t.TempDir()

		bkt := objstore.NewInMemBucket()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		extLset := labels.FromStrings("prometheus", "prom-1")

		testutil.Ok(t, p.Start())

		logger := log.NewNopLogger()
		upctx, upcancel := context.WithTimeout(ctx, 10*time.Second)
		defer upcancel()
		testutil.Ok(t, p.WaitPrometheusUp(upctx, logger))

		p.DisableCompaction()
		testutil.Ok(t, p.Restart())

		upctx2, upcancel2 := context.WithTimeout(ctx, 10*time.Second)
		defer upcancel2()
		testutil.Ok(t, p.WaitPrometheusUp(upctx2, logger))

		shipper := New(log.NewLogfmtLogger(os.Stderr), nil, dir, bkt, func() labels.Labels { return extLset }, metadata.TestSource, true, false, metadata.NoneFunc)

		// Create 10 new blocks. 9 of them (non compacted) should be actually uploaded.
		var (
			expBlocks = map[ulid.ULID]struct{}{}
			expFiles  = map[string][]byte{}
			randr     = rand.New(rand.NewSource(0))
			now       = time.Now()
			ids       = []ulid.ULID{}
		)
		for i := 0; i < 10; i++ {
			id := ulid.MustNew(uint64(i), randr)

			bdir := filepath.Join(dir, id.String())
			tmp := bdir + ".tmp"

			testutil.Ok(t, os.Mkdir(tmp, 0777))

			meta := metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					Version: 1,
					ULID:    id,
					Stats: tsdb.BlockStats{
						NumSamples: 1,
					},
					MinTime: timestamp.FromTime(now.Add(time.Duration(i) * time.Hour)),
					MaxTime: timestamp.FromTime(now.Add((time.Duration(i) * time.Hour) + 1)),

					Compaction: tsdb.BlockMetaCompaction{
						Level: 1,
					},
				},
				Thanos: metadata.Thanos{
					Source: metadata.TestSource,
				},
			}

			// Fifth block is compacted one.
			if i == 4 {
				meta.Compaction.Level = 2
			}

			metab, err := json.Marshal(&meta)
			testutil.Ok(t, err)

			testutil.Ok(t, os.WriteFile(tmp+"/meta.json", metab, 0666))
			testutil.Ok(t, os.WriteFile(tmp+"/index", []byte("indexcontents"), 0666))

			// Running shipper while a block is being written to temp dir should not trigger uploads.
			b, err := shipper.Sync(ctx)
			testutil.Ok(t, err)
			testutil.Equals(t, 0, b)

			shipMeta, err := ReadMetaFile(dir)
			testutil.Ok(t, err)
			if len(shipMeta.Uploaded) == 0 {
				shipMeta.Uploaded = []ulid.ULID{}
			}
			testutil.Equals(t, &Meta{Version: MetaVersion1, Uploaded: ids}, shipMeta)

			testutil.Ok(t, os.MkdirAll(tmp+"/chunks", 0777))
			testutil.Ok(t, os.WriteFile(tmp+"/chunks/0001", []byte("chunkcontents1"), 0666))
			testutil.Ok(t, os.WriteFile(tmp+"/chunks/0002", []byte("chunkcontents2"), 0666))

			testutil.Ok(t, os.Rename(tmp, bdir))

			// After rename sync should upload the block.
			b, err = shipper.Sync(ctx)
			testutil.Ok(t, err)
			testutil.Equals(t, 1, b)
			ids = append(ids, id)

			// The external labels must be attached to the meta file on upload.
			meta.Thanos.Labels = extLset.Map()
			meta.Thanos.SegmentFiles = []string{"0001", "0002"}
			meta.Thanos.Files = []metadata.File{
				{RelPath: "chunks/0001", SizeBytes: 14},
				{RelPath: "chunks/0002", SizeBytes: 14},
				{RelPath: "index", SizeBytes: 13},
				{RelPath: "meta.json"},
			}

			buf := bytes.Buffer{}
			testutil.Ok(t, meta.Write(&buf))

			// We will delete the fifth block and do not expect it to be re-uploaded later.
			if i != 4 {
				expBlocks[id] = struct{}{}

				expFiles[id.String()+"/meta.json"] = buf.Bytes()
				expFiles[id.String()+"/index"] = []byte("indexcontents")
				expFiles[id.String()+"/chunks/0001"] = []byte("chunkcontents1")
				expFiles[id.String()+"/chunks/0002"] = []byte("chunkcontents2")
			}
			if i == 4 {
				testutil.Ok(t, block.Delete(ctx, log.NewNopLogger(), bkt, ids[4]))
			}
			// The shipper meta file should show all blocks as uploaded except the compacted one.
			shipMeta, err = ReadMetaFile(dir)
			testutil.Ok(t, err)
			testutil.Equals(t, &Meta{Version: MetaVersion1, Uploaded: ids}, shipMeta)

			// Verify timestamps were updated correctly.
			minTotal, maxSync, err := shipper.Timestamps()
			testutil.Ok(t, err)
			testutil.Equals(t, timestamp.FromTime(now), minTotal)
			testutil.Equals(t, meta.MaxTime, maxSync)
		}

		for id := range expBlocks {
			ok, _ := bkt.Exists(ctx, path.Join(id.String(), block.MetaFilename))
			testutil.Assert(t, ok, "block %s was not uploaded", id)
		}
		for fn, exp := range expFiles {
			rc, err := bkt.Get(ctx, fn)
			testutil.Ok(t, err)
			act, err := io.ReadAll(rc)
			testutil.Ok(t, err)
			testutil.Ok(t, rc.Close())
			testutil.Equals(t, string(exp), string(act))
		}
		// Verify the fifth block is still deleted by the end.
		ok, err := bkt.Exists(ctx, ids[4].String()+"/meta.json")
		testutil.Ok(t, err)
		testutil.Assert(t, ok == false, "fifth block was reuploaded")
	})
}

// TestShipper_SyncOverlapBlocks_e2e is a unit test for the functionality by allowOutOfOrderUploads flag. This allows compacted(compaction level greater than 1) blocks to be uploaded despite overlapping time ranges.
func TestShipper_SyncOverlapBlocks_e2e(t *testing.T) {
	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	dir := t.TempDir()

	bkt := objstore.NewInMemBucket()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	extLset := labels.FromStrings("prometheus", "prom-1")

	testutil.Ok(t, p.Start())

	logger := log.NewNopLogger()
	upctx, upcancel := context.WithTimeout(ctx, 10*time.Second)
	defer upcancel()
	testutil.Ok(t, p.WaitPrometheusUp(upctx, logger))

	p.DisableCompaction()
	testutil.Ok(t, p.Restart())

	upctx2, upcancel2 := context.WithTimeout(ctx, 10*time.Second)
	defer upcancel2()
	testutil.Ok(t, p.WaitPrometheusUp(upctx2, logger))

	// Here, the allowOutOfOrderUploads flag is set to true, which allows blocks with overlaps to be uploaded.
	shipper := New(log.NewLogfmtLogger(os.Stderr), nil, dir, bkt, func() labels.Labels { return extLset }, metadata.TestSource, true, true, metadata.NoneFunc)

	// Creating 2 overlapping blocks - both uploaded when OOO uploads allowed.
	var (
		expBlocks = map[ulid.ULID]struct{}{}
		expFiles  = map[string][]byte{}
		randr     = rand.New(rand.NewSource(0))
		ids       = []ulid.ULID{}
	)

	id := make([]ulid.ULID, 2)
	tmp := make([]string, 2)
	m := make([]metadata.Meta, 2)

	for i := 0; i < 2; i++ {
		id[i] = ulid.MustNew(uint64(i), randr)

		bdir := filepath.Join(dir, id[i].String())
		tmp := bdir + ".tmp"

		testutil.Ok(t, os.Mkdir(tmp, 0777))

		m[i] = metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				Version: 1,
				ULID:    id[i],
				Stats: tsdb.BlockStats{
					NumSamples: 1,
				},
				Compaction: tsdb.BlockMetaCompaction{
					Level: 2,
				},
			},
			Thanos: metadata.Thanos{
				Source: metadata.TestSource,
			},
		}
	}

	m[0].BlockMeta.MinTime = 10
	m[0].BlockMeta.MaxTime = 20

	m[1].BlockMeta.MinTime = 15
	m[1].BlockMeta.MaxTime = 17

	for i := 0; i < 2; i++ {
		bdir := filepath.Join(dir, m[i].BlockMeta.ULID.String())
		tmp[i] = bdir + ".tmp"

		metab, err := json.Marshal(&m[i])
		testutil.Ok(t, err)

		testutil.Ok(t, os.WriteFile(tmp[i]+"/meta.json", metab, 0666))
		testutil.Ok(t, os.WriteFile(tmp[i]+"/index", []byte("indexcontents"), 0666))

		// Running shipper while a block is being written to temp dir should not trigger uploads.
		b, err := shipper.Sync(ctx)
		testutil.Ok(t, err)
		testutil.Equals(t, 0, b)

		shipMeta, err := ReadMetaFile(dir)
		testutil.Ok(t, err)
		if len(shipMeta.Uploaded) == 0 {
			shipMeta.Uploaded = []ulid.ULID{}
		}
		testutil.Equals(t, &Meta{Version: MetaVersion1, Uploaded: ids}, shipMeta)

		testutil.Ok(t, os.MkdirAll(tmp[i]+"/chunks", 0777))
		testutil.Ok(t, os.WriteFile(tmp[i]+"/chunks/0001", []byte("chunkcontents1"), 0666))
		testutil.Ok(t, os.WriteFile(tmp[i]+"/chunks/0002", []byte("chunkcontents2"), 0666))

		testutil.Ok(t, os.Rename(tmp[i], bdir))

		// After rename sync should upload the block.
		b, err = shipper.Sync(ctx)
		testutil.Ok(t, err)
		testutil.Equals(t, 1, b)
		ids = append(ids, id[i])

		// The external labels must be attached to the meta file on upload.
		m[i].Thanos.Labels = extLset.Map()
		m[i].Thanos.SegmentFiles = []string{"0001", "0002"}
		m[i].Thanos.Files = []metadata.File{
			{RelPath: "chunks/0001", SizeBytes: 14},
			{RelPath: "chunks/0002", SizeBytes: 14},
			{RelPath: "index", SizeBytes: 13},
			{RelPath: "meta.json"},
		}

		buf := bytes.Buffer{}
		testutil.Ok(t, m[i].Write(&buf))

		expBlocks[id[i]] = struct{}{}
		expFiles[id[i].String()+"/meta.json"] = buf.Bytes()
		expFiles[id[i].String()+"/index"] = []byte("indexcontents")
		expFiles[id[i].String()+"/chunks/0001"] = []byte("chunkcontents1")
		expFiles[id[i].String()+"/chunks/0002"] = []byte("chunkcontents2")

		// The shipper meta file should show all blocks as uploaded except the compacted one.
		shipMeta, err = ReadMetaFile(dir)
		testutil.Ok(t, err)
		testutil.Equals(t, &Meta{Version: MetaVersion1, Uploaded: ids}, shipMeta)
	}

	for id := range expBlocks {
		ok, _ := bkt.Exists(ctx, path.Join(id.String(), block.MetaFilename))
		testutil.Assert(t, ok, "block %s was not uploaded", id)
	}

	for fn, exp := range expFiles {
		rc, err := bkt.Get(ctx, fn)
		testutil.Ok(t, err)
		act, err := io.ReadAll(rc)
		testutil.Ok(t, err)
		testutil.Ok(t, rc.Close())
		testutil.Equals(t, string(exp), string(act))
	}
}
