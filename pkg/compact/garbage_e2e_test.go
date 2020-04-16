package compact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/objtesting"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestSyncer_GarbageCollect_e2e(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)

	objtesting.ForeachStore(t, func(t *testing.T, bkt objstore.Bucket) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		// Generate 10 source block metas and construct higher level blocks
		// that are higher compactions of them.
		var metas []*metadata.Meta
		var ids []ulid.ULID

		for i := 0; i < 10; i++ {
			var m metadata.Meta

			m.Version = 1
			m.ULID = ulid.MustNew(uint64(i), nil)
			m.Compaction.Sources = []ulid.ULID{m.ULID}
			m.Compaction.Level = 1

			ids = append(ids, m.ULID)
			metas = append(metas, &m)
		}

		var m1 metadata.Meta
		m1.Version = 1
		m1.ULID = ulid.MustNew(100, nil)
		m1.Compaction.Level = 2
		m1.Compaction.Sources = ids[:4]
		m1.Thanos.Downsample.Resolution = 0

		var m2 metadata.Meta
		m2.Version = 1
		m2.ULID = ulid.MustNew(200, nil)
		m2.Compaction.Level = 2
		m2.Compaction.Sources = ids[4:8] // last two source IDs are not part of a level 2 block.
		m2.Thanos.Downsample.Resolution = 0

		var m3 metadata.Meta
		m3.Version = 1
		m3.ULID = ulid.MustNew(300, nil)
		m3.Compaction.Level = 3
		m3.Compaction.Sources = ids[:9] // last source ID is not part of level 3 block.
		m3.Thanos.Downsample.Resolution = 0

		var m4 metadata.Meta
		m4.Version = 1
		m4.ULID = ulid.MustNew(400, nil)
		m4.Compaction.Level = 2
		m4.Compaction.Sources = ids[9:10] // covers the last block but is a different resolution. Must not trigger deletion.
		m4.Thanos.Downsample.Resolution = 1000

		dm := metadata.NewDeletionMarker(nil, logger, objstore.WithNoopInstr(bkt))
		rdyToBeIgnoredDm := metadata.NewDeletionMarker(nil, logger, objstore.WithNoopInstr(bkt))
		rdyToBeIgnoredDm.TimeNow = func() time.Time { return time.Now().Add(-40 * time.Hour) }

		var m5 metadata.Meta
		m5.Version = 1
		m5.ULID = ulid.MustNew(500, nil)
		m5.Compaction.Level = 2
		m5.Compaction.Sources = ids[4:8] // Duplicate, but will have deletion mark.
		m5.Thanos.Downsample.Resolution = 0
		testutil.Ok(t, dm.MarkForDeletion(ctx, m5.ULID, "I WANNA"))

		var m6 metadata.Meta
		m6.Version = 1
		m6.ULID = ulid.MustNew(600, nil)
		m6.Compaction.Level = 2
		m6.Compaction.Sources = ids[:4] // Duplicate, but will have deletion mark.
		m6.Thanos.Downsample.Resolution = 0
		testutil.Ok(t, rdyToBeIgnoredDm.MarkForDeletion(ctx, m6.ULID, "NO REASON"))

		// Create all blocks in the bucket.
		for _, m := range append(metas, &m1, &m2, &m3, &m4, &m5, &m6) {
			fmt.Println("create", m.ULID)
			var buf bytes.Buffer
			testutil.Ok(t, json.NewEncoder(&buf).Encode(&m))
			testutil.Ok(t, bkt.Upload(ctx, path.Join(m.ULID.String(), metadata.MetaFilename), &buf))
		}

		// Add 3 partial blocks for fun, second and third with deletion mark.
		partialBlock1 := ulid.MustNew(700, nil)
		testutil.Ok(t, bkt.Upload(ctx, path.Join(partialBlock1.String(), block.IndexFilename), strings.NewReader("yolo")))

		partialBlock2 := ulid.MustNew(800, nil)
		testutil.Ok(t, bkt.Upload(ctx, path.Join(partialBlock2.String(), block.IndexFilename), strings.NewReader("yolo2")))
		testutil.Ok(t, dm.MarkForDeletion(ctx, partialBlock2, ""))

		partialBlock3 := ulid.MustNew(900, nil)
		testutil.Ok(t, bkt.Upload(ctx, path.Join(partialBlock3.String(), block.IndexFilename), strings.NewReader("yolo3")))
		testutil.Ok(t, rdyToBeIgnoredDm.MarkForDeletion(ctx, partialBlock3, ""))

		// 2 partial blocks with just deletion markers.
		testutil.Ok(t, dm.MarkForDeletion(ctx, ulid.MustNew(1000, nil), ""))
		testutil.Ok(t, rdyToBeIgnoredDm.MarkForDeletion(ctx, ulid.MustNew(2000, nil), ""))

		// Mimick the filters we have on production.
		duplicateBlocksFilter := block.NewDeduplicateFilter()
		ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, objstore.WithNoopInstr(bkt), 24*time.Hour)
		metaFetcher, err := block.NewMetaFetcher(logger, 32, objstore.WithNoopInstr(bkt), "", nil, []block.MetadataFilter{
			block.NewConsistencyDelayMetaFilter(logger, 30*time.Minute, nil),
			ignoreDeletionMarkFilter,
			duplicateBlocksFilter,
		}, nil)

		testutil.Ok(t, err)

		reg := extprom.NewMockedRegisterer()
		gc := NewGarbage(logger, nil, metadata.NewDeletionMarker(reg, logger, objstore.WithNoopInstr(bkt)))
		markedForDeletion := reg.Collectors[0].(*prometheus.CounterVec)
		sy, err := NewSyncer(
			logger,
			nil,
			bkt,
			metaFetcher,
			1,
			false,
			false,
			gc,
			ignoreDeletionMarkFilter,
			duplicateBlocksFilter,
		)
		testutil.Ok(t, err)

		// Do one initial synchronization with the bucket.
		dups, marks, err := sy.SyncMetas(ctx)
		testutil.Ok(t, err)

		testutil.Equals(t, 0.0, promtest.ToFloat64(gc.metrics.garbageCollections))
		testutil.Equals(t, 0.0, promtest.ToFloat64(gc.metrics.garbageCollectedBlocks))
		testutil.Equals(t, 4, promtest.CollectAndCount(markedForDeletion))
		testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.BetweenCompactDuplicateReason))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.PostCompactDuplicateDeletion))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.RetentionDeletion))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.PartialForTooLongDeletion))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(gc.metrics.garbageCollectionFailures))

		testutil.Ok(t, gc.Collect(ctx, dups, marks))
		testutil.Equals(t, 1.0, promtest.ToFloat64(gc.metrics.garbageCollections))
		testutil.Equals(t, 11.0, promtest.ToFloat64(gc.metrics.garbageCollectedBlocks))
		testutil.Equals(t, 4, promtest.CollectAndCount(markedForDeletion))
		testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.BetweenCompactDuplicateReason))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.PostCompactDuplicateDeletion))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.RetentionDeletion))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.PartialForTooLongDeletion))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(gc.metrics.garbageCollectionFailures))

		var rem []ulid.ULID
		err = bkt.Iter(ctx, "", func(n string) error {
			id := ulid.MustParse(n[:len(n)-1])
			deletionMarkFile := path.Join(id.String(), metadata.DeletionMarkFilename)
			exists, err := bkt.Exists(ctx, deletionMarkFile)
			if err != nil {
				return err
			}
			if !exists {
				rem = append(rem, id)
			}
			return nil
		})
		testutil.Ok(t, err)

		sort.Slice(rem, func(i, j int) bool {
			return rem[i].Compare(rem[j]) < 0
		})

		// Only the level 3 block, the last source block in both resolutions should be left.
		testutil.Equals(t, []ulid.ULID{metas[9].ULID, m3.ULID, m4.ULID}, rem)

		// After another sync the changes should also be reflected in the local groups.
		dups, marks, err = sy.SyncMetas(ctx)
		testutil.Ok(t, err)

		testutil.Ok(t, gc.Collect(ctx, dups, marks))
		testutil.Equals(t, 1.0, promtest.ToFloat64(gc.metrics.garbageCollections))
		testutil.Equals(t, 0.0, promtest.ToFloat64(gc.metrics.garbageCollectedBlocks))
		testutil.Equals(t, 4, promtest.CollectAndCount(markedForDeletion))
		testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.BetweenCompactDuplicateReason))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.PostCompactDuplicateDeletion))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.RetentionDeletion))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.PartialForTooLongDeletion))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(gc.metrics.garbageCollectionFailures))

		// Only the level 3 block, the last source block in both resolutions should be left.
		groups, err := sy.Groups()
		testutil.Ok(t, err)

		testutil.Equals(t, "0@17241709254077376921", groups[0].Key())
		testutil.Equals(t, []ulid.ULID{metas[9].ULID, m3.ULID}, groups[0].IDs())
		testutil.Equals(t, "1000@17241709254077376921", groups[1].Key())
		testutil.Equals(t, []ulid.ULID{m4.ULID}, groups[1].IDs())
	})
}
