package dedup

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/query"
)

type TimeWindow struct {
	MinTime int64
	MaxTime int64
}

func (tw *TimeWindow) String() string {
	return fmt.Sprintf("[%d, %d]", tw.MinTime, tw.MaxTime)
}

func NewTimeWindow(minTime, maxTime int64) *TimeWindow {
	return &TimeWindow{MinTime: minTime, MaxTime: maxTime}
}

// Group blocks under the same time window from different replicas
type BlockGroup struct {
	window *TimeWindow
	blocks []*metadata.Meta
}

func (g *BlockGroup) String() string {
	builder := strings.Builder{}
	builder.WriteString("[")
	for i, b := range g.blocks {
		if i != 0 {
			builder.WriteString(",")
		}
		builder.WriteString(b.ULID.String())
	}
	builder.WriteString("]")
	return fmt.Sprintf("BlockGroup{window: %s, blocks: %s}", g.window, builder.String())
}

func NewBlockGroup(window *TimeWindow, blocks []*metadata.Meta) *BlockGroup {
	return &BlockGroup{window: window, blocks: blocks}
}

type BlockGroups []*BlockGroup

func NewBlockGroups(replicas Replicas) BlockGroups {
	if len(replicas) == 0 {
		return nil
	}
	blocks := make([]*metadata.Meta, 0)
	for _, v := range replicas {
		blocks = append(blocks, v.Blocks...)
	}
	// Prefer to use larger time window to group blocks, best effort to not break the compacted blocks
	// If two blocks with same duration, prefer to handle the one with smaller minTime firstly
	sort.Slice(blocks, func(i, j int) bool {
		d1 := blocks[i].MaxTime - blocks[i].MinTime
		d2 := blocks[j].MaxTime - blocks[j].MinTime
		if d1 == d2 {
			return blocks[i].MinTime < blocks[j].MinTime
		}
		return d1 > d2
	})
	groups := make(BlockGroups, 0)
	covered := make([]*TimeWindow, 0)
	for _, b := range blocks {
		tw := getUncoveredTimeWindow(covered, b)
		if tw == nil {
			continue
		}
		groups = append(groups, getBlockGroup(blocks, tw))
		covered = append(covered, tw)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].window.MinTime < groups[j].window.MinTime
	})
	return groups
}

func getUncoveredTimeWindow(covered []*TimeWindow, b *metadata.Meta) *TimeWindow {
	minTime := b.MinTime
	maxTime := b.MaxTime
	for _, v := range covered {
		if minTime >= v.MinTime && minTime < v.MaxTime {
			minTime = v.MaxTime
		}
		if maxTime > v.MinTime && maxTime <= v.MaxTime {
			maxTime = v.MinTime
		}
		if minTime >= maxTime {
			return nil
		}
	}
	return NewTimeWindow(minTime, maxTime)
}

func getBlockGroup(blocks []*metadata.Meta, tw *TimeWindow) *BlockGroup {
	target := make([]*metadata.Meta, 0)
	for _, b := range blocks {
		if b.MaxTime <= tw.MinTime || b.MinTime >= tw.MaxTime {
			continue
		}
		target = append(target, b)
	}
	return NewBlockGroup(tw, target)
}

type ReplicaMerger struct {
	logger       log.Logger
	metrics      *DedupMetrics
	bkt          objstore.Bucket
	dir          string
	replicaLabel string
}

func NewReplicaMerger(logger log.Logger, metrics *DedupMetrics, bkt objstore.Bucket, dir string, replicaLabel string) *ReplicaMerger {
	return &ReplicaMerger{
		logger:       logger,
		metrics:      metrics,
		bkt:          bkt,
		dir:          dir,
		replicaLabel: replicaLabel,
	}
}

func (rm *ReplicaMerger) Merge(ctx context.Context, replicas Replicas) error {
	groups := rm.plan(ctx, replicas)

	for _, group := range groups {
		if err := rm.prepare(ctx, group); err != nil {
			return errors.Wrapf(err, "prepare phase of group: %s", group)
		}
		id, err := rm.merge(ctx, group)
		if err != nil {
			return errors.Wrapf(err, "merge phase of group: %s", group)
		}
		if err := rm.upload(ctx, group, id); err != nil {
			return errors.Wrapf(err, "upload phase of group: %s", group)
		}
		if err := rm.clean(ctx, group, id); err != nil {
			return errors.Wrapf(err, "clean phase of group: %s", group)
		}
	}
	return nil
}

func (rm *ReplicaMerger) plan(ctx context.Context, replicas Replicas) BlockGroups {
	if len(replicas) < 2 {
		return nil
	}
	groups := NewBlockGroups(replicas)
	target := make(BlockGroups, 0, len(groups))
	for _, group := range groups {
		// if the group only includes less than 2 blocks, then skip it
		if len(group.blocks) < 2 {
			continue
		}
		target = append(target, group)
	}
	return target
}

func (rm *ReplicaMerger) prepare(ctx context.Context, group *BlockGroup) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	mCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errChan := make(chan error, len(group.blocks))

	for _, b := range group.blocks {
		wg.Add(1)
		go func(b *metadata.Meta) {
			defer wg.Done()
			rm.metrics.syncBlocks.WithLabelValues(rm.bkt.Name()).Inc()
			begin := time.Now()
			err := rm.download(mCtx, b)
			rm.metrics.syncBlockDuration.WithLabelValues(rm.bkt.Name()).Observe(time.Since(begin).Seconds())
			if err != nil {
				rm.metrics.syncMetaFailures.WithLabelValues(rm.bkt.Name(), b.ULID.String()).Inc()
				errChan <- err
			}
		}(b)
	}

	wg.Wait()
	close(errChan)

	if err := <-errChan; err != nil {
		return err
	}
	return nil
}

func (rm *ReplicaMerger) download(ctx context.Context, b *metadata.Meta) error {
	blockDir := filepath.Join(rm.dir, b.ULID.String())
	if err := rm.deleteLocalBlock(&b.ULID); err != nil {
		return compact.Retry(errors.Wrapf(err, "clean up block dir: %s", blockDir))
	}
	err := block.Download(ctx, rm.logger, rm.bkt, b.ULID, blockDir)
	if err != nil {
		rm.metrics.operateRemoteStorageFailures.WithLabelValues("get", rm.bkt.Name(), b.ULID.String()).Inc()
		return compact.Retry(errors.Wrapf(err, "download block %s", b.ULID))
	}
	level.Debug(rm.logger).Log("msg", "downloaded block from remote bucket", "block", b.ULID)
	return nil
}

func (rm *ReplicaMerger) merge(ctx context.Context, group *BlockGroup) (*ulid.ULID, error) {
	if len(group.blocks) == 0 {
		return nil, nil
	}
	baseBlock := group.blocks[0]
	readers := make([]*BlockReader, 0, len(group.blocks))

	defer func() {
		for _, reader := range readers {
			if err := reader.Close(); err != nil {
				level.Warn(rm.logger).Log("msg", "failed to close block reader", "err", err)
			}
		}
	}()

	for _, b := range group.blocks {
		blockDir := filepath.Join(rm.dir, b.ULID.String())
		reader, err := NewBlockReader(rm.logger, blockDir)
		if err != nil {
			if err := reader.Close(); err != nil {
				level.Warn(rm.logger).Log("msg", "failed to close block reader", "err", err)
			}
			return nil, err
		}
		readers = append(readers, reader)
	}

	newId := ulid.MustNew(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano())))
	newMeta := rm.newMeta(baseBlock, newId, group.window)
	blockDir := filepath.Join(rm.dir, newMeta.ULID.String())

	if err := rm.write(readers, blockDir, newMeta, group.window); err != nil {
		return nil, err
	}
	return &newId, nil
}

func (rm *ReplicaMerger) newMeta(baseMeta *metadata.Meta, newId ulid.ULID, tw *TimeWindow) *metadata.Meta {
	newMeta := *baseMeta
	newMeta.ULID = newId
	newMeta.MinTime = tw.MinTime
	newMeta.MaxTime = tw.MaxTime
	newSources := make([]ulid.ULID, 0, len(newMeta.Compaction.Sources))
	var hasOldId bool
	for _, source := range newMeta.Compaction.Sources {
		if source == baseMeta.ULID {
			hasOldId = true
			continue
		}
		newSources = append(newSources, source)
	}
	if hasOldId {
		newSources = append(newSources, newId)
	}
	newMeta.Compaction.Sources = newSources
	newMeta.Thanos.Labels[rm.replicaLabel] = AggReplicaLabel
	return &newMeta
}

func (rm *ReplicaMerger) write(readers []*BlockReader, blockDir string, meta *metadata.Meta, tw *TimeWindow) error {
	symbols, err := rm.getMergedSymbols(readers)
	if err != nil {
		return err
	}
	writer, err := downsample.NewStreamedBlockWriter(blockDir, symbols, rm.logger, *meta)
	if err != nil {
		return err
	}

	buf := make([]*SampleReader, len(readers), len(readers))

	running := true
	for running {
		running = false

		for i, reader := range readers {
			if buf[i] != nil {
				running = true
				continue
			}
			hasNext := reader.postings.Next()
			if !hasNext {
				continue
			}
			var lset labels.Labels
			var chks []chunks.Meta
			if err := reader.ir.Series(reader.postings.At(), &lset, &chks); err != nil {
				return err
			}
			buf[i] = NewSampleReader(reader.cr, lset, chks)
			running = true
		}

		cs, err := rm.getMergedChunkSeries(buf, tw)
		if err != nil {
			return err
		}

		if cs == nil {
			continue
		}

		if err := writer.WriteSeries(cs.lset, cs.chks); err != nil {
			return err
		}

		for i, v := range buf {
			if v == nil {
				continue
			}
			if labels.Compare(v.lset, cs.lset) == 0 {
				buf[i] = nil
			}
		}
	}

	if err := writer.Close(); err != nil {
		return err
	}
	return nil
}

func (rm *ReplicaMerger) getMergedSymbols(readers []*BlockReader) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	for _, reader := range readers {
		symbols, err := reader.Symbols()
		if err != nil {
			return nil, err
		}
		for k := range symbols {
			if _, ok := result[k]; !ok {
				result[k] = struct{}{}
			}
		}
	}
	return result, nil
}

func (rm *ReplicaMerger) getMergedChunkSeries(readers []*SampleReader, tw *TimeWindow) (*ChunkSeries, error) {
	buf := make([]*SampleReader, len(readers))
	copy(buf, readers)

	sort.Slice(buf, func(i, j int) bool {
		if buf[i] == nil {
			return false
		}
		if buf[j] == nil {
			return true
		}
		return labels.Compare(buf[i].lset, buf[j].lset) < 0
	})

	if buf[0] == nil {
		return nil, nil
	}

	lset := buf[0].lset
	samples, err := buf[0].Read(tw)
	if err != nil {
		return nil, err
	}
	it := query.NewDedupSeriesIterator(NewSampleIterator(nil), NewSampleIterator(samples))
	for i := 1; i < len(buf); i++ {
		if buf[i] == nil {
			break
		}
		if labels.Compare(buf[i].lset, lset) != 0 {
			break
		}
		ss, err := buf[i].Read(tw)
		if err != nil {
			return nil, err
		}
		if len(ss) == 0 {
			continue
		}
		it = query.NewDedupSeriesIterator(it, NewSampleIterator(ss))
	}

	return NewSampleSeries(lset, rm.getMergedSamples(it)).ToChunkSeries()
}

func (rm *ReplicaMerger) getMergedSamples(it storage.SeriesIterator) []*Sample {
	samples := make([]*Sample, 0)
	for it.Next() {
		t, v := it.At()
		samples = append(samples, NewSample(t, v))
	}
	return samples
}

func (rm *ReplicaMerger) upload(ctx context.Context, group *BlockGroup, newId *ulid.ULID) error {
	blockDir := filepath.Join(rm.dir, newId.String())
	if err := block.VerifyIndex(rm.logger, filepath.Join(blockDir, block.IndexFilename), group.window.MinTime, group.window.MaxTime); err != nil {
		return errors.Wrapf(err, "agg block index not valid: %s", newId)
	}
	level.Debug(rm.logger).Log("msg", "verified agg block index", "block", newId, "dir", blockDir)
	if err := block.Upload(ctx, rm.logger, rm.bkt, blockDir); err != nil {
		rm.metrics.operateRemoteStorageFailures.WithLabelValues("upload", rm.bkt.Name(), newId.String()).Inc()
		return compact.Retry(errors.Wrapf(err, "upload of %s failed", newId))
	}
	level.Debug(rm.logger).Log("msg", "uploaded agg block to remote bucket", "block", newId, "dir", blockDir)
	return nil
}

func (rm *ReplicaMerger) clean(ctx context.Context, group *BlockGroup, newId *ulid.ULID) error {
	// delete blocks in remote storage
	for _, b := range group.blocks {
		if b.MaxTime > group.window.MaxTime {
			continue
		}
		if err := rm.deleteRemoteBlock(&b.ULID); err != nil {
			return compact.Retry(errors.Wrapf(err, "delete block %s from bucket", b.ULID.String()))
		}
	}

	// delete blocks in local storage
	if err := rm.deleteLocalBlock(newId); err != nil {
		return compact.Retry(errors.Wrapf(err, "delete agg block: %s", newId.String()))
	}

	for _, b := range group.blocks {
		if err := rm.deleteLocalBlock(&b.ULID); err != nil {
			return compact.Retry(errors.Wrapf(err, "delete merged block: %s", newId.String()))
		}
	}

	return nil
}

func (rm *ReplicaMerger) deleteRemoteBlock(id *ulid.ULID) error {
	if id == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := block.Delete(ctx, rm.bkt, *id); err != nil {
		rm.metrics.operateRemoteStorageFailures.WithLabelValues("delete", rm.bkt.Name(), id.String()).Inc()
		return err
	}
	level.Debug(rm.logger).Log("msg", "deleted remote block", "block", id.String())
	return nil
}

func (rm *ReplicaMerger) deleteLocalBlock(id *ulid.ULID) error {
	if id == nil {
		return nil
	}
	blockDir := filepath.Join(rm.dir, id.String())
	if err := os.RemoveAll(blockDir); err != nil {
		rm.metrics.operateLocalStorageFailures.WithLabelValues("delete", id.String()).Inc()
		return err
	}
	level.Debug(rm.logger).Log("msg", "deleted local block", "block", blockDir)
	return nil
}
