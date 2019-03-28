package compact

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/improbable-eng/thanos/pkg/block/metadata"

	"io/ioutil"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/compact/downsample"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

type ResolutionLevel int64

const (
	ResolutionLevelRaw = ResolutionLevel(downsample.ResLevel0)
	ResolutionLevel5m  = ResolutionLevel(downsample.ResLevel1)
	ResolutionLevel1h  = ResolutionLevel(downsample.ResLevel2)
)

var blockTooFreshSentinelError = errors.New("Block too fresh")

// Syncer syncronizes block metas from a bucket into a local directory.
// It sorts them into compaction groups based on equal label sets.
type Syncer struct {
	logger               log.Logger
	reg                  prometheus.Registerer
	bkt                  objstore.Bucket
	syncDelay            time.Duration
	mtx                  sync.Mutex
	blocks               map[ulid.ULID]*metadata.Meta
	blocksMtx            sync.Mutex
	blockSyncConcurrency int
	metrics              *syncerMetrics
	acceptMalformedIndex bool
}

type syncerMetrics struct {
	syncMetas                 prometheus.Counter
	syncMetaFailures          prometheus.Counter
	syncMetaDuration          prometheus.Histogram
	garbageCollectedBlocks    prometheus.Counter
	garbageCollections        prometheus.Counter
	garbageCollectionFailures prometheus.Counter
	garbageCollectionDuration prometheus.Histogram
	compactions               *prometheus.CounterVec
	compactionFailures        *prometheus.CounterVec
}

func newSyncerMetrics(reg prometheus.Registerer) *syncerMetrics {
	var m syncerMetrics

	m.syncMetas = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_sync_meta_total",
		Help: "Total number of sync meta operations.",
	})
	m.syncMetaFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_sync_meta_failures_total",
		Help: "Total number of failed sync meta operations.",
	})
	m.syncMetaDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_compact_sync_meta_duration_seconds",
		Help: "Time it took to sync meta files.",
		Buckets: []float64{
			0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15, 30, 60, 100, 200, 500,
		},
	})

	m.garbageCollectedBlocks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collected_blocks_total",
		Help: "Total number of deleted blocks by compactor.",
	})

	m.garbageCollections = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collection_total",
		Help: "Total number of garbage collection operations.",
	})
	m.garbageCollectionFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collection_failures_total",
		Help: "Total number of failed garbage collection operations.",
	})
	m.garbageCollectionDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_compact_garbage_collection_duration_seconds",
		Help: "Time it took to perform garbage collection iteration.",
		Buckets: []float64{
			0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15, 30, 60, 100, 200, 500,
		},
	})

	m.compactions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_group_compactions_total",
		Help: "Total number of group compactions attempts.",
	}, []string{"group"})
	m.compactionFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_group_compactions_failures_total",
		Help: "Total number of failed group compactions.",
	}, []string{"group"})

	if reg != nil {
		reg.MustRegister(
			m.syncMetas,
			m.syncMetaFailures,
			m.syncMetaDuration,
			m.garbageCollectedBlocks,
			m.garbageCollections,
			m.garbageCollectionFailures,
			m.garbageCollectionDuration,
			m.compactions,
			m.compactionFailures,
		)
	}
	return &m
}

// NewSyncer returns a new Syncer for the given Bucket and directory.
// Blocks must be at least as old as the sync delay for being considered.
func NewSyncer(logger log.Logger, reg prometheus.Registerer, bkt objstore.Bucket, syncDelay time.Duration, blockSyncConcurrency int, acceptMalformedIndex bool) (*Syncer, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Syncer{
		logger:               logger,
		reg:                  reg,
		syncDelay:            syncDelay,
		blocks:               map[ulid.ULID]*metadata.Meta{},
		bkt:                  bkt,
		metrics:              newSyncerMetrics(reg),
		blockSyncConcurrency: blockSyncConcurrency,
		acceptMalformedIndex: acceptMalformedIndex,
	}, nil
}

// SyncMetas synchronizes all meta files from blocks in the bucket into
// the memory.
func (c *Syncer) SyncMetas(ctx context.Context) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	begin := time.Now()

	err := c.syncMetas(ctx)
	if err != nil {
		c.metrics.syncMetaFailures.Inc()
	}
	c.metrics.syncMetas.Inc()
	c.metrics.syncMetaDuration.Observe(time.Since(begin).Seconds())

	return err
}

func (c *Syncer) syncMetas(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	metaIDsChan := make(chan ulid.ULID)
	errChan := make(chan error, c.blockSyncConcurrency)

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for i := 0; i < c.blockSyncConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for id := range metaIDsChan {
				// Check if we already have this block cached locally.
				c.blocksMtx.Lock()
				_, seen := c.blocks[id]
				c.blocksMtx.Unlock()
				if seen {
					continue
				}

				meta, err := c.downloadMeta(workCtx, id)
				if err == blockTooFreshSentinelError {
					continue
				}
				if err != nil {
					errChan <- err
					return
				}

				c.blocksMtx.Lock()
				c.blocks[id] = meta
				c.blocksMtx.Unlock()
			}
		}()
	}

	// Read back all block metas so we can detect deleted blocks.
	remote := map[ulid.ULID]struct{}{}

	err := c.bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}

		remote[id] = struct{}{}

		select {
		case <-ctx.Done():
		case metaIDsChan <- id:
		}

		return nil
	})
	close(metaIDsChan)
	if err != nil {
		return retry(errors.Wrap(err, "retrieve bucket block metas"))
	}

	wg.Wait()
	close(errChan)

	if err := <-errChan; err != nil {
		return retry(err)
	}

	// Delete all local block dirs that no longer exist in the bucket.
	for id := range c.blocks {
		if _, ok := remote[id]; !ok {
			delete(c.blocks, id)
		}
	}

	return nil
}

func (c *Syncer) downloadMeta(ctx context.Context, id ulid.ULID) (*metadata.Meta, error) {
	level.Debug(c.logger).Log("msg", "download meta", "block", id)

	meta, err := block.DownloadMeta(ctx, c.logger, c.bkt, id)
	if err != nil {
		return nil, errors.Wrapf(err, "downloading meta.json for %s", id)
	}

	// ULIDs contain a millisecond timestamp. We do not consider blocks that have been created too recently to
	// avoid races when a block is only partially uploaded. This relates to all blocks, excluding:
	// - repair created blocks
	// - compactor created blocks
	// NOTE: It is not safe to miss "old" block (even that it is newly created) in sync step. Compactor needs to aware of ALL old blocks.
	// TODO(bplotka): https://github.com/improbable-eng/thanos/issues/377
	if ulid.Now()-id.Time() < uint64(c.syncDelay/time.Millisecond) &&
		meta.Thanos.Source != metadata.BucketRepairSource &&
		meta.Thanos.Source != metadata.CompactorSource &&
		meta.Thanos.Source != metadata.CompactorRepairSource {

		level.Debug(c.logger).Log("msg", "block is too fresh for now", "block", id)
		return nil, blockTooFreshSentinelError
	}

	return &meta, nil
}

// GroupKey returns a unique identifier for the group the block belongs to. It considers
// the downsampling resolution and the block's labels.
func GroupKey(meta metadata.Meta) string {
	return groupKey(meta.Thanos.Downsample.Resolution, labels.FromMap(meta.Thanos.Labels))
}

func groupKey(res int64, lbls labels.Labels) string {
	return fmt.Sprintf("%d@%s", res, lbls)
}

// Groups returns the compaction groups for all blocks currently known to the syncer.
// It creates all groups from the scratch on every call.
func (c *Syncer) Groups() (res []*Group, err error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	groups := map[string]*Group{}
	for _, m := range c.blocks {
		g, ok := groups[GroupKey(*m)]
		if !ok {
			g, err = newGroup(
				log.With(c.logger, "compactionGroup", GroupKey(*m)),
				c.bkt,
				labels.FromMap(m.Thanos.Labels),
				m.Thanos.Downsample.Resolution,
				c.acceptMalformedIndex,
				c.metrics.compactions.WithLabelValues(GroupKey(*m)),
				c.metrics.compactionFailures.WithLabelValues(GroupKey(*m)),
				c.metrics.garbageCollectedBlocks,
			)
			if err != nil {
				return nil, errors.Wrap(err, "create compaction group")
			}
			groups[GroupKey(*m)] = g
			res = append(res, g)
		}
		if err := g.Add(m); err != nil {
			return nil, errors.Wrap(err, "add compaction group")
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Key() < res[j].Key()
	})
	return res, nil
}

// GarbageCollect deletes blocks from the bucket if their data is available as part of a
// block with a higher compaction level.
func (c *Syncer) GarbageCollect(ctx context.Context) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	begin := time.Now()

	// Run a separate round of garbage collections for each valid resolution.
	for _, res := range []int64{
		downsample.ResLevel0, downsample.ResLevel1, downsample.ResLevel2,
	} {
		err := c.garbageCollect(ctx, res)
		if err != nil {
			c.metrics.garbageCollectionFailures.Inc()
		}
		c.metrics.garbageCollections.Inc()
		c.metrics.garbageCollectionDuration.Observe(time.Since(begin).Seconds())

		if err != nil {
			return errors.Wrapf(err, "garbage collect resolution %d", res)
		}
	}
	return nil
}

func (c *Syncer) GarbageBlocks(resolution int64) (ids []ulid.ULID, err error) {
	// Map each block to its highest priority parent. Initial blocks have themselves
	// in their source section, i.e. are their own parent.
	parents := map[ulid.ULID]ulid.ULID{}

	for id, meta := range c.blocks {

		// Skip any block that has a different resolution.
		if meta.Thanos.Downsample.Resolution != resolution {
			continue
		}

		// For each source block we contain, check whether we are the highest priority parent block.
		for _, sid := range meta.Compaction.Sources {
			pid, ok := parents[sid]
			// No parents for the source block so far.
			if !ok {
				parents[sid] = id
				continue
			}
			pmeta, ok := c.blocks[pid]
			if !ok {
				return nil, errors.Errorf("previous parent block %s not found", pid)
			}
			// The current block is the higher priority parent for the source if its
			// compaction level is higher than that of the previously set parent.
			// If compaction levels are equal, the more recent ULID wins.
			//
			// The ULID recency alone is not sufficient since races, e.g. induced
			// by downtime of garbage collection, may re-compact blocks that are
			// were already compacted into higher-level blocks multiple times.
			level, plevel := meta.Compaction.Level, pmeta.Compaction.Level

			if level > plevel || (level == plevel && id.Compare(pid) > 0) {
				parents[sid] = id
			}
		}
	}

	// A block can safely be deleted if they are not the highest priority parent for
	// any source block.
	topParents := map[ulid.ULID]struct{}{}
	for _, pid := range parents {
		topParents[pid] = struct{}{}
	}

	for id, meta := range c.blocks {
		// Skip any block that has a different resolution.
		if meta.Thanos.Downsample.Resolution != resolution {
			continue
		}
		if _, ok := topParents[id]; ok {
			continue
		}

		ids = append(ids, id)
	}
	return ids, nil
}

func (c *Syncer) garbageCollect(ctx context.Context, resolution int64) error {
	garbageIds, err := c.GarbageBlocks(resolution)
	if err != nil {
		return err
	}

	for _, id := range garbageIds {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Spawn a new context so we always delete a block in full on shutdown.
		delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

		level.Info(c.logger).Log("msg", "deleting outdated block", "block", id)

		err := block.Delete(delCtx, c.bkt, id)
		cancel()
		if err != nil {
			return retry(errors.Wrapf(err, "delete block %s from bucket", id))
		}

		// Immediately update our in-memory state so no further call to SyncMetas is needed
		/// after running garbage collection.
		delete(c.blocks, id)
		c.metrics.garbageCollectedBlocks.Inc()
	}
	return nil
}

// Group captures a set of blocks that have the same origin labels and downsampling resolution.
// Those blocks generally contain the same series and can thus efficiently be compacted.
type Group struct {
	logger                      log.Logger
	bkt                         objstore.Bucket
	labels                      labels.Labels
	resolution                  int64
	mtx                         sync.Mutex
	blocks                      map[ulid.ULID]*metadata.Meta
	acceptMalformedIndex        bool
	compactions                 prometheus.Counter
	compactionFailures          prometheus.Counter
	groupGarbageCollectedBlocks prometheus.Counter
}

// newGroup returns a new compaction group.
func newGroup(
	logger log.Logger,
	bkt objstore.Bucket,
	lset labels.Labels,
	resolution int64,
	acceptMalformedIndex bool,
	compactions prometheus.Counter,
	compactionFailures prometheus.Counter,
	groupGarbageCollectedBlocks prometheus.Counter,
) (*Group, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	g := &Group{
		logger:                      logger,
		bkt:                         bkt,
		labels:                      lset,
		resolution:                  resolution,
		blocks:                      map[ulid.ULID]*metadata.Meta{},
		acceptMalformedIndex:        acceptMalformedIndex,
		compactions:                 compactions,
		compactionFailures:          compactionFailures,
		groupGarbageCollectedBlocks: groupGarbageCollectedBlocks,
	}
	return g, nil
}

// Key returns an identifier for the group.
func (cg *Group) Key() string {
	return groupKey(cg.resolution, cg.labels)
}

// Add the block with the given meta to the group.
func (cg *Group) Add(meta *metadata.Meta) error {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()

	if !cg.labels.Equals(labels.FromMap(meta.Thanos.Labels)) {
		return errors.New("block and group labels do not match")
	}
	if cg.resolution != meta.Thanos.Downsample.Resolution {
		return errors.New("block and group resolution do not match")
	}
	cg.blocks[meta.ULID] = meta
	return nil
}

// IDs returns all sorted IDs of blocks in the group.
func (cg *Group) IDs() (ids []ulid.ULID) {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()

	for id := range cg.blocks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].Compare(ids[j]) < 0
	})
	return ids
}

// Labels returns the labels that all blocks in the group share.
func (cg *Group) Labels() labels.Labels {
	return cg.labels
}

// Resolution returns the common downsampling resolution of blocks in the group.
func (cg *Group) Resolution() int64 {
	return cg.resolution
}

// Compact plans and runs a single compaction against the group. The compacted result
// is uploaded into the bucket the blocks were retrieved from.
func (cg *Group) Compact(ctx context.Context, dir string, comp tsdb.Compactor) (bool, ulid.ULID, error) {
	subDir := filepath.Join(dir, cg.Key())

	if err := os.RemoveAll(subDir); err != nil {
		return false, ulid.ULID{}, errors.Wrap(err, "clean compaction group dir")
	}
	if err := os.MkdirAll(subDir, 0777); err != nil {
		return false, ulid.ULID{}, errors.Wrap(err, "create compaction group dir")
	}

	shouldRerun, compID, err := cg.compact(ctx, subDir, comp)
	if err != nil {
		cg.compactionFailures.Inc()
	}
	cg.compactions.Inc()

	return shouldRerun, compID, err
}

// Issue347Error is a type wrapper for errors that should invoke repair process for broken block.
type Issue347Error struct {
	err error

	id ulid.ULID
}

func issue347Error(err error, brokenBlock ulid.ULID) Issue347Error {
	return Issue347Error{err: err, id: brokenBlock}
}

func (e Issue347Error) Error() string {
	return e.err.Error()
}

// Issue347Error returns true if the base error is a Issue347Error.
func IsIssue347Error(err error) bool {
	_, ok := errors.Cause(err).(Issue347Error)
	return ok
}

// HaltError is a type wrapper for errors that should halt any further progress on compactions.
type HaltError struct {
	err error
}

func halt(err error) HaltError {
	return HaltError{err: err}
}

func (e HaltError) Error() string {
	return e.err.Error()
}

// IsHaltError returns true if the base error is a HaltError.
func IsHaltError(err error) bool {
	_, ok := errors.Cause(err).(HaltError)
	return ok
}

// RetryError is a type wrapper for errors that should trigger warning log and retry whole compaction loop, but aborting
// current compaction further progress.
type RetryError struct {
	err error
}

func retry(err error) error {
	if IsHaltError(err) {
		return err
	}
	return RetryError{err: err}
}

func (e RetryError) Error() string {
	return e.err.Error()
}

// IsRetryError returns true if the base error is a RetryError.
func IsRetryError(err error) bool {
	_, ok := errors.Cause(err).(RetryError)
	return ok
}

func (cg *Group) areBlocksOverlapping(include *metadata.Meta, excludeDirs ...string) error {
	var (
		metas   []tsdb.BlockMeta
		exclude = map[ulid.ULID]struct{}{}
	)

	for _, e := range excludeDirs {
		id, err := ulid.Parse(filepath.Base(e))
		if err != nil {
			return errors.Wrapf(err, "overlaps find dir %s", e)
		}
		exclude[id] = struct{}{}
	}

	for _, m := range cg.blocks {
		if _, ok := exclude[m.ULID]; ok {
			continue
		}
		metas = append(metas, m.BlockMeta)
	}

	if include != nil {
		metas = append(metas, include.BlockMeta)
	}

	sort.Slice(metas, func(i, j int) bool {
		return metas[i].MinTime < metas[j].MinTime
	})
	if overlaps := tsdb.OverlappingBlocks(metas); len(overlaps) > 0 {
		return errors.Errorf("overlaps found while gathering blocks. %s", overlaps)
	}
	return nil
}

// RepairIssue347 repairs the https://github.com/prometheus/tsdb/issues/347 issue when having issue347Error.
func RepairIssue347(ctx context.Context, logger log.Logger, bkt objstore.Bucket, issue347Err error) error {
	ie, ok := errors.Cause(issue347Err).(Issue347Error)
	if !ok {
		return errors.Errorf("Given error is not an issue347 error: %v", issue347Err)
	}

	level.Info(logger).Log("msg", "Repairing block broken by https://github.com/prometheus/tsdb/issues/347", "id", ie.id, "err", issue347Err)

	tmpdir, err := ioutil.TempDir("", fmt.Sprintf("repair-issue-347-id-%s-", ie.id))
	if err != nil {
		return err
	}

	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			level.Warn(logger).Log("msg", "failed to remote tmpdir", "err", err, "tmpdir", tmpdir)
		}
	}()

	bdir := filepath.Join(tmpdir, ie.id.String())
	if err := block.Download(ctx, logger, bkt, ie.id, bdir); err != nil {
		return retry(errors.Wrapf(err, "download block %s", ie.id))
	}

	meta, err := metadata.Read(bdir)
	if err != nil {
		return errors.Wrapf(err, "read meta from %s", bdir)
	}

	resid, err := block.Repair(logger, tmpdir, ie.id, metadata.CompactorRepairSource, block.IgnoreIssue347OutsideChunk)
	if err != nil {
		return errors.Wrapf(err, "repair failed for block %s", ie.id)
	}

	// Verify repaired id before uploading it.
	if err := block.VerifyIndex(logger, filepath.Join(tmpdir, resid.String(), block.IndexFilename), meta.MinTime, meta.MaxTime); err != nil {
		return errors.Wrapf(err, "repaired block is invalid %s", resid)
	}

	level.Info(logger).Log("msg", "uploading repaired block", "newID", resid)
	if err = block.Upload(ctx, logger, bkt, filepath.Join(tmpdir, resid.String())); err != nil {
		return retry(errors.Wrapf(err, "upload of %s failed", resid))
	}

	level.Info(logger).Log("msg", "deleting broken block", "id", ie.id)

	// Spawn a new context so we always delete a block in full on shutdown.
	delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// TODO(bplotka): Issue with this will introduce overlap that will halt compactor. Automate that (fix duplicate overlaps caused by this).
	if err := block.Delete(delCtx, bkt, ie.id); err != nil {
		return errors.Wrapf(err, "deleting old block %s failed. You need to delete this block manually", ie.id)
	}

	return nil
}

func (cg *Group) compact(ctx context.Context, dir string, comp tsdb.Compactor) (shouldRerun bool, compID ulid.ULID, err error) {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()

	// Check for overlapped blocks.
	if err := cg.areBlocksOverlapping(nil); err != nil {
		return false, ulid.ULID{}, halt(errors.Wrap(err, "pre compaction overlap check"))
	}

	// Planning a compaction works purely based on the meta.json files in our future group's dir.
	// So we first dump all our memory block metas into the directory.
	for _, meta := range cg.blocks {
		bdir := filepath.Join(dir, meta.ULID.String())
		if err := os.MkdirAll(bdir, 0777); err != nil {
			return false, ulid.ULID{}, errors.Wrap(err, "create planning block dir")
		}
		if err := metadata.Write(cg.logger, bdir, meta); err != nil {
			return false, ulid.ULID{}, errors.Wrap(err, "write planning meta file")
		}
	}

	// Plan against the written meta.json files.
	plan, err := comp.Plan(dir)
	if err != nil {
		return false, ulid.ULID{}, errors.Wrap(err, "plan compaction")
	}
	if len(plan) == 0 {
		// Nothing to do.
		return false, ulid.ULID{}, nil
	}

	// Due to #183 we verify that none of the blocks in the plan have overlapping sources.
	// This is one potential source of how we could end up with duplicated chunks.
	uniqueSources := map[ulid.ULID]struct{}{}

	// Once we have a plan we need to download the actual data.
	begin := time.Now()

	for _, pdir := range plan {
		meta, err := metadata.Read(pdir)
		if err != nil {
			return false, ulid.ULID{}, errors.Wrapf(err, "read meta from %s", pdir)
		}

		if cg.Key() != GroupKey(*meta) {
			return false, ulid.ULID{}, halt(errors.Wrapf(err, "compact planned compaction for mixed groups. group: %s, planned block's group: %s", cg.Key(), GroupKey(*meta)))
		}

		for _, s := range meta.Compaction.Sources {
			if _, ok := uniqueSources[s]; ok {
				return false, ulid.ULID{}, halt(errors.Errorf("overlapping sources detected for plan %v", plan))
			}
			uniqueSources[s] = struct{}{}
		}

		id, err := ulid.Parse(filepath.Base(pdir))
		if err != nil {
			return false, ulid.ULID{}, errors.Wrapf(err, "plan dir %s", pdir)
		}

		if meta.ULID.Compare(id) != 0 {
			return false, ulid.ULID{}, errors.Errorf("mismatch between meta %s and dir %s", meta.ULID, id)
		}

		if err := block.Download(ctx, cg.logger, cg.bkt, id, pdir); err != nil {
			return false, ulid.ULID{}, retry(errors.Wrapf(err, "download block %s", id))
		}

		// Ensure all input blocks are valid.
		stats, err := block.GatherIndexIssueStats(cg.logger, filepath.Join(pdir, block.IndexFilename), meta.MinTime, meta.MaxTime)
		if err != nil {
			return false, ulid.ULID{}, errors.Wrapf(err, "gather index issues for block %s", pdir)
		}

		if err := stats.CriticalErr(); err != nil {
			return false, ulid.ULID{}, halt(errors.Wrapf(err, "block with not healthy index found %s; Compaction level %v; Labels: %v", pdir, meta.Compaction.Level, meta.Thanos.Labels))
		}

		if err := stats.Issue347OutsideChunksErr(); err != nil {
			return false, ulid.ULID{}, issue347Error(errors.Wrapf(err, "invalid, but reparable block %s", pdir), meta.ULID)
		}

		if err := stats.PrometheusIssue5372Err(); !cg.acceptMalformedIndex && err != nil {
			return false, ulid.ULID{}, errors.Wrapf(err,
				"block id %s, try running with --debug.accept-malformed-index", id)
		}
	}
	level.Debug(cg.logger).Log("msg", "downloaded and verified blocks",
		"blocks", fmt.Sprintf("%v", plan), "duration", time.Since(begin))

	begin = time.Now()

	compID, err = comp.Compact(dir, plan, nil)
	if err != nil {
		return false, ulid.ULID{}, halt(errors.Wrapf(err, "compact blocks %v", plan))
	}
	if compID == (ulid.ULID{}) {
		// Prometheus compactor found that the compacted block would have no samples.
		level.Info(cg.logger).Log("msg", "compacted block would have no samples, deleting source blocks", "blocks", fmt.Sprintf("%v", plan))
		for _, block := range plan {
			meta, err := metadata.Read(block)
			if err != nil {
				level.Warn(cg.logger).Log("msg", "failed to read meta for block", "block", block)
				continue
			}
			if meta.Stats.NumSamples == 0 {
				if err := cg.deleteBlock(block); err != nil {
					level.Warn(cg.logger).Log("msg", "failed to delete empty block found during compaction", "block", block)
				}
			}
		}
		// Even though this block was empty, there may be more work to do
		return true, ulid.ULID{}, nil
	}
	level.Debug(cg.logger).Log("msg", "compacted blocks",
		"blocks", fmt.Sprintf("%v", plan), "duration", time.Since(begin))

	bdir := filepath.Join(dir, compID.String())

	newMeta, err := metadata.InjectThanos(cg.logger, bdir, metadata.Thanos{
		Labels:     cg.labels.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: cg.resolution},
		Source:     metadata.CompactorSource,
	}, nil)
	if err != nil {
		return false, ulid.ULID{}, errors.Wrapf(err, "failed to finalize the block %s", bdir)
	}

	if err = os.Remove(filepath.Join(bdir, "tombstones")); err != nil {
		return false, ulid.ULID{}, errors.Wrap(err, "remove tombstones")
	}

	// Ensure the output block is valid.
	if err := block.VerifyIndex(cg.logger, filepath.Join(bdir, block.IndexFilename), newMeta.MinTime, newMeta.MaxTime); !cg.acceptMalformedIndex && err != nil {
		return false, ulid.ULID{}, halt(errors.Wrapf(err, "invalid result block %s", bdir))
	}

	// Ensure the output block is not overlapping with anything else.
	if err := cg.areBlocksOverlapping(newMeta, plan...); err != nil {
		return false, ulid.ULID{}, halt(errors.Wrapf(err, "resulted compacted block %s overlaps with something", bdir))
	}

	begin = time.Now()

	if err := block.Upload(ctx, cg.logger, cg.bkt, bdir); err != nil {
		return false, ulid.ULID{}, retry(errors.Wrapf(err, "upload of %s failed", compID))
	}
	level.Debug(cg.logger).Log("msg", "uploaded block", "result_block", compID, "duration", time.Since(begin))

	// Delete the blocks we just compacted from the group and bucket so they do not get included
	// into the next planning cycle.
	// Eventually the block we just uploaded should get synced into the group again (including sync-delay).
	for _, b := range plan {
		if err := cg.deleteBlock(b); err != nil {
			return false, ulid.ULID{}, retry(errors.Wrapf(err, "delete old block from bucket"))
		}
		cg.groupGarbageCollectedBlocks.Inc()
	}

	return true, compID, nil
}

func (cg *Group) deleteBlock(b string) error {
	id, err := ulid.Parse(filepath.Base(b))
	if err != nil {
		return errors.Wrapf(err, "plan dir %s", b)
	}

	if err := os.RemoveAll(b); err != nil {
		return errors.Wrapf(err, "remove old block dir %s", id)
	}

	// Spawn a new context so we always delete a block in full on shutdown.
	delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	level.Info(cg.logger).Log("msg", "deleting compacted block", "old_block", id)
	if err := block.Delete(delCtx, cg.bkt, id); err != nil {
		return errors.Wrapf(err, "delete block %s from bucket", id)
	}
	return nil
}

// BucketCompactor compacts blocks in a bucket.
type BucketCompactor struct {
	logger     log.Logger
	sy         *Syncer
	comp       tsdb.Compactor
	compactDir string
	bkt        objstore.Bucket
}

// NewBucketCompactor creates a new bucket compactor.
func NewBucketCompactor(logger log.Logger, sy *Syncer, comp tsdb.Compactor, compactDir string, bkt objstore.Bucket) *BucketCompactor {
	return &BucketCompactor{
		logger:     logger,
		sy:         sy,
		comp:       comp,
		compactDir: compactDir,
		bkt:        bkt,
	}
}

// Compact runs compaction over bucket.
func (c *BucketCompactor) Compact(ctx context.Context) error {
	// Loop over bucket and compact until there's no work left.
	for {
		// Clean up the compaction temporary directory at the beginning of every compaction loop.
		if err := os.RemoveAll(c.compactDir); err != nil {
			return errors.Wrap(err, "clean up the compaction temporary directory")
		}

		level.Info(c.logger).Log("msg", "start sync of metas")

		if err := c.sy.SyncMetas(ctx); err != nil {
			return errors.Wrap(err, "sync")
		}

		level.Info(c.logger).Log("msg", "start of GC")

		if err := c.sy.GarbageCollect(ctx); err != nil {
			return errors.Wrap(err, "garbage")
		}

		level.Info(c.logger).Log("msg", "start of compaction")

		groups, err := c.sy.Groups()
		if err != nil {
			return errors.Wrap(err, "build compaction groups")
		}
		finishedAllGroups := true
		for _, g := range groups {
			shouldRerunGroup, _, err := g.Compact(ctx, c.compactDir, c.comp)
			if err == nil {
				if shouldRerunGroup {
					finishedAllGroups = false
				}
				continue
			}

			if IsIssue347Error(err) {
				if err := RepairIssue347(ctx, c.logger, c.bkt, err); err == nil {
					finishedAllGroups = false
					continue
				}
			}
			return errors.Wrap(err, "compaction")
		}
		if finishedAllGroups {
			break
		}
	}
	return nil
}
