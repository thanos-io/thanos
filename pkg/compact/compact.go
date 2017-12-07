package compact

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/prometheus/tsdb/fileutil"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

// Bucket represents a readable bucket of data objects.
type Bucket interface {
	// Iter calls the given function with each found top-level object name in the bucket.
	// It exits if the context is canceled or the function returns an error.
	Iter(ctx context.Context, dir string, f func(name string) error) error

	// Get returns a new reader against the object with the given name.
	Get(ctx context.Context, name string) (io.ReadCloser, error)

	Upload(ctx context.Context, dst, src string) error

	Delete(ctx context.Context, dir string) error
}

// Syncer syncronizes block metas from a bucket into a local directory.
// It sorts them into compaction groups based on equal label sets.
type Syncer struct {
	logger log.Logger
	dir    string
	bkt    Bucket
	mtx    sync.Mutex
	groups map[string]*Group
}

// NewSyncer returns a new Syncer for the given Bucket and directory.
func NewSyncer(logger log.Logger, dir string, bkt Bucket) (*Syncer, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	c := &Syncer{
		logger: logger,
		dir:    dir,
		groups: map[string]*Group{},
		bkt:    bkt,
	}
	return c, errors.Wrap(c.reloadGroups(), "reload groups")
}

func (c *Syncer) reloadGroups() error {
	groupDir := filepath.Join(c.dir, "groups")

	if err := os.RemoveAll(groupDir); err != nil {
		return errors.Wrap(err, "remove old groups")
	}
	if err := os.MkdirAll(groupDir, 0777); err != nil {
		return errors.Wrap(err, "create group dir")
	}

	return iterMetas(c.dir, func(dir string, _ ulid.ULID) error {
		if err := c.add(dir); err != nil {
			return errors.Wrap(err, "add block to groups")
		}
		return nil
	})
}

// SyncMetas synchronizes all meta files from blocks in the bucket into
// the given directory.
func (c *Syncer) SyncMetas(ctx context.Context) error {
	// Read back all block metas so we can detect deleted blocks.
	var (
		local  = map[ulid.ULID]*block.Meta{}
		remote = map[ulid.ULID]struct{}{}
	)
	err := iterMetas(c.dir, func(dir string, id ulid.ULID) error {
		meta, err := block.ReadMetaFile(dir)
		if err != nil {
			return err
		}
		local[id] = meta
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "read local blocks")
	}

	err = c.bkt.Iter(ctx, "", func(name string) error {
		if !strings.HasSuffix(name, "/") {
			return nil
		}
		id, err := ulid.Parse(name[:len(name)-1])
		if err != nil {
			return nil
		}
		remote[id] = struct{}{}

		if _, err = os.Stat(filepath.Join(c.dir, name)); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}

		// ULIDs contain a millisecond timestamp. We do not consider blocks
		// that have been created within the last 5 minutes to avoid races when a block
		// is only partially uploaded.
		// TODO(fabxc): increase the duration for production later. We are probably fine
		// here with 1 hour or more.
		if ulid.Now()-id.Time() < 5*60*1000 {
			fmt.Println("skipping block", id, "due to time", ulid.Now(), id.Time())
			return nil
		}
		level.Debug(c.logger).Log("msg", "download meta", "block", id)

		dir := filepath.Join(c.dir, name)
		tmpdir := dir + ".tmp"

		// TODO(fabxc): make atomic via rename.
		if err := os.MkdirAll(tmpdir, 0777); err != nil {
			return err
		}
		defer os.RemoveAll(tmpdir)

		src := path.Join(name, "meta.json")
		dst := filepath.Join(tmpdir, "meta.json")

		if err := downloadBucketObject(ctx, c.bkt, dst, src); err != nil {
			level.Warn(c.logger).Log("msg", "downloading meta.json failed", "block", id, "err", err)
			return nil
		}
		// The first time we see a new block, we sort it into a group. The compactor
		// may delete a block from a group after having compacted it.
		// This prevents re-compacting the same data. The block remain in the top-level
		// directory until it has actually been deleted from the bucket.
		if err := c.add(filepath.Dir(dst)); err != nil {
			level.Warn(c.logger).Log("msg", "add new block to group", "err", err)
		}
		return renameFile(tmpdir, dir)
	})
	if err != nil {
		return errors.Wrap(err, "retrieve bucket block metas")
	}

	// Delete all local block dirs that no longer exist in the bucket.
	for id, meta := range local {
		if _, ok := remote[id]; ok {
			continue
		}
		if err := os.RemoveAll(filepath.Join(c.dir, id.String())); err != nil {
			level.Warn(c.logger).Log("msg", "delete outdated block", "block", id, "err", err)
		}
		if err := os.RemoveAll(filepath.Join(c.dir, "groups", c.groupKey(meta), id.String())); err != nil {
			level.Warn(c.logger).Log("msg", "delete outdated block from group", "block", id, "err", err)
		}
	}
	return nil
}

func (c *Syncer) groupKey(meta *block.Meta) string {
	return fmt.Sprintf("%x", labels.FromMap(meta.Thanos.Labels).Hash())
}

// add adds the block in the given directory to its respective compaction group.
func (c *Syncer) add(bdir string) error {
	meta, err := block.ReadMetaFile(bdir)
	if err != nil {
		return errors.Wrap(err, "read meta file")
	}
	h := c.groupKey(meta)

	c.mtx.Lock()
	defer c.mtx.Unlock()

	g, ok := c.groups[h]
	if !ok {
		g, err = NewGroup(
			log.With(c.logger, "compactionGroup", h),
			c.bkt,
			filepath.Join(c.dir, "groups", h),
			meta.Thanos.Labels,
		)
		if err != nil {
			return errors.Wrap(err, "create compaction group")
		}
		c.groups[h] = g
	}
	return g.Add(meta)
}

// Groups returns the compaction groups created by the Syncer.
func (c *Syncer) Groups() (res []*Group) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for _, g := range c.groups {
		res = append(res, g)
	}
	return res
}

// GarbageCollect deletes blocks from the bucket if their data is available as part of a
// block with a higher compaction level.
func (c *Syncer) GarbageCollect(ctx context.Context, bkt Bucket) error {
	// Map each block to its highest priority parent. Initial blocks have themselves
	// in their source section, i.e. are their own parent.
	var (
		all     = map[ulid.ULID]*block.Meta{}
		parents = map[ulid.ULID]ulid.ULID{}
	)
	err := iterMetas(c.dir, func(dir string, _ ulid.ULID) error {
		meta, err := block.ReadMetaFile(dir)
		if err != nil {
			return errors.Wrap(err, "read meta")
		}
		all[meta.ULID] = meta
		return nil
	})
	if err != nil {
		return err
	}

	for id, meta := range all {
		// For each source block we contain, check whether we are the highest priotiy parent block.
		for _, sid := range meta.Compaction.Sources {
			pid, ok := parents[sid]
			// No parents for the source block so far.
			if !ok {
				parents[sid] = id
				continue
			}
			pmeta, ok := all[pid]
			if !ok {
				return errors.Errorf("previous parent block %s not found", pid)
			}
			// A parent is of higher priority if its compaction level is higher.
			// If compaction levels are equal, the more recent ULID wins.
			//
			// The ULID recency alone is not sufficient since races, e.g. induced
			// by downtime of garbage collection, may re-compact blocks that are
			// were already compacted into higher-level blocks multiple times.
			level, plevel := meta.Compaction.Level, pmeta.Compaction.Level

			if level > plevel || (level == plevel && id.Compare(pid) > 0) {
				fmt.Println("overwrite parent for", sid, "from", pid, "to", id)
				parents[sid] = id
			}
		}
	}

	// A block can safely be deleted if they are not the highest priority parent for
	// any source block.
	hasChildren := map[ulid.ULID]struct{}{}
	for _, pid := range parents {
		hasChildren[pid] = struct{}{}
	}

	for id := range all {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, ok := hasChildren[id]; ok {
			continue
		}
		// Spawn a new context so we always delete a block in full on shutdown.
		delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

		level.Info(c.logger).Log("msg", "deleting outdated block", "block", id)

		// TODO(fabxc): yet another hack due to the fuzzy directory semantics of our Buckets.
		err := c.bkt.Delete(delCtx, id.String()+"/")
		cancel()
		if err != nil {
			return errors.Wrapf(err, "delete block %s from bucket", id)
		}
	}
	return nil
}

// Group captures a set of blocks that have the same origin labels.
// Those blocks generally contain the same series and can thus efficiently be compacted.
type Group struct {
	logger log.Logger
	mtx    sync.Mutex
	dir    string
	bkt    Bucket
	labels map[string]string
}

// NewGroup returns a new compaction group.
func NewGroup(logger log.Logger, bkt Bucket, dir string, labels map[string]string) (*Group, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	return &Group{
		logger: logger,
		dir:    dir,
		bkt:    bkt,
		labels: labels,
	}, nil
}

// Add the block with the given meta to the group.
func (cg *Group) Add(meta *block.Meta) error {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()

	if !reflect.DeepEqual(cg.labels, meta.Thanos.Labels) {
		return errors.New("block and group labels do not match")
	}

	bdir := filepath.Join(cg.dir, meta.ULID.String())

	if err := os.MkdirAll(bdir, 0777); err != nil {
		return errors.Wrap(err, "create block dir")
	}
	return block.WriteMetaFile(bdir, meta)
}

func (cg *Group) Remove(id ulid.ULID) error {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()

	return os.RemoveAll(filepath.Join(cg.dir, id.String()))
}

// Dir returns the groups directory.
func (cg *Group) Dir() string {
	return cg.dir
}

// Labels returns the labels that all blocks in the group share.
func (cg *Group) Labels() map[string]string {
	return cg.labels
}

// Compact plans and runs a single compaction against the group. The compacted result
// is uploaded into the bucket the blocks were retrived from.
func (cg *Group) Compact(ctx context.Context, comp tsdb.Compactor) error {
	// Planning a compaction works purely based on the meta.json files in our group's dir.
	cg.mtx.Lock()
	plan, err := comp.Plan(cg.dir)
	cg.mtx.Unlock()

	if err != nil {
		return errors.Wrap(err, "plan compaction")
	}
	if len(plan) == 0 {
		return nil
	}
	fmt.Println("will compact", cg.dir)
	for _, p := range plan {
		meta, _ := block.ReadMetaFile(p)
		fmt.Println("   ", meta.ULID, "[", meta.MinTime, ",", meta.MaxTime, "]")
	}

	// Once we have a plan we need to download the actual data. We don't touch
	// the main directory but use an intermediate one instead.
	wdir := filepath.Join(cg.dir, "tmp")

	if err := os.MkdirAll(wdir, 0777); err != nil {
		return errors.Wrap(err, "create working dir")
	}
	defer os.RemoveAll(wdir)

	var compDirs []string
	begin := time.Now()

	for _, b := range plan {
		id := filepath.Base(b)
		dst := filepath.Join(wdir, id)

		if err := downloadBlock(ctx, cg.bkt, id, dst); err != nil {
			return errors.Wrap(err, "download block")
		}
		compDirs = append(compDirs, dst)
	}
	level.Debug(cg.logger).Log("msg", "downloaded blocks",
		"blocks", fmt.Sprintf("%v", plan), "duration", time.Since(begin))

	begin = time.Now()

	id, err := comp.Compact(wdir, compDirs...)
	if err != nil {
		return errors.Wrapf(err, "compact blocks %v", plan)
	}
	level.Debug(cg.logger).Log("msg", "compacted blocks",
		"blocks", fmt.Sprintf("%v", plan), "duration", time.Since(begin))

	bdir := filepath.Join(wdir, id.String())

	os.Remove(filepath.Join(bdir, "tombstones"))

	newMeta, err := block.ReadMetaFile(bdir)
	if err != nil {
		return errors.Wrap(err, "read new meta")
	}
	newMeta.Thanos.Labels = cg.labels

	if err := block.WriteMetaFile(bdir, newMeta); err != nil {
		return errors.Wrap(err, "write new meta")
	}

	begin = time.Now()

	if err = uploadBlock(ctx, cg.bkt, id, bdir); err != nil {
		return errors.Wrap(err, "upload block")
	}
	level.Debug(cg.logger).Log("msg", "uploaded block", "block", id, "duration", time.Since(begin))

	// Delete the blocks we just compacted from the group so they do not get included
	// into the next planning cycle.
	// Eventually the block we just uploaded should get synced into the group again.
	for _, p := range plan {
		if err := os.RemoveAll(p); err != nil {
			level.Error(cg.logger).Log("msg", "remove compacted block dir", "err", err)
		}
	}
	return nil
}

func uploadBlock(ctx context.Context, bkt Bucket, id ulid.ULID, dir string) error {
	err := filepath.Walk(dir, func(src string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}
		target := filepath.Join(id.String(), strings.TrimPrefix(src, dir))
		return bkt.Upload(ctx, src, target)
	})
	if err == nil {
		return nil
	}
	// We don't want to leave partially uploaded directories behind. Cleanup everything related to it
	// and use a uncanceled context.
	bkt.Delete(ctx, dir)
	return err
}

func downloadBlock(ctx context.Context, bkt Bucket, id, dst string) error {
	if err := os.MkdirAll(filepath.Join(dst, "chunks"), 0777); err != nil {
		return err
	}
	objs := []string{"meta.json", "index"}

	err := bkt.Iter(ctx, id+"/chunks/", func(n string) error {
		objs = append(objs, path.Join("chunks", path.Base(n)))
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "get chunk object list")
	}

	for _, o := range objs {
		err := downloadBucketObject(ctx, bkt, filepath.Join(dst, o), path.Join(id, o))
		if err != nil {
			return errors.Wrap(err, "download meta.json")
		}
	}
	return nil
}

func downloadBucketObject(ctx context.Context, bkt Bucket, dst, src string) error {
	r, err := bkt.Get(ctx, src)
	if err != nil {
		return errors.Wrap(err, "create reader")
	}
	defer r.Close()

	f, err := os.Create(dst)
	if err != nil {
		return errors.Wrap(err, "create file")
	}
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(dst)
		}
	}()
	_, err = io.Copy(f, r)
	return err
}

// iterMetas calls f for each meta.json of block directories in dir.
func iterMetas(dir string, f func(dir string, id ulid.ULID) error) error {
	names, err := fileutil.ReadDir(dir)
	if err != nil {
		return errors.Wrap(err, "read dir")
	}
	for _, n := range names {
		id, err := ulid.Parse(n)
		if err != nil {
			continue
		}
		if err := f(filepath.Join(dir, n), id); err != nil {
			return err
		}
	}
	return nil
}

func renameFile(from, to string) error {
	if err := os.RemoveAll(to); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := fileutil.OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}

	if err = fileutil.Fsync(pdir); err != nil {
		pdir.Close()
		return err
	}
	return pdir.Close()
}
