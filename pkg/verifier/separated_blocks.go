package verifier

import (
	"context"
	"time"

	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
)

const SeparatedBlocksID = "separated_blocks"

// TODO(bplotka): Move that to flag?
var MinBlockSize = 2 * time.Hour

// SeparatedBlocks verifies and fixes (if repair is true) the issue with block being missed and assumed that no block is
// there for given time range.
// Example:
//  - a block with range 0 - 100
//  - another ovelapped block with range 20-50
//  - bigger block is missing exactly these sources from smaller block to fill its range.
// Various bugs could introduce these blocks, including broken compaction for Prometheus 2.2.0.
func SeparatedBlocks(ctx context.Context, logger log.Logger, bkt objstore.Bucket, backupBkt objstore.Bucket, repair bool) error {
	level.Info(logger).Log("msg", "started verifying issue", "with-repair", repair, "issue", SeparatedBlocksID)

	overlaps, err := fetchOverlaps(ctx, bkt)
	if err != nil {
		return errors.Wrap(err, SeparatedBlocksID)
	}

	if len(overlaps) == 0 {
		// All good, no issue.
		return nil
	}

	var blocksPairsToBeMerged []tsdb.BlockMeta
	// Check for separated pairs.
	for _, groupOverlaps := range overlaps {
		for _, o := range groupOverlaps {
			if len(o) != 2 {
				level.Warn(logger).Log("msg", "detected unrelated overlap. duplicated_compaction issue might fix it", "issue", SeparatedBlocksID)
				continue
			}

			a := o[0]
			b := o[1]
			if a.MaxTime-a.MinTime < b.MaxTime-b.MinTime {
				return errors.Errorf("Unexpected order of blocks. Bigger is after smaller. Smaller: %s Bigger: %s", a.ULID, b.ULID)
			}

			sources := map[ulid.ULID]struct{}{}
			for _, s := range a.Compaction.Sources {
				if _, ok := sources[s]; ok {
					return errors.Errorf("Sources duplicate in %s sources %v", a.ULID, a.Compaction.Sources)
				}
				sources[s] = struct{}{}
			}
			for _, s := range b.Compaction.Sources {
				if _, ok := sources[s]; ok {
					return errors.Errorf("Sources duplicate between %s and %s sources %v, %v", a.ULID, b.ULID, a.Compaction.Sources, b.Compaction.Sources)
				}
				sources[s] = struct{}{}
			}

			r := time.Duration((a.MaxTime-a.MinTime)/1000) * time.Second
			if time.Duration(len(sources))*MinBlockSize > r {
				return errors.Errorf("All from %s and %s together compose range longer than bigger block %s range: %s", a.ULID, b.ULID, a.ULID, r.String())
			}

			level.Warn(logger).Log("msg", "detected pair to be merged", "a", a.ULID, "b", b.ULID, "issue", SeparatedBlocksID)
			blocksPairsToBeMerged = append(blocksPairsToBeMerged, a, b)
		}
	}

	if repair {
		comp, err := tsdb.NewLeveledCompactor(nil, logger, []int64{int64(2 * time.Hour / time.Millisecond)}, nil)
		if err != nil {
			return errors.Wrap(err, "create fake compactor")
		}

		for i := 0; i < len(blocksPairsToBeMerged); i += 2 {
			err := func() error {
				a := blocksPairsToBeMerged[i]
				b := blocksPairsToBeMerged[i+1]

				tmpdir, err := ioutil.TempDir("", fmt.Sprintf("seperated-blocks-%s-and-%s-", a.ULID, b.ULID))
				if err != nil {
					return err
				}
				defer os.RemoveAll(tmpdir)

				err = block.Download(ctx, bkt, a.ULID, path.Join(tmpdir, a.ULID.String()))
				if err != nil {
					return errors.Wrapf(err, "download block %s", a.ULID)
				}

				err = block.Download(ctx, bkt, b.ULID, path.Join(tmpdir, b.ULID.String()))
				if err != nil {
					return errors.Wrapf(err, "download block %s", b.ULID)
				}

				// We need to do certain hack. We need our resulted block to have time range of bigger block "a":
				// Compact assumes:
				// 		MinTime: blocks[0].MinTime,
				// 		MaxTime: blocks[len(blocks)-1].MaxTime,
				// so it is enough for us to intentionally malform meta file of "b" with MaxTime of "a".
				bmeta, err := block.ReadMetaFile(path.Join(tmpdir, b.ULID.String()))
				if err != nil {
					return errors.Wrapf(err, "read meta %s", b.ULID)
				}
				bmeta.MaxTime = a.MaxTime

				err = block.WriteMetaFile(path.Join(tmpdir, b.ULID.String()), bmeta)
				if err != nil {
					return errors.Wrapf(err, "write meta %s", b.ULID)
				}

				level.Info(logger).Log("msg", "compacting two blocks together", "a", a.ULID, "b", b.ULID, "issue", SeparatedBlocksID)
				resid, err := comp.Compact(tmpdir, path.Join(tmpdir, a.ULID.String()), path.Join(tmpdir, b.ULID.String()))
				if err != nil {
					return errors.Wrapf(err, "merge blocks %s and %s", a.ULID, b.ULID)
				}

				level.Info(logger).Log("msg", "uploading merged block", "newID", resid, "issue", SeparatedBlocksID)
				err = objstore.UploadDir(ctx, bkt, filepath.Join(tmpdir, resid.String()), resid.String())
				if err != nil {
					return errors.Wrapf(err, "upload of %s failed", resid)
				}

				level.Info(logger).Log("msg", "safe deleting block a", "id", a.ULID, "issue", SeparatedBlocksID)
				if err := SafeDelete(ctx, bkt, backupBkt, a.ULID); err != nil {
					return errors.Wrapf(err, "safe deleting old block %s failed", a.ULID)
				}

				level.Info(logger).Log("msg", "safe deleting block b", "id", b.ULID, "issue", SeparatedBlocksID)
				if err := SafeDelete(ctx, bkt, backupBkt, b.ULID); err != nil {
					return errors.Wrapf(err, "safe deleting old block %s failed", b.ULID)
				}

				return nil
			}()
			if err != nil {
				return err
			}
		}

	}

	level.Info(logger).Log("msg", "verified issue", "with-repair", repair, "issue", SeparatedBlocksID)
	return nil
}
