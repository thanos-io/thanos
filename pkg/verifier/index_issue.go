package verifier

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"hash/crc32"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

const IndexIssueID = "index_issue"

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type vChunks struct {
	id   ulid.ULID
	chks []vChunk
}

type vChunk struct {
	chunks.Meta
	duplicated bool
}

// IndexIssue verifies any known index issue.
// It rewrites the problematic blocks while fixing repairable inconsistencies.
// If the replacement was created successfully it is uploaded to the bucket and the input
// block is deleted.
// NOTE: This also verifies all indexes against chunks mismatches and duplicates.
func IndexIssue(ctx context.Context, logger log.Logger, bkt objstore.Bucket, backupBkt objstore.Bucket, repair bool) error {
	level.Info(logger).Log("msg", "started verifying issue", "with-repair", repair, "issue", IndexIssueID)

	blockOutsiders := map[string][]vChunks{}
	err := bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}

		tmpdir, err := ioutil.TempDir("", fmt.Sprintf("index-issue-block-%s", id))
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmpdir)

		indexPath := filepath.Join(tmpdir, "index")
		err = objstore.DownloadFile(ctx, bkt, path.Join(id.String(), "index"), indexPath)
		if err != nil {
			return errors.Wrapf(err, "download index file %s", path.Join(id.String(), "index"))
		}
		meta, err := block.DownloadMeta(ctx, bkt, id)
		if err != nil {
			return errors.Wrapf(err, "download meta file %s", id)
		}

		stats, outsiders, err := block.GatherIndexIssueStats(indexPath, meta.MinTime, meta.MaxTime)
		if err != nil {
			return errors.Wrapf(err, "gather index issues %s", id)
		}

		err = stats.ErrSummary()
		if err == nil {
			if outsiders.Len() > 0 {
				level.Warn(logger).Log("msg", "detected outsiders", "id", id, "issue", IndexIssueID, "num", outsiders.Len())
				for lset, o := range outsiders {
					var vChks []vChunk
					for _, ch := range o {
						vChks = append(vChks, vChunk{Meta: ch})
					}

					blockOutsiders[lset] = append(blockOutsiders[lset], vChunks{
						id:   id,
						chks: vChks,
					})
				}
			}
			return nil
		}

		level.Warn(logger).Log("msg", "detected issue", "id", id, "err", err, "issue", IndexIssueID)

		if !repair {
			// Only verify.
			return nil
		}

		if stats.OutOfOrderSum > stats.ExactSum {
			level.Warn(logger).Log("msg", "detected overlaps are not entirely by duplicated chunks. We are able to repair only duplicates", "id", id, "issue", IndexIssueID)
		}

		level.Info(logger).Log("msg", "repairing block", "id", id, "issue", IndexIssueID)

		if meta.Thanos.Downsample.Resolution > 0 {
			return errors.New("cannot repair downsampled blocks")
		}

		resid, err := block.Repair(tmpdir, meta.ULID, map[string][]chunks.Meta{})
		if err != nil {
			return errors.Wrapf(err, "repair failed for block %s", id)
		}

		// Verify repaired block before uploading it.
		stats, outsiders, err = block.GatherIndexIssueStats(filepath.Join(tmpdir, resid.String(), "index"), meta.MinTime, meta.MaxTime)
		if err != nil {
			return errors.Wrapf(err, "gather index issues %s for repaired block", id)
		}

		if outsiders.Len() > 0 {
			level.Warn(logger).Log("msg", "detected outsiders", "id", id, "issue", IndexIssueID, "num", outsiders.Len())
			for lset, o := range outsiders {
				var vChks []vChunk
				for _, ch := range o {
					vChks = append(vChks, vChunk{Meta: ch})
				}

				blockOutsiders[lset] = append(blockOutsiders[lset], vChunks{
					id:   id,
					chks: vChks,
				})
			}
		}

		if stats.ErrSummary() != nil {
			return errors.Wrap(err, "repaired block is invalid")
		}

		level.Info(logger).Log("msg", "create repaired block", "newID", resid, "issue", IndexIssueID)

		err = objstore.UploadDir(ctx, bkt, filepath.Join(tmpdir, resid.String()), resid.String())
		if err != nil {
			return errors.Wrapf(err, "upload of %s failed", resid)
		}
		if err := SafeDelete(ctx, bkt, backupBkt, id); err != nil {
			return errors.Wrapf(err, "safe deleting old block %s failed", id)
		}

		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "verify iter, issue %s", IndexIssueID)
	}

	if repair {
		// Verify if we can delete outsiders safely (there is an exact duplicate somewhere).
		err = bkt.Iter(ctx, "", func(name string) error {
			id, ok := block.IsBlockDir(name)
			if !ok {
				return nil
			}

			tmpdir, err := ioutil.TempDir("", fmt.Sprintf("index-issue-block-%s", id))
			if err != nil {
				return err
			}
			defer os.RemoveAll(tmpdir)

			// Get index file.
			indexPath := filepath.Join(tmpdir, "index")
			err = objstore.DownloadFile(ctx, bkt, path.Join(id.String(), "index"), indexPath)
			if err != nil {
				return errors.Wrapf(err, "download index file %s", path.Join(id.String(), "index"))
			}

			r, err := index.NewFileReader(indexPath)
			if err != nil {
				return errors.Wrap(err, "open index file")
			}
			defer r.Close()

			p, err := r.Postings(index.AllPostingsKey())
			if err != nil {
				return errors.Wrap(err, "get all postings")
			}
			var (
				lset labels.Labels
				chks []chunks.Meta
			)
			for p.Next() {
				sid := p.At()
				if err := r.Series(sid, &lset, &chks); err != nil {
					return errors.Wrap(err, "read series")
				}

				for _, c := range chks {
					for i, outsider := range blockOutsiders[lset.String()] {
						if outsider.id.Compare(id) == 0 {
							// Repair should already fix internal dups.
							continue
						}

						for j, ch := range outsider.chks {
							// We have a match. Is it duplicate?
							if c.MinTime == ch.MinTime && c.MaxTime == ch.MaxTime {
								ca := crc32.Checksum(ch.Chunk.Bytes(), castagnoli)
								cb := crc32.Checksum(c.Chunk.Bytes(), castagnoli)
								if ca == cb {
									// Duplicate. We can remove it in further steps.
									blockOutsiders[lset.String()][i].chks[j].duplicated = true
								}
							}
						}
					}
				}
			}
			if p.Err() != nil {
				return errors.Wrap(err, "walk postings")
			}

			return nil
		})
		if err != nil {
			return errors.Wrapf(err, "outsider verify dups iter, issue %s", IndexIssueID)
		}

		toDelete := map[ulid.ULID]map[string][]chunks.Meta{}
		total := map[ulid.ULID]int{}
		for lset, outsider := range blockOutsiders {
			for _, vchunk := range outsider {
				dups := 0
				for _, ch := range vchunk.chks {
					total[vchunk.id]++
					if ch.duplicated {
						dups++
						m, ok := toDelete[vchunk.id]
						if !ok {
							m = map[string][]chunks.Meta{}
						}
						m[lset] = append(m[lset], ch.Meta)
						toDelete[vchunk.id] = m
					}
				}
			}
		}

		tmpdir, err := ioutil.TempDir("", "index-issue-block-all")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmpdir)

		for id, totalNum := range total {
			level.Warn(logger).Log("msg", "deleting outsiders to delete", "id", id, "toDelete", len(toDelete[id]), "outsiders", totalNum, "issue", IndexIssueID)

			resid, err := block.Repair(tmpdir, id, toDelete[id])
			if err != nil {
				return errors.Wrapf(err, "repair failed for block %s", id)
			}

			meta, err := block.ReadMetaFile(filepath.Join(tmpdir, resid.String()))
			if err != nil {
				return errors.Wrap(err, "read meta file")
			}

			// Verify repaired block before uploading it.
			err = block.VerifyIndex(filepath.Join(tmpdir, resid.String(), "index"), meta.MinTime, meta.MaxTime)
			if err != nil {
				return errors.Wrap(err, "repaired outsider block is invalid")
			}

			level.Info(logger).Log("msg", "create repaired outsider block", "newID", resid, "issue", IndexIssueID)

			err = objstore.UploadDir(ctx, bkt, filepath.Join(tmpdir, resid.String()), resid.String())
			if err != nil {
				return errors.Wrapf(err, "upload of %s failed", resid)
			}
			if err := SafeDelete(ctx, bkt, backupBkt, id); err != nil {
				return errors.Wrapf(err, "safe deleting old block %s failed", id)
			}
		}
	}

	level.Info(logger).Log("msg", "verified issue", "with-repair", repair, "issue", IndexIssueID)
	return nil
}
