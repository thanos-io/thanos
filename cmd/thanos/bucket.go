package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"text/template"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/compact"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerBucket(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "inspect metric data in an object storage bucket")

	gcsBucket := cmd.Flag("gcs-bucket", "Google Cloud Storage bucket name for stored blocks.").
		PlaceHolder("<bucket>").String()

	s3Config := s3.RegisterS3Params(cmd)

	check := cmd.Command("check", "verify all blocks in the bucket")

	checkRepair := check.Flag("repair", "attempt to repair blocks for which issues were detected").
		Short('r').Default("false").Bool()

	m[name+" check"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer) error {
		bkt, closeFn, err := getBucketClient(gcsBucket, *s3Config, reg)
		if err != nil {
			return err
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		defer closeFn()

		return runBucketCheck(logger, bkt, *checkRepair)
	}

	ls := cmd.Command("ls", "list all blocks in the bucket")

	lsOutput := ls.Flag("ouput", "format in which to print each block's information; may be 'json' or custom template").
		Short('o').Default("").String()

	m[name+" ls"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer) error {
		bkt, closeFn, err := getBucketClient(gcsBucket, *s3Config, reg)
		if err != nil {
			return err
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		defer closeFn()

		return runBucketList(bkt, *lsOutput)
	}
}

func runBucketCheck(logger log.Logger, bkt objstore.Bucket, repair bool) error {
	var all []ulid.ULID

	ctx := context.Background()

	err := bkt.Iter(ctx, "", func(name string) error {
		if id, err := ulid.Parse(name[:len(name)-1]); err == nil {
			all = append(all, id)
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "iter bucket")
	}
	level.Info(logger).Log("msg", "start verifying blocks", "count", len(all))

	for _, id := range all {
		level.Info(logger).Log("msg", "verify block", "id", id)

		if err = verifyBlock(ctx, bkt, id); err != nil {
			level.Warn(logger).Log("msg", "detected issue", "id", id, "err", err)
		}
		if err == nil || !repair {
			continue
		}
		repid, err := repairBlock(ctx, bkt, id)
		if err != nil {
			level.Warn(logger).Log("msg", "repairing block failed", "id", id, "err", err)
			continue
		}
		level.Info(logger).Log("msg", "repaired block", "id", id, "repl", repid)
	}
	return nil
}

// verifyBlock checks whether the block in the bucket has inconsistencies.
func verifyBlock(ctx context.Context, bkt objstore.BucketReader, id ulid.ULID) error {
	tmpdir, err := ioutil.TempDir("", fmt.Sprintf("verify-block-%s", id))
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpdir)

	err = objstore.DownloadFile(ctx, bkt,
		path.Join(id.String(), "index"), filepath.Join(tmpdir, "index"))
	if err != nil {
		return errors.Wrap(err, "download index file")
	}

	if err := block.VerifyIndex(filepath.Join(tmpdir, "index")); err != nil {
		return errors.Wrap(err, "verify index")
	}
	return nil
}

// repairBlock rewrites the given block while fixing repairable inconsistencies.
// If the replacement was created successfully it is uploaded to the bucket and the input
// block is deleted.
func repairBlock(ctx context.Context, bkt objstore.Bucket, id ulid.ULID) (resid ulid.ULID, err error) {
	tmpdir, err := ioutil.TempDir("", fmt.Sprintf("repair-block-%s", id))
	if err != nil {
		return resid, err
	}
	defer os.RemoveAll(tmpdir)

	bdir := filepath.Join(tmpdir, id.String())

	if err := compact.DownloadBlockDir(ctx, bkt, id.String(), bdir); err != nil {
		return resid, errors.Wrap(err, "download block")
	}
	meta, err := block.ReadMetaFile(bdir)
	if err != nil {
		return resid, errors.Wrap(err, "read meta file")
	}

	if meta.Thanos.Downsample.Resolution > 0 {
		return resid, errors.New("cannot repair downsampled blocks")
	}

	resid, err = block.Repair(tmpdir, meta.ULID)
	if err != nil {
		return resid, errors.Wrap(err, "repair failed")
	}
	// Verify repaired block before uploading it.
	if err := block.VerifyIndex(filepath.Join(tmpdir, resid.String(), "index")); err != nil {
		return resid, errors.Wrap(err, "repaired block invalid")
	}

	err = objstore.UploadDir(ctx, bkt, filepath.Join(tmpdir, resid.String()), resid.String())
	if err != nil {
		return resid, errors.Wrapf(err, "upload of %s failed", resid)
	}
	if err := objstore.DeleteDir(ctx, bkt, id.String()); err != nil {
		return resid, errors.Wrapf(err, "deleting old block %s failed", id)
	}
	return resid, nil
}

func parseMeta(ctx context.Context, bkt objstore.Bucket, name string) (block.Meta, error) {
	rc, err := bkt.Get(ctx, path.Join(name, "meta.json"))
	if err != nil {
		return block.Meta{}, errors.Wrap(err, "get reader for meta.json")
	}
	defer rc.Close()

	// Do a full decode/encode cycle to ensure we only print valid JSON.
	var m block.Meta

	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return block.Meta{}, errors.Wrap(err, "deocde meta.json")
	}
	return m, nil
}

func runBucketList(bkt objstore.Bucket, format string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var printBlock func(name string) error

	switch format {
	case "":
		printBlock = func(name string) error {
			fmt.Fprintln(os.Stdout, name[:len(name)-1])
			return nil
		}
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "\t")

		printBlock = func(name string) error {
			m, err := parseMeta(ctx, bkt, name)
			if err != nil {
				return err
			}
			return enc.Encode(&m)
		}
	default:
		tmpl, err := template.New("").Parse(format)
		if err != nil {
			return errors.Wrap(err, "invalid template")
		}
		printBlock = func(name string) error {
			m, err := parseMeta(ctx, bkt, name)
			if err != nil {
				return err
			}

			if err := tmpl.Execute(os.Stdout, &m); err != nil {
				return errors.Wrap(err, "execute template")
			}
			fmt.Fprintln(os.Stdout, "")
			return nil
		}
	}
	return bkt.Iter(ctx, "", printBlock)
}

func getBucketClient(gcsBucket *string, s3Config s3.Config, reg *prometheus.Registry) (objstore.Bucket, func() error, error) {
	var (
		bkt     objstore.Bucket
		closeFn = func() error { return nil }
	)

	// Initialize object storage clients.
	if *gcsBucket != "" {
		gcsClient, err := storage.NewClient(context.Background())
		if err != nil {
			return bkt, closeFn, errors.Wrap(err, "create GCS client")
		}
		bkt = gcs.NewBucket(*gcsBucket, gcsClient.Bucket(*gcsBucket), reg)
		closeFn = gcsClient.Close
	} else if s3Config.Validate() == nil {
		b, err := s3.NewBucket(&s3Config, reg)
		if err != nil {
			return bkt, closeFn, errors.Wrap(err, "create s3 client")
		}

		bkt = b
	} else {
		return bkt, closeFn, errors.New("no valid GCS or S3 configuration supplied")
	}

	return bkt, closeFn, nil
}
