package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/improbable-eng/thanos/pkg/block"

	"github.com/go-kit/kit/log/level"

	"github.com/oklog/ulid"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerBucket(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "inspect metric data in an object storage bucket")

	gcsBucket := cmd.Flag("gcs-bucket", "Google Cloud Storage bucket name for stored blocks.").
		PlaceHolder("<bucket>").Required().String()

	check := cmd.Command("check", "verify all blocks in the bucket")

	repair := check.Flag("repair", "attempt to repair blocks for which issues were detected").
		Short('r').Default("false").Bool()

	m[name+" check"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer) error {
		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		gcsClient, err := storage.NewClient(context.Background())
		if err != nil {
			return errors.Wrap(err, "create GCS client")
		}
		defer gcsClient.Close()

		bkt := gcs.NewBucket(*gcsBucket, gcsClient.Bucket(*gcsBucket), reg)

		return runBucketCheck(logger, bkt, *repair)
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

		if err := verifyBlock(ctx, bkt, id); err != nil {
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

	if err := objstore.DownloadDir(ctx, bkt, id.String(), bdir); err != nil {
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
