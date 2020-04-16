// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"path"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
)

const (
	// DeletionMarkFilename is the known json filename to store details about when block is marked for deletion.
	DeletionMarkFilename = "deletion-mark.json"

	// DeletionMarkVersion1 is the version of deletion-mark file supported by Thanos.
	DeletionMarkVersion1 = 1
)

// ErrorDeletionMarkNotFound is the error when deletion-mark.json file is not found.
var ErrorDeletionMarkNotFound = errors.New("deletion-mark.json not found")

// ErrorMalformedDeletionMark is the error when we cannot understand deletion-mark.json file.
// This error can occur because deletion-mark.json has been partially uploaded to block storage
// or the deletion-mark.json file is not a valid json file. It can be triggered also when version is unknown or
// if id is not matching the directory id.
var ErrorMalformedDeletionMark = errors.New("unmarshal deletion-mark.json")

// DeletionMark stores block id and when block was marked for deletion.
type DeletionMark struct {
	// ID of the tsdb block.
	ID ulid.ULID `json:"id"`

	// DeletionTime is a unix timestamp of when the block was marked to be deleted.
	DeletionTime int64          `json:"deletion_time"`
	Reason       DeletionReason `json:"reason,omitempty"`

	// Version of the file.
	Version int `json:"version"`
}

// ReadDeletionMark reads the given deletion mark file from <dir>/deletion-mark.json in bucket.
func ReadDeletionMark(ctx context.Context, bkt objstore.InstrumentedBucketReader, logger log.Logger, id ulid.ULID) (*DeletionMark, error) {
	deletionMarkFile := path.Join(id.String(), DeletionMarkFilename)

	r, err := bkt.ReaderWithExpectedErrs(bkt.IsObjNotFoundErr).Get(ctx, deletionMarkFile)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, ErrorDeletionMarkNotFound
		}
		return nil, errors.Wrapf(err, "get file: %s", deletionMarkFile)
	}

	defer runutil.CloseWithLogOnErr(logger, r, "close bkt deletion-mark reader for block %s", id)

	metaContent, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrapf(err, "read file: %s", deletionMarkFile)
	}

	deletionMark := DeletionMark{}
	if err := json.Unmarshal(metaContent, &deletionMark); err != nil {
		return nil, errors.Wrapf(ErrorMalformedDeletionMark, "file: %s; err: %v", deletionMarkFile, err.Error())
	}
	if deletionMark.Version != DeletionMarkVersion1 {
		return nil, errors.Wrapf(ErrorMalformedDeletionMark, "file: %s; err: %v", deletionMarkFile, errors.Errorf("unexpected deletion-mark file version %d", deletionMark.Version).Error())
	}
	if deletionMark.ID != id {
		return nil, errors.Wrapf(ErrorMalformedDeletionMark, "file: %s; err: %v", deletionMarkFile, errors.Errorf("unexpected deletion-mark file version %d", deletionMark.Version).Error())
	}
	return &deletionMark, nil
}

// DeletionMarker allows marking blocks for future delete.
type DeletionMarker struct {
	logger            log.Logger
	bkt               objstore.InstrumentedBucket
	markedForDeletion *prometheus.CounterVec

	// TimeNow allows injecting time for test purposes.
	TimeNow func() time.Time
}

type DeletionReason string

const (
	PostCompactDuplicateDeletion  DeletionReason = "after-compact-dup"
	BetweenCompactDuplicateReason DeletionReason = "between-compact-dup"
	RetentionDeletion             DeletionReason = "retention"
	PartialForTooLongDeletion     DeletionReason = "partial-for-too-long"
)

// NewDeletionMarker create new deletion marker.
func NewDeletionMarker(reg prometheus.Registerer, logger log.Logger, bkt objstore.InstrumentedBucket) *DeletionMarker {
	markedForDeletion := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		// TODO(bwplotka): Change to thanos_block_marked_for_deletion_total to be consistent with package name.
		Name: "thanos_compactor_blocks_marked_for_deletion_total",
		Help: "Total number of blocks marked for deletion in compactor.",
	}, []string{"reason"})

	for _, reason := range []DeletionReason{
		PostCompactDuplicateDeletion,
		BetweenCompactDuplicateReason,
		RetentionDeletion,
		PartialForTooLongDeletion,
	} {
		// Initialize.
		markedForDeletion.WithLabelValues(string(reason))
	}

	return &DeletionMarker{
		logger:            logger,
		bkt:               bkt,
		markedForDeletion: markedForDeletion,
		TimeNow:           time.Now,
	}
}

// MarkForDeletion creates a file which stores information about when the block was marked for deletion.
// This file can be then understood by readers to know that the block will be eventually deleted.
func (d *DeletionMarker) MarkForDeletion(ctx context.Context, id ulid.ULID, reason DeletionReason) (err error) {
	markerFilepath := path.Join(id.String(), DeletionMarkFilename)
	rc, err := d.bkt.ReaderWithExpectedErrs(d.bkt.IsObjNotFoundErr).Get(ctx, markerFilepath)
	if err != nil {
		if !d.bkt.IsObjNotFoundErr(err) {
			return errors.Wrapf(err, "get %s from bucket", markerFilepath)
		}
		// Not found, as expected.
	}

	if err == nil {
		// There is deletion mark even though we expected none. Maybe partial deletion file?
		// TODO(bwplotka): Deal with that!
		level.Warn(d.logger).Log(
			"msg", "requested to mark for deletion, but file already exists; this should not happen; investigate",
			"reason", reason,
			"err", errors.Errorf("file %s already exists in bucket", markerFilepath),
		)
		runutil.ExhaustCloseWithErrCapture(&err, rc, "close rc")
		return nil
	}

	deletionMark, err := json.Marshal(DeletionMark{
		ID: id,
		// TODO(bwplotka): Changing this semantics to "time when block will be deleted" will be more helpful to readers and might be more
		// understandable.
		DeletionTime: d.TimeNow().Unix(),
		Reason:       reason,
		Version:      DeletionMarkVersion1,
	})
	if err != nil {
		return errors.Wrap(err, "json encode deletion mark")
	}

	if err := d.bkt.Upload(ctx, markerFilepath, bytes.NewBuffer(deletionMark)); err != nil {
		return errors.Wrapf(err, "upload file %s to bucket", markerFilepath)
	}
	d.markedForDeletion.WithLabelValues(string(reason)).Inc()
	level.Info(d.logger).Log("msg", "block has been marked for deletion", "block", id, "reason", reason)
	return nil
}
