// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tombstone

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"io/ioutil"
	"path"
)

const (
	// TombstoneDir is the name of directory to upload tombstones.
	TombstoneDir = "tombstones"
)

// Tombstone represents a tombstone.
type Tombstone struct {
	ULID     ulid.ULID          `json:"ulid"`
	Matchers *metadata.Matchers `json:"matchers"`
	MinTime  int64              `json:"minTime"`
	MaxTime  int64              `json:"maxTime"`
	Author   string             `json:"author"`
	Reason   string             `json:"reason"`
	// Labels are the external labels identifying the producer as well as tenant.
	// See https://thanos.io/tip/thanos/storage.md#external-labels for details.
	Labels labels.Labels `json:"labels"`
}

// NewTombstone returns a new instance of Tombstone.
func NewTombstone(ulid ulid.ULID, matchers metadata.Matchers, minTime, maxTime int64, author, reason string, labels labels.Labels) *Tombstone {
	return &Tombstone{
		ULID:     ulid,
		Matchers: &matchers,
		MinTime:  minTime,
		MaxTime:  maxTime,
		Author:   author,
		Reason:   reason,
		Labels:   labels,
	}
}

// UploadTombstone uploads the given tombstone to object storage.
func UploadTombstone(ctx context.Context, tombstone *Tombstone, bkt objstore.Bucket) error {
	b, err := json.Marshal(tombstone)
	if err != nil {
		return err
	}

	tsPath := path.Join(TombstoneDir, tombstone.ULID.String())
	return bkt.Upload(ctx, tsPath, bytes.NewBuffer(b))
}

// ReadTombstones returns all the tombstones present in the object storage.
func ReadTombstones(ctx context.Context, bkt objstore.InstrumentedBucketReader, logger log.Logger) ([]*Tombstone, error) {
	var ts []*Tombstone

	if err := bkt.Iter(ctx, TombstoneDir, func(name string) error {
		tombstoneFilename := path.Join("", name)
		tombstoneFile, err := bkt.Get(ctx, tombstoneFilename)
		if err != nil {
			return nil
		}
		defer runutil.CloseWithLogOnErr(logger, tombstoneFile, "close bkt tombstone reader")

		var t Tombstone
		tombstone, err := ioutil.ReadAll(tombstoneFile)
		if err != nil {
			return nil
		}
		if err := json.Unmarshal(tombstone, &t); err != nil {
			level.Error(logger).Log("msg", "failed to unmarshal tombstone", "file", tombstoneFilename, "err", err)
			return nil
		}
		ts = append(ts, &t)
		return nil
	}); err != nil {
		return nil, err
	}
	return ts, nil
}
