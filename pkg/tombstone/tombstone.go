// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tombstone

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/pkg/timestamp"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
)

const (
	// TombstoneDir is the name of directory to upload tombstones.
	TombstoneDir = "thanos/tombstones"
)

// Tombstone represents a tombstone.
type Tombstone struct {
	Matchers     metadata.Matchers `json:"matchers"`
	MinTime      int64             `json:"minTime"`
	MaxTime      int64             `json:"maxTime"`
	CreationTime int64             `json:"creationTime"`
	Author       string            `json:"author"`
	Reason       string            `json:"reason"`
}

// NewTombstone returns a new instance of Tombstone.
func NewTombstone(matchers metadata.Matchers, minTime, maxTime int64, author string, reason string) *Tombstone {
	return &Tombstone{
		Matchers:     matchers,
		MinTime:      minTime,
		MaxTime:      maxTime,
		CreationTime: timestamp.FromTime(time.Now()),
		Author:       author,
		Reason:       reason,
	}
}

// GenName generates file name based on Matchers, MinTime and MaxTime of a tombstone.
func GenName() string {
	return fmt.Sprintf("tombstones-%s.json", ulid.MustNew(uint64(time.Now().Unix()), nil))
}

// UploadTombstone uploads the given tombstone to object storage.
func UploadTombstone(ctx context.Context, tombstone *Tombstone, bkt objstore.Bucket, logger log.Logger) error {
	b, err := json.Marshal(tombstone)
	if err != nil {
		return err
	}

	tmpDir := os.TempDir()

	tsPath := tmpDir + "/tombstone.json"
	if err := ioutil.WriteFile(tsPath, b, 0644); err != nil {
		return err
	}

	return objstore.UploadFile(ctx, logger, bkt, tsPath, path.Join(TombstoneDir, GenName()))

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

		var t *Tombstone
		tombstone, err := ioutil.ReadAll(tombstoneFile)
		if err != nil {
			return nil
		}
		if err := json.Unmarshal(tombstone, t); err != nil {
			return nil
		}
		ts = append(ts, t)
		return nil
	}); err != nil {
		return nil, err
	}
	return ts, nil
}
