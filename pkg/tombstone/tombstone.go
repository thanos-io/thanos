// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tombstone

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/cespare/xxhash"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/timestamp"

	"github.com/thanos-io/thanos/pkg/objstore"
)

const (
	// TombstoneDir is the name of directory to upload tombstones.
	TombstoneDir = "thanos/tombstones"
)

// Tombstone represents a tombstone.
type Tombstone struct {
	Matchers     string `json:"matchers"`
	MinTime      int64  `json:"minTime"`
	MaxTime      int64  `json:"maxTime"`
	CreationTime int64  `json:"creationTime"`
	Author       string `json:"author"`
	Reason       string `json:"reason"`
}

// NewTombstone returns a new instance of Tombstone.
func NewTombstone(matchers string, minTime, maxTime int64, author string, reason string) Tombstone {
	return Tombstone{
		Matchers:     matchers,
		MinTime:      minTime,
		MaxTime:      maxTime,
		CreationTime: timestamp.FromTime(time.Now()),
		Author:       author,
		Reason:       reason,
	}
}

// GenName generates file name based on Matchers, MinTime and MaxTime of a tombstone.
func GenName(ts Tombstone) string {
	hash := xxhash.Sum64([]byte(ts.Matchers + strconv.FormatInt(ts.MinTime, 10) + strconv.FormatInt(ts.MaxTime, 10)))
	return strconv.FormatUint(hash, 10) + ".json"
}

// UploadTombstone uploads the given tombstone to object storage.
func UploadTombstone(tombstone Tombstone, bkt objstore.Bucket, logger log.Logger) error {
	b, err := json.Marshal(tombstone)
	if err != nil {
		return err
	}

	tmpDir := os.TempDir()

	tsPath := tmpDir + "/tombstone.json"
	if err := ioutil.WriteFile(tsPath, b, 0644); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	return objstore.UploadFile(ctx, logger, bkt, tsPath, path.Join(TombstoneDir, GenName(tombstone)))

}

// ReadTombstones returns all the tombstones present in the object storage.
func ReadTombstones(bkt objstore.InstrumentedBucketReader, logger log.Logger) ([]Tombstone, error) {
	var ts []Tombstone
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	bkt.Iter(ctx, TombstoneDir, func(name string) error {
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
			return nil
		}
		ts = append(ts, t)
		return nil
	})
	return ts, nil
}
