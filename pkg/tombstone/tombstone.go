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

	"github.com/cespare/xxhash"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/timestamp"

	"github.com/thanos-io/thanos/pkg/objstore"
)

const (
	// TombstoneDir is the name of directory to upload tombstones.
	TombstoneDir = "tombstones"
)

// Tombstone represents a tombstone.
type Tombstone struct {
	Matcher      string `json:"matcher"`
	MinTime      int64  `json:"minTime"`
	MaxTime      int64  `json:"maxTime"`
	CreationTime int64  `json:"creationTime"`
}

// NewTombstone returns a new instance of Tombstone.
func NewTombstone(matcher string, minTime, maxTime int64) Tombstone {
	return Tombstone{
		Matcher:      matcher,
		MinTime:      minTime,
		MaxTime:      maxTime,
		CreationTime: timestamp.FromTime(time.Now()),
	}
}

// GenName generates file name based on Matcher, MinTime and MaxTime of a tombstone.
func GenName(ts Tombstone) string {
	hash := xxhash.Sum64([]byte(ts.Matcher + string(ts.MinTime) + string(ts.MaxTime)))
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
	err = ioutil.WriteFile(tsPath, b, 0644)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	err = objstore.UploadFile(ctx, logger, bkt, tsPath, path.Join(TombstoneDir, GenName(tombstone)))
	if err != nil {
		return err
	}
	return nil
}
