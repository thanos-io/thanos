// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tombstone

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
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
}

// NewTombstone returns a new instance of Tombstone.
func NewTombstone(ulid ulid.ULID, matchers metadata.Matchers, minTime, maxTime int64, author, reason string) *Tombstone {
	return &Tombstone{
		ULID:     ulid,
		Matchers: &matchers,
		MinTime:  minTime,
		MaxTime:  maxTime,
		Author:   author,
		Reason:   reason,
	}
}

// ReadFromPath the tombstone from filesystem path.
func ReadFromPath(path string) (_ *Tombstone, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer runutil.ExhaustCloseWithErrCapture(&err, f, "close tombstone")

	var t Tombstone
	if err = json.NewDecoder(f).Decode(&t); err != nil {
		return nil, err
	}
	return &t, nil
}

// WriteToDir writes the encoded meta into <dir>/meta.json.
func (t *Tombstone) WriteToDir(logger log.Logger, dir string) error {
	// Make any changes to the file appear atomic.
	path := filepath.Join(dir, t.ULID.String()+".json")
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	if err := t.Write(f); err != nil {
		runutil.CloseWithLogOnErr(logger, f, "close tombstone")
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return metadata.RenameFile(logger, tmp, path)
}

func (t *Tombstone) Write(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "\t")
	return enc.Encode(&t)
}

// UploadTombstone uploads the given tombstone to object storage.
func UploadTombstone(ctx context.Context, tombstone *Tombstone, bkt objstore.Bucket) error {
	b, err := json.Marshal(tombstone)
	if err != nil {
		return err
	}

	tsPath := path.Join(TombstoneDir, tombstone.ULID.String()+".json")
	return bkt.Upload(ctx, tsPath, bytes.NewBuffer(b))
}

// OverlapsClosedInterval Returns true if the chunk overlaps [mint, maxt].
func (t *Tombstone) OverlapsClosedInterval(mint, maxt int64) bool {
	return t.MinTime <= maxt && mint <= t.MaxTime
}

func (t *Tombstone) MatchMeta(meta *metadata.Meta) (*metadata.Matchers, bool) {
	if !t.OverlapsClosedInterval(meta.MinTime, meta.MaxTime-1) {
		return nil, false
	}
	// We add the special __block_id label to support matching by block ID.
	lbls := labels.FromMap(meta.Thanos.Labels)
	lbls = append(lbls, labels.Label{Name: block.BlockIDLabel, Value: meta.ULID.String()})
	return t.MatchLabels(lbls)
}

func (t *Tombstone) MatchLabels(lbls labels.Labels) (*metadata.Matchers, bool) {
	matchers := make(metadata.Matchers, 0, len(*t.Matchers))
	for _, m := range *t.Matchers {
		v := lbls.Get(m.Name)
		if v == "" {
			matchers = append(matchers, m)
			continue
		}
		if !m.Matches(v) {
			return nil, false
		}
	}
	return &matchers, true
}
