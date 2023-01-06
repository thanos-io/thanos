// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"go.uber.org/goleak"

	"github.com/efficientgo/core/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestReadMarker(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	t.Run(DeletionMarkFilename, func(t *testing.T) {
		blockWithoutMark := ulid.MustNew(uint64(1), nil)
		d := DeletionMark{}
		err := ReadMarker(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, blockWithoutMark.String()), &d)
		testutil.NotOk(t, err)
		testutil.Equals(t, ErrorMarkerNotFound, err)

		blockWithPartialMark := ulid.MustNew(uint64(2), nil)
		testutil.Ok(t, bkt.Upload(ctx, path.Join(tmpDir, blockWithPartialMark.String(), DeletionMarkFilename), bytes.NewBufferString("not a valid deletion-mark.json")))
		err = ReadMarker(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, blockWithPartialMark.String()), &d)
		testutil.NotOk(t, err)
		testutil.Equals(t, ErrorUnmarshalMarker, errors.Cause(err))

		blockWithDifferentVersionMark := ulid.MustNew(uint64(3), nil)
		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&DeletionMark{
			ID:           blockWithDifferentVersionMark,
			DeletionTime: time.Now().Unix(),
			Version:      2,
		}))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(tmpDir, blockWithDifferentVersionMark.String(), DeletionMarkFilename), &buf))
		err = ReadMarker(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, blockWithDifferentVersionMark.String()), &d)
		testutil.NotOk(t, err)
		testutil.Equals(t, "unexpected deletion-mark file version 2, expected 1", err.Error())

		blockWithValidMark := ulid.MustNew(uint64(3), nil)
		buf.Reset()
		expected := &DeletionMark{
			ID:           blockWithValidMark,
			DeletionTime: time.Now().Unix(),
			Version:      1,
		}
		testutil.Ok(t, json.NewEncoder(&buf).Encode(expected))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(tmpDir, blockWithValidMark.String(), DeletionMarkFilename), &buf))
		err = ReadMarker(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, blockWithValidMark.String()), &d)
		testutil.Ok(t, err)
		testutil.Equals(t, *expected, d)
	})
	t.Run(NoCompactMarkFilename, func(t *testing.T) {
		blockWithoutMark := ulid.MustNew(uint64(1), nil)
		n := NoCompactMark{}
		err := ReadMarker(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, blockWithoutMark.String()), &n)
		testutil.NotOk(t, err)
		testutil.Equals(t, ErrorMarkerNotFound, err)

		blockWithPartialMark := ulid.MustNew(uint64(2), nil)
		testutil.Ok(t, bkt.Upload(ctx, path.Join(tmpDir, blockWithPartialMark.String(), NoCompactMarkFilename), bytes.NewBufferString("not a valid no-compact-mark.json")))
		err = ReadMarker(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, blockWithPartialMark.String()), &n)
		testutil.NotOk(t, err)
		testutil.Equals(t, ErrorUnmarshalMarker, errors.Cause(err))

		blockWithDifferentVersionMark := ulid.MustNew(uint64(3), nil)
		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&NoCompactMark{
			ID:      blockWithDifferentVersionMark,
			Version: 2,
			Reason:  IndexSizeExceedingNoCompactReason,
			Details: "yolo",
		}))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(tmpDir, blockWithDifferentVersionMark.String(), NoCompactMarkFilename), &buf))
		err = ReadMarker(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, blockWithDifferentVersionMark.String()), &n)
		testutil.NotOk(t, err)
		testutil.Equals(t, "unexpected no-compact-mark file version 2, expected 1", err.Error())

		blockWithValidMark := ulid.MustNew(uint64(3), nil)
		buf.Reset()
		expected := &NoCompactMark{
			ID:      blockWithValidMark,
			Version: 1,
			Reason:  IndexSizeExceedingNoCompactReason,
			Details: "yolo",
		}
		testutil.Ok(t, json.NewEncoder(&buf).Encode(expected))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(tmpDir, blockWithValidMark.String(), NoCompactMarkFilename), &buf))
		err = ReadMarker(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, blockWithValidMark.String()), &n)
		testutil.Ok(t, err)
		testutil.Equals(t, *expected, n)
	})
}
