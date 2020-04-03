// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestReadDeletionMark(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "test-read-deletion-mark")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	{
		blockWithoutDeletionMark := ulid.MustNew(uint64(1), nil)
		_, err := ReadDeletionMark(ctx, bkt, nil, path.Join(tmpDir, blockWithoutDeletionMark.String()))

		testutil.NotOk(t, err)
		testutil.Equals(t, ErrorDeletionMarkNotFound, err)
	}
	{
		blockWithPartialDeletionMark := ulid.MustNew(uint64(2), nil)

		testutil.Ok(t, bkt.Upload(ctx, path.Join(tmpDir, blockWithPartialDeletionMark.String(), DeletionMarkFilename), bytes.NewBufferString("not a valid deletion-mark.json")))
		_, err = ReadDeletionMark(ctx, bkt, nil, path.Join(tmpDir, blockWithPartialDeletionMark.String()))

		testutil.NotOk(t, err)
		testutil.Equals(t, ErrorUnmarshalDeletionMark, errors.Cause(err))
	}
	{
		blockWithDifferentVersionDeletionMark := ulid.MustNew(uint64(3), nil)
		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&DeletionMark{
			ID:           blockWithDifferentVersionDeletionMark,
			DeletionTime: time.Now().Unix(),
			Version:      2,
		}))

		testutil.Ok(t, bkt.Upload(ctx, path.Join(tmpDir, blockWithDifferentVersionDeletionMark.String(), DeletionMarkFilename), &buf))
		_, err = ReadDeletionMark(ctx, bkt, nil, path.Join(tmpDir, blockWithDifferentVersionDeletionMark.String()))

		testutil.NotOk(t, err)
		testutil.Equals(t, "unexpected deletion-mark file version 2", err.Error())
	}
	{
		blockWithValidDeletionMark := ulid.MustNew(uint64(3), nil)
		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&DeletionMark{
			ID:           blockWithValidDeletionMark,
			DeletionTime: time.Now().Unix(),
			Version:      1,
		}))

		testutil.Ok(t, bkt.Upload(ctx, path.Join(tmpDir, blockWithValidDeletionMark.String(), DeletionMarkFilename), &buf))
		_, err = ReadDeletionMark(ctx, bkt, nil, path.Join(tmpDir, blockWithValidDeletionMark.String()))

		testutil.Ok(t, err)
	}
}
