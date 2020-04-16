// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestReadDeletionMark(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx := context.Background()

	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	{
		blockWithoutDeletionMark := ulid.MustNew(uint64(1), nil)

		_, err := ReadDeletionMark(ctx, bkt, nil, blockWithoutDeletionMark)
		testutil.NotOk(t, err)
		testutil.Equals(t, ErrorDeletionMarkNotFound, err)
	}
	{
		blockWithPartialDeletionMark := ulid.MustNew(uint64(2), nil)

		testutil.Ok(t, bkt.Upload(ctx, path.Join(blockWithPartialDeletionMark.String(), DeletionMarkFilename), bytes.NewBufferString("not a valid deletion-mark.json")))

		_, err := ReadDeletionMark(ctx, bkt, nil, blockWithPartialDeletionMark)
		testutil.NotOk(t, err)
		testutil.Equals(t, ErrorMalformedDeletionMark, errors.Cause(err))
	}
	{
		blockWithDifferentVersionDeletionMark := ulid.MustNew(uint64(3), nil)
		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&DeletionMark{
			ID:           blockWithDifferentVersionDeletionMark,
			DeletionTime: time.Now().Unix(),
			Version:      2,
		}))

		testutil.Ok(t, bkt.Upload(ctx, path.Join(blockWithDifferentVersionDeletionMark.String(), DeletionMarkFilename), &buf))

		_, err := ReadDeletionMark(ctx, bkt, nil, blockWithDifferentVersionDeletionMark)
		testutil.NotOk(t, err)
		testutil.Equals(t, ErrorMalformedDeletionMark, errors.Cause(err))
	}
	{
		blockWithWrongIDDeletionMark := ulid.MustNew(uint64(3), nil)
		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&DeletionMark{
			ID:           ulid.MustNew(uint64(323), nil),
			DeletionTime: time.Now().Unix(),
			Version:      1,
		}))

		testutil.Ok(t, bkt.Upload(ctx, path.Join(blockWithWrongIDDeletionMark.String(), DeletionMarkFilename), &buf))

		_, err := ReadDeletionMark(ctx, bkt, nil, blockWithWrongIDDeletionMark)
		testutil.NotOk(t, err)
		testutil.Equals(t, ErrorMalformedDeletionMark, errors.Cause(err))
	}
	{
		blockWithValidDeletionMark := ulid.MustNew(uint64(3), nil)
		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&DeletionMark{
			ID:           blockWithValidDeletionMark,
			DeletionTime: time.Now().Unix(),
			Version:      1,
		}))

		testutil.Ok(t, bkt.Upload(ctx, path.Join(blockWithValidDeletionMark.String(), DeletionMarkFilename), &buf))

		_, err := ReadDeletionMark(ctx, bkt, nil, blockWithValidDeletionMark)
		testutil.Ok(t, err)
	}
}

func TestMarkForDeletion(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx := context.Background()
	logger := log.NewLogfmtLogger(os.Stderr)

	id := ulid.MustNew(1, nil)
	for _, tcase := range []struct {
		name     string
		expected func(t testing.TB, now time.Time, bkt objstore.Bucket) DeletionMark

		blocksMarked int
	}{
		{
			name: "block marked for deletion",
			expected: func(t testing.TB, now time.Time, bkt objstore.Bucket) DeletionMark {
				return DeletionMark{
					ID:           id,
					DeletionTime: now.Unix(),
					Reason:       BetweenCompactDuplicateReason,
					Version:      DeletionMarkVersion1,
				}
			},
			blocksMarked: 1,
		},
		{
			name: "block with deletion mark already, expected log and no metric increment",
			expected: func(t testing.TB, now time.Time, bkt objstore.Bucket) DeletionMark {
				expected := DeletionMark{
					ID:           id,
					DeletionTime: now.Add(-2 * time.Hour).Unix(),
					Reason:       BetweenCompactDuplicateReason,
					Version:      DeletionMarkVersion1,
				}
				deletionMark, err := json.Marshal(expected)
				testutil.Ok(t, err)
				testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), DeletionMarkFilename), bytes.NewReader(deletionMark)))

				return expected
			},
			blocksMarked: 0,
		},
		//{ TODO(bwplotka): TO FIX.
		//	name: "block with broken, malformed deletion mark already, expected to be repaired / replaced",
		//	expected: func(t testing.TB, now time.Time, bkt objstore.Bucket) DeletionMark {
		//		expected := DeletionMark{
		//			ID:           id,
		//			DeletionTime: now.Add(-2 * time.Hour).Unix(),
		//			Reason:       BetweenCompactDuplicateReason,
		//			Version:      DeletionMarkVersion1,
		//		}
		//		deletionMark, err := json.Marshal(expected)
		//		testutil.Ok(t, err)
		//
		//		// Uploaded malformed JSON file.
		//		testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), DeletionMarkFilename), bytes.NewReader(deletionMark[:len(deletionMark)-5])))
		//
		//		// Expect file to be overwritten.
		//		expected.DeletionTime = now.Unix()
		//		return expected
		//	},
		//	blocksMarked: 1,
		//},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			// NOTE: DeletionMarker does no care about block. Directory can even not exists, no need to upload anything.
			bkt := objstore.NewInMemBucket()

			now := time.Now()
			d := NewDeletionMarker(nil, logger, objstore.WithNoopInstr(bkt))
			d.TimeNow = func() time.Time { return now }

			exp := tcase.expected(t, now, bkt)

			testutil.Ok(t, d.MarkForDeletion(ctx, id, BetweenCompactDuplicateReason))
			testutil.Equals(t, 4, promtest.CollectAndCount(d.markedForDeletion))
			testutil.Equals(t, float64(tcase.blocksMarked), promtest.ToFloat64(d.markedForDeletion.WithLabelValues(string(BetweenCompactDuplicateReason))))
			testutil.Equals(t, 0.0, promtest.ToFloat64(d.markedForDeletion.WithLabelValues(string(PostCompactDuplicateDeletion))))
			testutil.Equals(t, 0.0, promtest.ToFloat64(d.markedForDeletion.WithLabelValues(string(RetentionDeletion))))
			testutil.Equals(t, 0.0, promtest.ToFloat64(d.markedForDeletion.WithLabelValues(string(PartialForTooLongDeletion))))

			dm, err := ReadDeletionMark(ctx, objstore.WithNoopInstr(bkt), logger, id)
			testutil.Ok(t, err)
			testutil.Equals(t, exp, *dm)
		})
	}
}
