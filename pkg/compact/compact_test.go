package compact

import (
	"bytes"
	"context"
	"path"
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/relabel"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/thanos/pkg/objstore/inmem"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestHaltError(t *testing.T) {
	err := errors.New("test")
	testutil.Assert(t, !IsHaltError(err), "halt error")

	err = halt(errors.New("test"))
	testutil.Assert(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(halt(errors.New("test")), "something")
	testutil.Assert(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(errors.Wrap(halt(errors.New("test")), "something"), "something2")
	testutil.Assert(t, IsHaltError(err), "not a halt error")
}

func TestHaltMultiError(t *testing.T) {
	haltErr := halt(errors.New("halt error"))
	nonHaltErr := errors.New("not a halt error")

	errs := terrors.MultiError{nonHaltErr}
	testutil.Assert(t, !IsHaltError(errs), "should not be a halt error")

	errs.Add(haltErr)
	testutil.Assert(t, IsHaltError(errs), "if any halt errors are present this should return true")
}

func TestRetryMultiError(t *testing.T) {
	retryErr := Retry(errors.New("retry error"))
	nonRetryErr := errors.New("not a retry error")

	errs := terrors.MultiError{nonRetryErr}
	testutil.Assert(t, !IsRetryError(errs), "should not be a retry error")

	errs = terrors.MultiError{retryErr}
	testutil.Assert(t, IsRetryError(errs), "if all errors are retriable this should return true")

	errs = terrors.MultiError{nonRetryErr, retryErr}
	testutil.Assert(t, !IsRetryError(errs), "mixed errors should return false")
}

func TestRetryError(t *testing.T) {
	err := errors.New("test")
	testutil.Assert(t, !IsRetryError(err), "retry error")

	err = Retry(errors.New("test"))
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(Retry(errors.New("test")), "something")
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(errors.Wrap(Retry(errors.New("test")), "something"), "something2")
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(Retry(errors.Wrap(halt(errors.New("test")), "something")), "something2")
	testutil.Assert(t, IsHaltError(err), "not a halt error. Retry should not hide halt error")
}

func TestSyncer_SyncMetas_HandlesMalformedBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bkt := inmem.NewBucket()
	relabelConfig := make([]*relabel.Config, 0)
	sy, err := NewSyncer(nil, nil, bkt, 10*time.Second, 1, false, relabelConfig)
	testutil.Ok(t, err)

	// Generate 1 block which is older than MinimumAgeForRemoval which has chunk data but no meta.  Compactor should delete it.
	shouldDeleteId, err := ulid.New(uint64(time.Now().Add(-time.Hour).Unix()*1000), nil)
	testutil.Ok(t, err)

	var fakeChunk bytes.Buffer
	fakeChunk.Write([]byte{0, 1, 2, 3})
	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldDeleteId.String(), "chunks", "000001"), &fakeChunk))

	// Generate 1 block which is older than consistencyDelay but younger than MinimumAgeForRemoval, and which has chunk
	// data but no meta.  Compactor should ignore it.
	shouldIgnoreId, err := ulid.New(uint64(time.Now().Unix()*1000), nil)
	testutil.Ok(t, err)

	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldIgnoreId.String(), "chunks", "000001"), &fakeChunk))

	testutil.Ok(t, sy.SyncMetas(ctx))

	exists, err := bkt.Exists(ctx, path.Join(shouldDeleteId.String(), "chunks", "000001"))
	testutil.Ok(t, err)
	testutil.Equals(t, false, exists)

	exists, err = bkt.Exists(ctx, path.Join(shouldIgnoreId.String(), "chunks", "000001"))
	testutil.Ok(t, err)
	testutil.Equals(t, true, exists)
}

func TestGroupKey(t *testing.T) {
	for _, tcase := range []struct {
		input    metadata.Thanos
		expected string
	}{
		{
			input:    metadata.Thanos{},
			expected: "0@17241709254077376921",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@17241709254077376921",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{"foo": "bar", "foo1": "bar2"},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@2124638872457683483",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{`foo/some..thing/some.thing/../`: `a_b_c/bar-something-a\metric/a\x`},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@16590761456214576373",
		},
	} {
		if ok := t.Run("", func(t *testing.T) {
			testutil.Equals(t, tcase.expected, GroupKey(tcase.input))
		}); !ok {
			return
		}
	}
}
