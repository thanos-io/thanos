// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objstore

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/testutil"
	"golang.org/x/time/rate"
)

const burstLimit = 1000 * 1000 * 1000

// Adopted from https://github.com/fujiwara/shapeio
// Converted to use io.ReadCloser.
type RateLimitedReader struct {
	r       io.ReadCloser
	limiter *rate.Limiter
	ctx     context.Context
}

// NewReader returns a reader that implements io.ReadCloser with rate limiting.
func NewReader(r io.ReadCloser) *RateLimitedReader {
	return &RateLimitedReader{
		r:   r,
		ctx: context.Background(),
	}
}

// NewReaderWithContext returns a reader that implements io.ReadCloser with rate limiting.
func NewReaderWithContext(r io.ReadCloser, ctx context.Context) *RateLimitedReader {
	return &RateLimitedReader{
		r:   r,
		ctx: ctx,
	}
}

// SetRateLimit sets rate limit (bytes/sec) to the reader.
func (s *RateLimitedReader) SetRateLimit(bytesPerSec float64) {
	s.limiter = rate.NewLimiter(rate.Limit(bytesPerSec), burstLimit)
	s.limiter.AllowN(time.Now(), burstLimit) // spend initial burst
}

// Read reads bytes into p.
func (s *RateLimitedReader) Read(p []byte) (int, error) {
	if s.limiter == nil {
		return s.r.Read(p)
	}
	n, err := s.r.Read(p)
	if err != nil {
		return n, err
	}
	if err := s.limiter.WaitN(s.ctx, n); err != nil {
		return n, err
	}
	return n, nil
}

func (s *RateLimitedReader) Close() error {
	return s.r.Close()
}

// LaggyBucket is a Bucket that wraps another one and limits
// the throughput via io.Reader.
type LaggyBucket struct {
	throughput float64
	wrappedBkt Bucket
}

func NewLaggyBucket(throughput float64, bkt Bucket) Bucket {
	return &LaggyBucket{throughput: throughput, wrappedBkt: bkt}
}

func (b *LaggyBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	return b.wrappedBkt.Upload(ctx, name, r)
}

func (b *LaggyBucket) Delete(ctx context.Context, name string) error {
	return b.wrappedBkt.Delete(ctx, name)

}

func (b *LaggyBucket) Name() string {
	return b.wrappedBkt.Name()
}

func (b *LaggyBucket) Close() error {
	return b.wrappedBkt.Close()
}

func (b *LaggyBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...IterOption) error {
	return b.wrappedBkt.Iter(ctx, dir, f, options...)
}

func (b *LaggyBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	rc, err := b.wrappedBkt.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	reader := NewReader(rc)
	reader.SetRateLimit(b.throughput)
	return reader, nil
}

func (b *LaggyBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	rc, err := b.wrappedBkt.GetRange(ctx, name, off, length)
	if err != nil {
		return nil, err
	}
	reader := NewReader(rc)
	reader.SetRateLimit(b.throughput)
	return reader, nil

}

func (b *LaggyBucket) Exists(ctx context.Context, name string) (bool, error) {
	return b.wrappedBkt.Exists(ctx, name)
}

func (b *LaggyBucket) IsObjNotFoundErr(err error) bool {
	return b.wrappedBkt.IsObjNotFoundErr(err)
}

func (b *LaggyBucket) Attributes(ctx context.Context, name string) (ObjectAttributes, error) {
	return b.wrappedBkt.Attributes(ctx, name)
}

func CreateTemporaryTestBucketName(t testing.TB) string {
	src := rand.NewSource(time.Now().UnixNano())

	// Bucket name need to conform: https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-s3-bucket-naming-requirements.html.
	name := strings.ReplaceAll(strings.Replace(fmt.Sprintf("test_%x_%s", src.Int63(), strings.ToLower(t.Name())), "_", "-", -1), "/", "-")
	if len(name) >= 63 {
		name = name[:63]
	}
	return name
}

// EmptyBucket deletes all objects from bucket. This operation is required to properly delete bucket as a whole.
// It is used for testing only.
// TODO(bplotka): Add retries.
func EmptyBucket(t testing.TB, ctx context.Context, bkt Bucket) {
	var wg sync.WaitGroup

	queue := []string{""}
	for len(queue) > 0 {
		elem := queue[0]
		queue = queue[1:]

		err := bkt.Iter(ctx, elem, func(p string) error {
			if strings.HasSuffix(p, DirDelim) {
				queue = append(queue, p)
				return nil
			}

			wg.Add(1)
			go func() {
				if err := bkt.Delete(ctx, p); err != nil {
					t.Logf("deleting object %s failed: %s", p, err)
				}
				wg.Done()
			}()
			return nil
		})
		if err != nil {
			t.Logf("iterating over bucket objects failed: %s", err)
			wg.Wait()
			return
		}
	}
	wg.Wait()
}

func WithNoopInstr(bkt Bucket) InstrumentedBucket {
	return noopInstrumentedBucket{Bucket: bkt}
}

type noopInstrumentedBucket struct {
	Bucket
}

func (b noopInstrumentedBucket) WithExpectedErrs(IsOpFailureExpectedFunc) Bucket {
	return b
}

func (b noopInstrumentedBucket) ReaderWithExpectedErrs(IsOpFailureExpectedFunc) BucketReader {
	return b
}

func AcceptanceTest(t *testing.T, bkt Bucket) {
	ctx := context.Background()

	_, err := bkt.Get(ctx, "")
	testutil.NotOk(t, err)
	testutil.Assert(t, !bkt.IsObjNotFoundErr(err), "expected user error got not found %s", err)

	_, err = bkt.Get(ctx, "id1/obj_1.some")
	testutil.NotOk(t, err)
	testutil.Assert(t, bkt.IsObjNotFoundErr(err), "expected not found error got %s", err)

	ok, err := bkt.Exists(ctx, "id1/obj_1.some")
	testutil.Ok(t, err)
	testutil.Assert(t, !ok, "expected not exits")

	_, err = bkt.Attributes(ctx, "id1/obj_1.some")
	testutil.NotOk(t, err)
	testutil.Assert(t, bkt.IsObjNotFoundErr(err), "expected not found error but got %s", err)

	// Upload first object.
	testutil.Ok(t, bkt.Upload(ctx, "id1/obj_1.some", strings.NewReader("@test-data@")))

	// Double check we can immediately read it.
	rc1, err := bkt.Get(ctx, "id1/obj_1.some")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, rc1.Close()) }()
	content, err := ioutil.ReadAll(rc1)
	testutil.Ok(t, err)
	testutil.Equals(t, "@test-data@", string(content))

	// Check if we can get the correct size.
	attrs, err := bkt.Attributes(ctx, "id1/obj_1.some")
	testutil.Ok(t, err)
	testutil.Assert(t, attrs.Size == 11, "expected size to be equal to 11")

	rc2, err := bkt.GetRange(ctx, "id1/obj_1.some", 1, 3)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, rc2.Close()) }()
	content, err = ioutil.ReadAll(rc2)
	testutil.Ok(t, err)
	testutil.Equals(t, "tes", string(content))

	// Unspecified range with offset.
	rcUnspecifiedLen, err := bkt.GetRange(ctx, "id1/obj_1.some", 1, -1)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, rcUnspecifiedLen.Close()) }()
	content, err = ioutil.ReadAll(rcUnspecifiedLen)
	testutil.Ok(t, err)
	testutil.Equals(t, "test-data@", string(content))

	// Out of band offset. Do not rely on outcome.
	// NOTE: For various providers we have different outcome.
	// * GCS is giving 416 status code
	// * S3 errors immdiately with invalid range error.
	// * inmem and filesystem are returning 0 bytes.
	//rcOffset, err := bkt.GetRange(ctx, "id1/obj_1.some", 124141, 3)

	// Out of band length. We expect to read file fully.
	rcLength, err := bkt.GetRange(ctx, "id1/obj_1.some", 3, 9999)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, rcLength.Close()) }()
	content, err = ioutil.ReadAll(rcLength)
	testutil.Ok(t, err)
	testutil.Equals(t, "st-data@", string(content))

	ok, err = bkt.Exists(ctx, "id1/obj_1.some")
	testutil.Ok(t, err)
	testutil.Assert(t, ok, "expected exits")

	// Upload other objects.
	testutil.Ok(t, bkt.Upload(ctx, "id1/obj_2.some", strings.NewReader("@test-data2@")))
	// Upload should be idempotent.
	testutil.Ok(t, bkt.Upload(ctx, "id1/obj_2.some", strings.NewReader("@test-data2@")))
	testutil.Ok(t, bkt.Upload(ctx, "id1/obj_3.some", strings.NewReader("@test-data3@")))
	testutil.Ok(t, bkt.Upload(ctx, "id1/sub/subobj_1.some", strings.NewReader("@test-data4@")))
	testutil.Ok(t, bkt.Upload(ctx, "id1/sub/subobj_2.some", strings.NewReader("@test-data5@")))
	testutil.Ok(t, bkt.Upload(ctx, "id2/obj_4.some", strings.NewReader("@test-data6@")))
	testutil.Ok(t, bkt.Upload(ctx, "obj_5.some", strings.NewReader("@test-data7@")))

	// Can we iter over items from top dir?
	var seen []string
	testutil.Ok(t, bkt.Iter(ctx, "", func(fn string) error {
		seen = append(seen, fn)
		return nil
	}))
	expected := []string{"obj_5.some", "id1/", "id2/"}
	sort.Strings(expected)
	sort.Strings(seen)
	testutil.Equals(t, expected, seen)

	// Can we iter over items from top dir recursively?
	seen = []string{}
	testutil.Ok(t, bkt.Iter(ctx, "", func(fn string) error {
		seen = append(seen, fn)
		return nil
	}, WithRecursiveIter))
	expected = []string{"id1/obj_1.some", "id1/obj_2.some", "id1/obj_3.some", "id1/sub/subobj_1.some", "id1/sub/subobj_2.some", "id2/obj_4.some", "obj_5.some"}
	sort.Strings(expected)
	sort.Strings(seen)
	testutil.Equals(t, expected, seen)

	// Can we iter over items from id1/ dir?
	seen = []string{}
	testutil.Ok(t, bkt.Iter(ctx, "id1/", func(fn string) error {
		seen = append(seen, fn)
		return nil
	}))
	testutil.Equals(t, []string{"id1/obj_1.some", "id1/obj_2.some", "id1/obj_3.some", "id1/sub/"}, seen)

	// Can we iter over items from id1/ dir recursively?
	seen = []string{}
	testutil.Ok(t, bkt.Iter(ctx, "id1/", func(fn string) error {
		seen = append(seen, fn)
		return nil
	}, WithRecursiveIter))
	testutil.Equals(t, []string{"id1/obj_1.some", "id1/obj_2.some", "id1/obj_3.some", "id1/sub/subobj_1.some", "id1/sub/subobj_2.some"}, seen)

	// Can we iter over items from id1 dir?
	seen = []string{}
	testutil.Ok(t, bkt.Iter(ctx, "id1", func(fn string) error {
		seen = append(seen, fn)
		return nil
	}))
	testutil.Equals(t, []string{"id1/obj_1.some", "id1/obj_2.some", "id1/obj_3.some", "id1/sub/"}, seen)

	// Can we iter over items from id1 dir recursively?
	seen = []string{}
	testutil.Ok(t, bkt.Iter(ctx, "id1", func(fn string) error {
		seen = append(seen, fn)
		return nil
	}, WithRecursiveIter))
	testutil.Equals(t, []string{"id1/obj_1.some", "id1/obj_2.some", "id1/obj_3.some", "id1/sub/subobj_1.some", "id1/sub/subobj_2.some"}, seen)

	// Can we iter over items from not existing dir?
	testutil.Ok(t, bkt.Iter(ctx, "id0", func(fn string) error {
		t.Error("Not expected to loop through not existing directory")
		t.FailNow()

		return nil
	}))

	testutil.Ok(t, bkt.Delete(ctx, "id1/obj_2.some"))

	// Delete is expected to fail on non existing object.
	// NOTE: Don't rely on this. S3 is not complying with this as GCS is.
	// testutil.NotOk(t, bkt.Delete(ctx, "id1/obj_2.some"))

	// Can we iter over items from id1/ dir and see obj2 being deleted?
	seen = []string{}
	testutil.Ok(t, bkt.Iter(ctx, "id1/", func(fn string) error {
		seen = append(seen, fn)
		return nil
	}))
	testutil.Equals(t, []string{"id1/obj_1.some", "id1/obj_3.some", "id1/sub/"}, seen)

	testutil.Ok(t, bkt.Delete(ctx, "id2/obj_4.some"))

	seen = []string{}
	testutil.Ok(t, bkt.Iter(ctx, "", func(fn string) error {
		seen = append(seen, fn)
		return nil
	}))
	expected = []string{"obj_5.some", "id1/"}
	sort.Strings(expected)
	sort.Strings(seen)
	testutil.Equals(t, expected, seen)
}
