// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objstore

import (
	"context"
	"io/ioutil"
	"sort"
	"strings"
	"testing"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMetricBucket_Close(t *testing.T) {
	bkt := BucketWithMetrics("abc", NewInMemBucket(), nil)
	// Expected initialized metrics.
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.ops))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsFailures))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsDuration))

	AcceptanceTest(t, bkt.WithExpectedErrs(bkt.IsObjNotFoundErr))
	testutil.Equals(t, float64(6), promtest.ToFloat64(bkt.ops.WithLabelValues(OpIter)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(bkt.ops.WithLabelValues(OpAttributes)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.ops.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.ops.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(bkt.ops.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(6), promtest.ToFloat64(bkt.ops.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(bkt.ops.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.ops))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpIter)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpAttributes)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsFailures))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsDuration))
	lastUpload := promtest.ToFloat64(bkt.lastSuccessfulUploadTime)
	testutil.Assert(t, lastUpload > 0, "last upload not greater than 0, val: %f", lastUpload)

	// Clear bucket, but don't clear metrics to ensure we use same.
	bkt.bkt = NewInMemBucket()
	AcceptanceTest(t, bkt)
	testutil.Equals(t, float64(12), promtest.ToFloat64(bkt.ops.WithLabelValues(OpIter)))
	testutil.Equals(t, float64(4), promtest.ToFloat64(bkt.ops.WithLabelValues(OpAttributes)))
	testutil.Equals(t, float64(6), promtest.ToFloat64(bkt.ops.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(6), promtest.ToFloat64(bkt.ops.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(4), promtest.ToFloat64(bkt.ops.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(12), promtest.ToFloat64(bkt.ops.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(4), promtest.ToFloat64(bkt.ops.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.ops))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpIter)))
	// Not expected not found error here.
	testutil.Equals(t, float64(1), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpAttributes)))
	// Not expected not found errors, this should increment failure metric on get for not found as well, so +2.
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsFailures))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsDuration))
	testutil.Assert(t, promtest.ToFloat64(bkt.lastSuccessfulUploadTime) > lastUpload)
}

func TestPrefixedBucket(t *testing.T) {
	prefix := "/abc/def/"
	ctx := context.Background()
	bkt := NewPrefixedBucket(NewInMemBucket(), prefix)
	testutil.Ok(t, bkt.Upload(ctx, "id1/obj_1.some", strings.NewReader("@test-data@")))
	// Double check we can immediately read it.
	rc1, err := bkt.Get(ctx, "id1/obj_1.some")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, rc1.Close()) }()
	content, err := ioutil.ReadAll(rc1)
	testutil.Ok(t, err)
	testutil.Equals(t, "@test-data@", string(content))
	// Upload other objects.
	testutil.Ok(t, bkt.Upload(ctx, "id1/obj_2.some", strings.NewReader("@test-data2@")))
	// Upload should be idempotent.
	testutil.Ok(t, bkt.Upload(ctx, "id1/obj_2.some", strings.NewReader("@test-data2@")))
	testutil.Ok(t, bkt.Upload(ctx, "id1/obj_3.some", strings.NewReader("@test-data3@")))
	testutil.Ok(t, bkt.Upload(ctx, "id2/obj_4.some", strings.NewReader("@test-data4@")))
	testutil.Ok(t, bkt.Upload(ctx, "obj_5.some", strings.NewReader("@test-data5@")))

	// Can we iter over items from top dir?
	var seen []string
	testutil.Ok(t, bkt.Iter(ctx, "", func(fn string) error {
		seen = append(seen, fn)
		return nil
	}))
	expected := []string{"obj_5.some", "id1/", "id2/"}
	sort.Strings(expected)
	sort.Strings(seen)
	// fmt.Printf("%v\n", inmembkt.Objects())
	testutil.Equals(t, expected, seen)

	// Can we iter over items from id1/ dir?
	seen = []string{}
	testutil.Ok(t, bkt.Iter(ctx, "id1/", func(fn string) error {
		seen = append(seen, fn)
		return nil
	}))
	testutil.Equals(t, []string{"id1/obj_1.some", "id1/obj_2.some", "id1/obj_3.some"}, seen)

	// Can we iter over items from id1 dir?
	seen = []string{}
	testutil.Ok(t, bkt.Iter(ctx, "id1", func(fn string) error {
		seen = append(seen, fn)
		return nil
	}))
	testutil.Equals(t, []string{"id1/obj_1.some", "id1/obj_2.some", "id1/obj_3.some"}, seen)

	// Can we iter over items from not existing dir?
	testutil.Ok(t, bkt.Iter(ctx, "id0", func(fn string) error {
		t.Error("Not expected to loop through not existing directory")
		t.FailNow()

		return nil
	}))
}
