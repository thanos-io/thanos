// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package s3_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/efficientgo/e2e"
	"github.com/go-kit/log"

	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"

	"github.com/thanos-io/thanos/pkg/testutil"
)

// Regression benchmark for https://github.com/thanos-io/thanos/issues/3917 and https://github.com/thanos-io/thanos/issues/3967.
// $ export ver=v1 && go test ./pkg/objstore/s3/... -run '^$' -bench '^BenchmarkUpload' -benchtime 5s -count 5 \
//	-memprofile=${ver}.mem.pprof -cpuprofile=${ver}.cpu.pprof | tee ${ver}.txt .
func BenchmarkUpload(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()

	e, err := e2e.NewDockerEnvironment("e2e_bench_mino_client", e2e.WithLogger(log.NewNopLogger()))
	testutil.Ok(b, err)
	b.Cleanup(e2ethanos.CleanScenario(b, e))

	const bucket = "benchmark"
	m := e2ethanos.NewMinio(e, "benchmark", bucket)
	testutil.Ok(b, e2e.StartAndWaitReady(m))

	bkt, err := s3.NewBucketWithConfig(
		log.NewNopLogger(),
		e2ethanos.NewS3Config(bucket, m.Endpoint("https"), m.Dir()),
		"test-feed",
	)
	testutil.Ok(b, err)

	buf := bytes.Buffer{}
	buf.Grow(1028 * 1028 * 100) // 100MB.
	word := "abcdefghij"
	for i := 0; i < buf.Cap()/len(word); i++ {
		_, _ = buf.WriteString(word)
	}
	str := buf.String()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testutil.Ok(b, bkt.Upload(ctx, "test", strings.NewReader(str)))
	}
}
