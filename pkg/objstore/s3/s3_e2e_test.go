// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package s3_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/efficientgo/e2e"
	"github.com/go-kit/log"

	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"

	"github.com/thanos-io/thanos/pkg/testutil"
)

// TestUploadConcurrent tries to reproduce race; unsuccessful, no race detected for cases mentioned in
// https://github.com/minio/mc/issues/3376#issuecomment-1158001018 .
// $ go test -v -race ./pkg/objstore/s3 -run='TestUploadConcurrent$'.
func TestUploadConcurrent(t *testing.T) {
	t.Skip("// NOTE(bwplotka): Prerequsite: 'dd if=/dev/urandom of=./test-file bs=1024b count=1000'")
	ctx := context.Background()

	e, err := e2e.NewDockerEnvironment("e2e_bench_mino_client")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	const bucket = "benchmark"
	m := e2ethanos.NewMinio(e, "benchmark", bucket)
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	bkt, err := s3.NewBucketWithConfig(
		log.NewLogfmtLogger(os.Stderr),
		e2ethanos.NewS3Config(bucket, m.Endpoint("https"), m.Dir()),
		"test-feed",
	)
	testutil.Ok(t, err)

	wg := sync.WaitGroup{}

	// 500MB file created via `dd if=/dev/urandom of=./test-file bs=1024b count=1000`
	f, err := os.Open("../../../test-file")
	testutil.Ok(t, err)

	h := sha256.New()
	_, err = io.Copy(h, f)
	testutil.Ok(t, err)
	expectedHash := h.Sum(nil)

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			for k := 0; k < 6; k++ {
				testutil.Ok(t, bkt.Upload(ctx, fmt.Sprintf("test-%v_%v", i, k), f))
				fmt.Printf("uploaded\n")

				// Verify if object is not malformed.
				r, err := bkt.Get(ctx, fmt.Sprintf("test-%v_%v", i, k))
				testutil.Ok(t, err)

				h := sha256.New()
				_, err = io.Copy(h, r)
				testutil.Ok(t, err)
				testutil.Equals(t, expectedHash, h.Sum(nil))
			}
		}(i)
	}

	wg.Wait()
}

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
