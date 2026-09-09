// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package reloader

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/efficientgo/core/testutil"
)

// generateConfigData creates a realistic Prometheus config buffer of approx targetBytes.
func generateConfigData(targetBytes int, withEnvVars bool) []byte {
	var buf bytes.Buffer
	if withEnvVars {
		buf.WriteString("global:\n  scrape_interval: 15s\n  external_labels:\n    replica: '$(RELOADER_TEST_REPLICA)'\n    env: '$(RELOADER_TEST_ENV)'\nscrape_configs:\n")
	} else {
		buf.WriteString("global:\n  scrape_interval: 15s\n  external_labels:\n    replica: 'r1'\n    env: 'production'\nscrape_configs:\n")
	}

	rulePattern := `  - job_name: 'job-%06d'
    static_configs:
      - targets: ['localhost:90%04d']
        labels:
          cluster: 'us-east-1'
          service: 'service-%06d'
          note: 'regular text with $ and non_var and $123'
`
	if withEnvVars {
		rulePattern = `  - job_name: 'job-%06d'
    static_configs:
      - targets: ['localhost:$(RELOADER_TEST_PORT)%04d']
        labels:
          cluster: '$(RELOADER_TEST_CLUSTER)'
          service: 'service-%06d'
          note: 'regular text with $ and non_var and $123'
`
	}

	i := 0
	for buf.Len() < targetBytes {
		fmt.Fprintf(&buf, rulePattern, i, i%10000, i)
		i++
	}
	return buf.Bytes()
}

func setupTestEnv(t testing.TB) {
	t.Helper()
	t.Setenv("RELOADER_TEST_REPLICA", "r1")
	t.Setenv("RELOADER_TEST_ENV", "production")
	t.Setenv("RELOADER_TEST_PORT", "90")
	t.Setenv("RELOADER_TEST_CLUSTER", "us-east-1")
}

// Recommended CLI invocation:
/*
	export bench=normalize && go test ./... \
		-run '^$' -bench '^BenchmarkNormalize' \
		-benchtime 2s -count 6 -cpu 2 -timeout 999m \
		| tee ${bench}.txt
*/
func BenchmarkNormalize(b *testing.B) {
	setupTestEnv(b)

	for _, sz := range []struct {
		name  string
		bytes int
	}{
		{"10KB", 10 * 1024},
		{"1MB", 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
		{"50MB", 50 * 1024 * 1024},
	} {
		for _, compressed := range []bool{false, true} {
			for _, envvars := range []bool{false, true} {
				b.Run(fmt.Sprintf("size=%v/compressed=%v/envvars=%v", sz.name, compressed, envvars), func(b *testing.B) {
					data := generateConfigData(sz.bytes, envvars)
					if compressed {
						var gzBuf bytes.Buffer
						gw := gzip.NewWriter(&gzBuf)
						_, err := gw.Write(data)
						testutil.Ok(b, err)
						testutil.Ok(b, gw.Close())
						data = gzBuf.Bytes()
					}

					dir := b.TempDir()
					input := filepath.Join(dir, "input.yaml")
					testutil.Ok(b, os.WriteFile(input, data, 0644))

					r := New(log.NewNopLogger(), prometheus.NewRegistry(), &Options{})

					output := filepath.Join(dir, "output.yaml")
					b.SetBytes(int64(len(data)))
					b.ReportAllocs()
					b.ResetTimer()

					for b.Loop() {
						if err := r.normalize(input, output); err != nil {
							b.Fatalf("normalize error: %v", err)
						}
					}
				})
			}
		}
	}
}
