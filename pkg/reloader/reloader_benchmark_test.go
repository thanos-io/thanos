// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package reloader

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/efficientgo/core/testutil"
)

var envReOld = regexp.MustCompile(`\$\(([a-zA-Z_0-9]+)\)`)

func expandEnvOld(r *Reloader, b []byte) ([]byte, error) {
	var (
		configEnvVarExpansionErrorsCount = 0
		expansionErr                     error
	)
	defer func() {
		r.configEnvVarExpansionErrors.Set(float64(configEnvVarExpansionErrorsCount))
	}()

	replaced := envReOld.ReplaceAllFunc(b, func(n []byte) []byte {
		if expansionErr != nil {
			return nil
		}
		m := n
		n = n[2 : len(n)-1]

		v, ok := os.LookupEnv(string(n))
		if !ok {
			configEnvVarExpansionErrorsCount++
			errStr := errors.Errorf("found reference to unset environment variable %q", n)
			if r.tolerateEnvVarExpansionErrors {
				return m
			}
			expansionErr = errStr
			return nil
		}
		return []byte(v)
	})
	return replaced, expansionErr
}

func normalizeOld(r *Reloader, inputFile, outputFile string) error {
	b, err := os.ReadFile(inputFile)
	if err != nil {
		return errors.Wrap(err, "read file")
	}

	if len(b) >= 3 && bytes.Equal(b[0:3], firstGzipBytes) {
		zr, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			return errors.Wrap(err, "create gzip reader")
		}
		defer zr.Close()

		b, err = io.ReadAll(zr)
		if err != nil {
			return errors.Wrap(err, "read compressed config file")
		}
	}

	b, err = expandEnvOld(r, b)
	if err != nil {
		return errors.Wrap(err, "expand environment variables")
	}

	tmpFile := outputFile + ".tmp"
	defer func() {
		_ = os.Remove(tmpFile)
	}()
	if err := os.WriteFile(tmpFile, b, 0644); err != nil {
		return errors.Wrap(err, "write file")
	}
	if err := os.Rename(tmpFile, outputFile); err != nil {
		return errors.Wrap(err, "rename file")
	}
	return nil
}

// generateConfigData creates a realistic Prometheus config buffer of approx targetBytes.
func generateConfigData(targetBytes int) []byte {
	var buf bytes.Buffer
	buf.WriteString("global:\n  scrape_interval: 15s\n  external_labels:\n    replica: '$(RELOADER_TEST_REPLICA)'\n    env: '$(RELOADER_TEST_ENV)'\nscrape_configs:\n")
	rulePattern := `  - job_name: 'job-%06d'
    static_configs:
      - targets: ['localhost:$(RELOADER_TEST_PORT)%04d']
        labels:
          cluster: '$(RELOADER_TEST_CLUSTER)'
          service: 'service-%06d'
          note: 'regular text with $ and non_var and $123'
`
	i := 0
	for buf.Len() < targetBytes {
		fmt.Fprintf(&buf, rulePattern, i, i%10000, i)
		i++
	}
	return buf.Bytes()
}

func setupBenchmarkEnv(b *testing.B) {
	b.Helper()
	b.Setenv("RELOADER_TEST_REPLICA", "r1")
	b.Setenv("RELOADER_TEST_ENV", "production")
	b.Setenv("RELOADER_TEST_PORT", "90")
	b.Setenv("RELOADER_TEST_CLUSTER", "us-east-1")
}

func setupTestEnv(t *testing.T) {
	t.Helper()
	t.Setenv("RELOADER_TEST_REPLICA", "r1")
	t.Setenv("RELOADER_TEST_ENV", "production")
	t.Setenv("RELOADER_TEST_PORT", "90")
	t.Setenv("RELOADER_TEST_CLUSTER", "us-east-1")
}

func BenchmarkNormalize(b *testing.B) {
	setupBenchmarkEnv(b)

	sizes := []struct {
		name  string
		bytes int
	}{
		{"10KB", 10 * 1024},
		{"1MB", 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
		{"50MB", 50 * 1024 * 1024},
	}

	for _, sz := range sizes {
		data := generateConfigData(sz.bytes)
		dir := b.TempDir()
		input := filepath.Join(dir, "input.yaml")
		testutil.Ok(b, os.WriteFile(input, data, 0644))

		r := New(log.NewNopLogger(), prometheus.NewRegistry(), &Options{
			TolerateEnvVarExpansionErrors: true,
		})

		b.Run(fmt.Sprintf("Old/%s", sz.name), func(b *testing.B) {
			output := filepath.Join(dir, "output-old.yaml")
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := normalizeOld(r, input, output); err != nil {
					b.Fatalf("normalizeOld error: %v", err)
				}
			}
		})

		b.Run(fmt.Sprintf("Streaming/%s", sz.name), func(b *testing.B) {
			output := filepath.Join(dir, "output-streaming.yaml")
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := r.normalize(input, output); err != nil {
					b.Fatalf("normalize streaming error: %v", err)
				}
			}
		})
	}
}

func BenchmarkNormalize_Gzip(b *testing.B) {
	setupBenchmarkEnv(b)

	sizes := []struct {
		name  string
		bytes int
	}{
		{"1MB", 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
	}

	for _, sz := range sizes {
		data := generateConfigData(sz.bytes)
		dir := b.TempDir()
		input := filepath.Join(dir, "input.yaml.gz")

		var gzBuf bytes.Buffer
		gw := gzip.NewWriter(&gzBuf)
		_, err := gw.Write(data)
		testutil.Ok(b, err)
		testutil.Ok(b, gw.Close())
		testutil.Ok(b, os.WriteFile(input, gzBuf.Bytes(), 0644))

		r := New(log.NewNopLogger(), prometheus.NewRegistry(), &Options{
			TolerateEnvVarExpansionErrors: true,
		})

		b.Run(fmt.Sprintf("Old/%s", sz.name), func(b *testing.B) {
			output := filepath.Join(dir, "output-old.yaml")
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := normalizeOld(r, input, output); err != nil {
					b.Fatalf("normalizeOld error: %v", err)
				}
			}
		})

		b.Run(fmt.Sprintf("Streaming/%s", sz.name), func(b *testing.B) {
			output := filepath.Join(dir, "output-streaming.yaml")
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := r.normalize(input, output); err != nil {
					b.Fatalf("normalize streaming error: %v", err)
				}
			}
		})
	}
}

func generateRealisticConfigData(targetBytes int) []byte {
	var buf bytes.Buffer
	buf.WriteString("global:\n  scrape_interval: 15s\n  external_labels:\n    replica: '$(RELOADER_TEST_REPLICA)'\n    env: '$(RELOADER_TEST_ENV)'\n    cluster: '$(RELOADER_TEST_CLUSTER)'\nrule_files:\n")
	rulePattern := `  - 'rules/recording_rules_group_%06d.yaml'
`
	i := 0
	for buf.Len() < targetBytes {
		fmt.Fprintf(&buf, rulePattern, i)
		i++
	}
	return buf.Bytes()
}

func BenchmarkNormalize_RealisticConfig(b *testing.B) {
	setupBenchmarkEnv(b)

	sizes := []struct {
		name  string
		bytes int
	}{
		{"10KB", 10 * 1024},
		{"1MB", 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
		{"50MB", 50 * 1024 * 1024},
	}

	for _, sz := range sizes {
		data := generateRealisticConfigData(sz.bytes)
		dir := b.TempDir()
		input := filepath.Join(dir, "input.yaml")
		testutil.Ok(b, os.WriteFile(input, data, 0644))

		r := New(log.NewNopLogger(), prometheus.NewRegistry(), &Options{
			TolerateEnvVarExpansionErrors: true,
		})

		b.Run(fmt.Sprintf("Old/%s", sz.name), func(b *testing.B) {
			output := filepath.Join(dir, "output-old.yaml")
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := normalizeOld(r, input, output); err != nil {
					b.Fatalf("normalizeOld error: %v", err)
				}
			}
		})

		b.Run(fmt.Sprintf("Streaming/%s", sz.name), func(b *testing.B) {
			output := filepath.Join(dir, "output-streaming.yaml")
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := r.normalize(input, output); err != nil {
					b.Fatalf("normalize streaming error: %v", err)
				}
			}
		})
	}
}

func BenchmarkExpandEnv(b *testing.B) {
	setupBenchmarkEnv(b)

	sizes := []struct {
		name  string
		bytes int
	}{
		{"10KB", 10 * 1024},
		{"1MB", 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
	}

	for _, sz := range sizes {
		data := generateConfigData(sz.bytes)
		r := New(log.NewNopLogger(), prometheus.NewRegistry(), &Options{
			TolerateEnvVarExpansionErrors: true,
		})

		b.Run(fmt.Sprintf("Old/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				res, err := expandEnvOld(r, data)
				if err != nil {
					b.Fatalf("expandEnvOld error: %v", err)
				}
				_ = res
			}
		})

		b.Run(fmt.Sprintf("Streaming/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				var out bytes.Buffer
				if err := r.expandEnvStream(bytes.NewReader(data), &out); err != nil {
					b.Fatalf("expandEnvStream error: %v", err)
				}
			}
		})
	}
}

// TestReloader_Normalize_Equivalence tests that old and streaming normalize produce identical outputs.
func TestReloader_Normalize_Equivalence(t *testing.T) {
	setupTestEnv(t)

	tests := []struct {
		name     string
		content  string
		gzipData bool
		tolerate bool
	}{
		{
			name:    "simple config",
			content: "hello: world\nval: $(RELOADER_TEST_ENV)\n",
		},
		{
			name:     "multiple vars with special characters",
			content:  "$$$($(RELOADER_TEST_ENV))$$$$(RELOADER_TEST_PORT)$(NOT_SET_VAR)",
			tolerate: true,
		},
		{
			name:    "empty config",
			content: "",
		},
		{
			name:     "gzip config",
			content:  generateLargeTestConfig(1000),
			gzipData: true,
		},
		{
			name:    "large config with rules",
			content: generateLargeTestConfig(5000),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			input := filepath.Join(dir, "input")
			if tc.gzipData {
				var gzBuf bytes.Buffer
				gw := gzip.NewWriter(&gzBuf)
				_, err := gw.Write([]byte(tc.content))
				testutil.Ok(t, err)
				testutil.Ok(t, gw.Close())
				testutil.Ok(t, os.WriteFile(input, gzBuf.Bytes(), 0644))
			} else {
				testutil.Ok(t, os.WriteFile(input, []byte(tc.content), 0644))
			}

			outOld := filepath.Join(dir, "out-old")
			outNew := filepath.Join(dir, "out-new")

			rOld := New(log.NewNopLogger(), prometheus.NewRegistry(), &Options{
				TolerateEnvVarExpansionErrors: tc.tolerate,
			})
			rNew := New(log.NewNopLogger(), prometheus.NewRegistry(), &Options{
				TolerateEnvVarExpansionErrors: tc.tolerate,
			})

			errOld := normalizeOld(rOld, input, outOld)
			errNew := rNew.normalize(input, outNew)

			if errOld != nil {
				testutil.NotOk(t, errNew)
				return
			}
			testutil.Ok(t, errNew)

			contentOld, err := os.ReadFile(outOld)
			testutil.Ok(t, err)
			contentNew, err := os.ReadFile(outNew)
			testutil.Ok(t, err)

			testutil.Equals(t, string(contentOld), string(contentNew))
		})
	}
}

// chunkReader splits reads into fixed small chunks to test boundary handling.
type chunkReader struct {
	r         io.Reader
	chunkSize int
}

func (cr *chunkReader) Read(p []byte) (n int, err error) {
	toRead := cr.chunkSize
	if toRead > len(p) {
		toRead = len(p)
	}
	buf := make([]byte, toRead)
	n, err = cr.r.Read(buf)
	if n > 0 {
		copy(p, buf[:n])
	}
	return n, err
}

func TestReloader_ExpandEnvStream_ChunkBoundaries(t *testing.T) {
	setupTestEnv(t)

	testCases := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"$", "$"},
		{"$$", "$$"},
		{"$()", "$()"},
		{"$(", "$("},
		{"$(A", "$(A"},
		{"$(RELOADER_TEST_ENV)", "production"},
		{"$(RELOADER_TEST_ENV)$(RELOADER_TEST_PORT)", "production90"},
		{"prefix $(RELOADER_TEST_ENV) middle $(RELOADER_TEST_PORT) suffix", "prefix production middle 90 suffix"},
		{"$(UNKNOWN-VAR)", "$(UNKNOWN-VAR)"},
		{"$$($(RELOADER_TEST_ENV))", "$$(production)"},
		{"$(RELOADER_TEST_ENV", "$(RELOADER_TEST_ENV"},
	}

	chunkSizes := []int{1, 2, 3, 5, 7, 13, 1024}

	r := New(log.NewNopLogger(), prometheus.NewRegistry(), &Options{
		TolerateEnvVarExpansionErrors: true,
	})

	for _, tc := range testCases {
		for _, sz := range chunkSizes {
			t.Run(fmt.Sprintf("chunk_%d_%s", sz, tc.input), func(t *testing.T) {
				cr := &chunkReader{r: strings.NewReader(tc.input), chunkSize: sz}
				var out bytes.Buffer
				err := r.expandEnvStream(cr, &out)
				testutil.Ok(t, err)
				testutil.Equals(t, tc.expected, out.String())
			})
		}
	}
}

func generateLargeTestConfig(lines int) string {
	var sb strings.Builder
	sb.WriteString("global:\n  scrape_interval: 15s\nscrape_configs:\n")
	for i := 0; i < lines; i++ {
		fmt.Fprintf(&sb, "  - job_name: 'job-%d'\n    params:\n      env: ['$(RELOADER_TEST_ENV)']\n", i)
	}
	return sb.String()
}
