// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/exp/api/remote"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	prommetadata "github.com/prometheus/prometheus/model/metadata"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/agent"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/extpromql"
)

type ruleBenchMode struct {
	name                    string
	enableMetadataWAL       bool
	enableTypeAndUnitLabels bool
	appendMetadata          bool
	includeHelpAndUnit      bool
}

var ruleBenchModes = []ruleBenchMode{
	{
		name:                    "none",
		enableMetadataWAL:       false,
		enableTypeAndUnitLabels: false,
		appendMetadata:          false,
	},
	{
		name:                    "type-and-unit-labels",
		enableMetadataWAL:       false,
		enableTypeAndUnitLabels: true,
		appendMetadata:          false,
		includeHelpAndUnit:      true,
	},
	{
		name:                    "metadata-wal-deduced",
		enableMetadataWAL:       true,
		enableTypeAndUnitLabels: false,
		appendMetadata:          true,
		includeHelpAndUnit:      false,
	},
	{
		name:                    "metadata-wal-full",
		enableMetadataWAL:       true,
		enableTypeAndUnitLabels: false,
		appendMetadata:          true,
		includeHelpAndUnit:      true,
	},
	{
		name:                    "both",
		enableMetadataWAL:       true,
		enableTypeAndUnitLabels: true,
		appendMetadata:          true,
		includeHelpAndUnit:      true,
	},
}

func makeRuleBenchDataset(numSeries int, mode ruleBenchMode) ([]labels.Labels, []prommetadata.Metadata) {
	const numRules = 50
	lsets := make([]labels.Labels, numSeries)
	metas := make([]prommetadata.Metadata, numSeries)

	for i := range numSeries {
		ruleIdx := i % numRules
		mType := model.MetricTypeGauge
		unit := ""
		if ruleIdx%2 == 0 {
			mType = model.MetricTypeCounter
			if mode.includeHelpAndUnit {
				unit = "requests"
			}
		} else if mode.includeHelpAndUnit {
			unit = "seconds"
		}

		pairs := []string{
			"__name__", fmt.Sprintf("job:http_requests_rule_%02d:rate5m", ruleIdx),
			"cluster", fmt.Sprintf("cluster-%02d", i%8),
			"namespace", fmt.Sprintf("ns-team-%03d", i%50),
			"service", fmt.Sprintf("svc-%03d", i%100),
			"instance", fmt.Sprintf("10.0.%d.%d:8080", (i/250)%255, i%250),
		}
		if mode.enableTypeAndUnitLabels {
			pairs = append(pairs, "__type__", string(mType))
			if unit != "" {
				pairs = append(pairs, "__unit__", unit)
			}
		}
		lsets[i] = labels.FromStrings(pairs...)

		if mode.appendMetadata {
			meta := prommetadata.Metadata{Type: mType}
			if mode.includeHelpAndUnit {
				meta.Unit = unit
				meta.Help = fmt.Sprintf("Recorded rate for service rule %02d across cluster instances.", ruleIdx)
			}
			metas[i] = meta
		}
	}
	return lsets, metas
}

func dirBytes(tb testing.TB, dir string) int64 {
	tb.Helper()
	var total int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	testutil.Ok(tb, err)
	return total
}

func heapAllocBytes() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

func newBenchAgentOpts(enableMetadataWAL bool) *agent.Options {
	opts := agent.DefaultOptions()
	opts.WALCompression = compression.Snappy
	opts.NoLockfile = true
	opts.EnableMetadataWALRecords = enableMetadataWAL
	return opts
}

// Recommended CLI invocation:
//
//	export bench=ruler_eval_append && go test -tags slicelabels ./cmd/thanos -run '^$' -bench '^BenchmarkStatelessRuleStorage_EvalAndAppend$' -benchtime 1s -count 6 -cpu 2 -timeout 999m -benchmem | tee ${bench}.txt
func BenchmarkStatelessRuleStorage_EvalAndAppend(b *testing.B) {
	const (
		numRules         = 20
		seriesPerRule    = 200
		totalSeriesCount = numRules * seriesPerRule
	)

	baseVec := make([]labels.Labels, seriesPerRule)
	for i := range seriesPerRule {
		baseVec[i] = labels.FromStrings(
			"__name__", "http_requests_total",
			"cluster", fmt.Sprintf("cluster-%02d", i%8),
			"namespace", fmt.Sprintf("ns-team-%03d", i%50),
			"instance", fmt.Sprintf("10.0.%d.%d:8080", (i/250)%255, i%250),
		)
	}

	for _, mode := range ruleBenchModes {
		b.Run("mode="+mode.name, func(b *testing.B) {
			dir := b.TempDir()
			db, err := agent.Open(slog.New(slog.DiscardHandler), nil, nil, dir, newBenchAgentOpts(mode.enableMetadataWAL))
			testutil.Ok(b, err)
			b.Cleanup(func() { testutil.Ok(b, db.Close()) })

			groupRules := make([]rules.Rule, numRules)
			for r := range numRules {
				expr, err := extpromql.ParseExpr("http_requests_total")
				testutil.Ok(b, err)
				var meta prommetadata.Metadata
				if mode.includeHelpAndUnit {
					meta = prommetadata.Metadata{
						Type: model.MetricTypeCounter,
						Unit: "requests",
						Help: fmt.Sprintf("Recorded request total for rule %02d.", r),
					}
				}
				groupRules[r] = rules.NewRecordingRuleWithMetadata(
					fmt.Sprintf("job:http_requests_rule_%02d_total", r),
					expr,
					labels.EmptyLabels(),
					meta,
				)
			}

			queryFn := func(_ context.Context, _ string, t time.Time) (promql.Vector, error) {
				ts := t.UnixMilli()
				vec := make(promql.Vector, seriesPerRule)
				for i, lset := range baseVec {
					vec[i] = promql.Sample{T: ts, F: float64(i), Metric: lset}
				}
				return vec, nil
			}

			grp := rules.NewGroup(rules.GroupOptions{
				Name:     "bench",
				File:     "bench.yaml",
				Interval: 15 * time.Second,
				Rules:    groupRules,
				Opts: &rules.ManagerOptions{
					Logger:                  slog.New(slog.DiscardHandler),
					AppendableV2:            db,
					QueryFunc:               queryFn,
					AppendMetadata:          mode.appendMetadata,
					AutoRecordingMetadata:   true,
					EnableTypeAndUnitLabels: mode.enableTypeAndUnitLabels,
				},
			})

			evalTS := time.Now()
			// Seed series before timing steady-state rule group evaluation.
			grp.Eval(b.Context(), evalTS)

			walBefore := dirBytes(b, filepath.Join(dir, "wal"))
			var iters int64
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				iters++
				evalTS = evalTS.Add(15 * time.Second)
				grp.Eval(b.Context(), evalTS)
			}
			b.StopTimer()

			walDelta := dirBytes(b, filepath.Join(dir, "wal")) - walBefore
			b.ReportMetric(float64(walDelta)/float64(iters)/float64(totalSeriesCount), "wal-bytes/sample")
		})
	}
}

// Recommended CLI invocation:
//
//	export bench=ruler_append_steady && go test -tags slicelabels ./cmd/thanos -run '^$' -bench '^BenchmarkStatelessRuleStorage_AppendSteadyState$' -benchtime 1s -count 6 -cpu 2 -timeout 999m -benchmem | tee ${bench}.txt
func BenchmarkStatelessRuleStorage_AppendSteadyState(b *testing.B) {
	const numSeries = 10_000

	for _, mode := range ruleBenchModes {
		b.Run("mode="+mode.name, func(b *testing.B) {
			lsets, metas := makeRuleBenchDataset(numSeries, mode)
			dir := b.TempDir()
			db, err := agent.Open(slog.New(slog.DiscardHandler), nil, nil, dir, newBenchAgentOpts(mode.enableMetadataWAL))
			testutil.Ok(b, err)
			b.Cleanup(func() { testutil.Ok(b, db.Close()) })

			ts := time.Now().UnixMilli()
			// Seed all series before timing steady-state rule evaluations.
			app := db.AppenderV2(b.Context())
			for i := range numSeries {
				_, err := app.Append(0, lsets[i], 0, ts, float64(i), nil, nil, storage.AOptions{Metadata: metas[i]})
				testutil.Ok(b, err)
			}
			testutil.Ok(b, app.Commit())

			walBefore := dirBytes(b, filepath.Join(dir, "wal"))
			var iters int64
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				iters++
				ts += 15_000
				app := db.AppenderV2(b.Context())
				for i := range numSeries {
					_, err := app.Append(0, lsets[i], 0, ts, float64(i), nil, nil, storage.AOptions{Metadata: metas[i]})
					if err != nil {
						b.Fatal(err)
					}
				}
				if err := app.Commit(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			walDelta := dirBytes(b, filepath.Join(dir, "wal")) - walBefore
			b.ReportMetric(float64(walDelta)/float64(iters), "wal-bytes/op")
		})
	}
}

// Recommended CLI invocation:
//
//	export bench=ruler_append_new && go test -tags slicelabels ./cmd/thanos -run '^$' -bench '^BenchmarkStatelessRuleStorage_AppendNewSeries$' -benchtime 1s -count 6 -cpu 2 -timeout 999m -benchmem | tee ${bench}.txt
func BenchmarkStatelessRuleStorage_AppendNewSeries(b *testing.B) {
	const numSeries = 10_000

	for _, mode := range ruleBenchModes {
		b.Run("mode="+mode.name, func(b *testing.B) {
			lsets, metas := makeRuleBenchDataset(numSeries, mode)
			ts := time.Now().UnixMilli()

			var (
				lastWALBytes  int64
				lastHeapBytes int64
			)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				dir := b.TempDir()
				heapBefore := heapAllocBytes()
				db, err := agent.Open(slog.New(slog.DiscardHandler), nil, nil, dir, newBenchAgentOpts(mode.enableMetadataWAL))
				testutil.Ok(b, err)
				b.StartTimer()

				app := db.AppenderV2(b.Context())
				for i := range numSeries {
					_, err := app.Append(0, lsets[i], 0, ts, float64(i), nil, nil, storage.AOptions{Metadata: metas[i]})
					if err != nil {
						b.Fatal(err)
					}
				}
				if err := app.Commit(); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				heapAfter := heapAllocBytes()
				if heapAfter > heapBefore {
					lastHeapBytes = int64(heapAfter - heapBefore)
				}
				lastWALBytes = dirBytes(b, filepath.Join(dir, "wal"))
				testutil.Ok(b, db.Close())
				b.StartTimer()
			}
			b.StopTimer()

			b.ReportMetric(float64(lastWALBytes)/float64(numSeries), "wal-bytes/series")
			b.ReportMetric(float64(lastHeapBytes)/float64(numSeries), "heap-bytes/series")
		})
	}
}

// Recommended CLI invocation:
//
//	export bench=ruler_wal_checkpoint_replay && go test -tags slicelabels ./cmd/thanos -run '^$' -bench '^BenchmarkStatelessRuleStorage_WALCheckpointAndReplay$' -benchtime 5x -count 6 -cpu 2 -timeout 999m -benchmem | tee ${bench}.txt
func BenchmarkStatelessRuleStorage_WALCheckpointAndReplay(b *testing.B) {
	const numSeries = 20_000

	for _, mode := range ruleBenchModes {
		srcDir := b.TempDir()
		lsets, metas := makeRuleBenchDataset(numSeries, mode)
		db, err := agent.Open(slog.New(slog.DiscardHandler), nil, nil, srcDir, newBenchAgentOpts(mode.enableMetadataWAL))
		testutil.Ok(b, err)

		ts := time.Now().UnixMilli()
		app := db.AppenderV2(b.Context())
		for i := range numSeries {
			_, err := app.Append(0, lsets[i], 0, ts, float64(i), nil, nil, storage.AOptions{Metadata: metas[i]})
			testutil.Ok(b, err)
		}
		testutil.Ok(b, app.Commit())
		testutil.Ok(b, db.Close())

		b.Run("op=checkpoint/mode="+mode.name, func(b *testing.B) {
			var lastCheckpointBytes int64
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				workDir := b.TempDir()
				walDir := filepath.Join(workDir, "wal")
				testutil.Ok(b, os.CopyFS(walDir, os.DirFS(filepath.Join(srcDir, "wal"))))
				w, err := wlog.New(slog.New(slog.DiscardHandler), nil, walDir, compression.Snappy)
				testutil.Ok(b, err)
				first, last, err := wlog.Segments(walDir)
				testutil.Ok(b, err)
				b.StartTimer()

				_, err = wlog.Checkpoint(slog.New(slog.DiscardHandler), w, first, last, func(chunks.HeadSeriesRef) bool { return true }, ts-1, false)
				if err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				cpDir, _, err := wlog.LastCheckpoint(walDir)
				testutil.Ok(b, err)
				lastCheckpointBytes = dirBytes(b, cpDir)
				testutil.Ok(b, w.Close())
				b.StartTimer()
			}
			b.StopTimer()

			b.ReportMetric(float64(lastCheckpointBytes)/float64(numSeries), "checkpoint-bytes/series")
		})

		b.Run("op=replay/mode="+mode.name, func(b *testing.B) {
			var lastHeapBytes int64
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				heapBefore := heapAllocBytes()
				b.StartTimer()

				replayed, err := agent.Open(slog.New(slog.DiscardHandler), nil, nil, srcDir, newBenchAgentOpts(mode.enableMetadataWAL))
				if err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				heapAfter := heapAllocBytes()
				if heapAfter > heapBefore {
					lastHeapBytes = int64(heapAfter - heapBefore)
				}
				testutil.Ok(b, replayed.Close())
				b.StartTimer()
			}
			b.StopTimer()

			b.ReportMetric(float64(lastHeapBytes)/float64(numSeries), "heap-bytes/series")
		})
	}
}

// Recommended CLI invocation:
//
//	export bench=ruler_rw2_e2e && go test -tags slicelabels ./cmd/thanos -run '^$' -bench '^BenchmarkStatelessRuleStorage_RemoteWriteV2Queue$' -benchtime 3x -count 6 -cpu 2 -timeout 999m -benchmem | tee ${bench}.txt
func BenchmarkStatelessRuleStorage_RemoteWriteV2Queue(b *testing.B) {
	const numSeries = 10_000

	var (
		receivedSamples atomic.Int64
		wireBytes       atomic.Int64
		doneMu          sync.Mutex
		doneCh          chan struct{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		wireBytes.Add(int64(len(compressed)))
		raw, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req writev2.Request
		if err := req.Unmarshal(raw); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var samples int64
		for _, ts := range req.Timeseries {
			samples += int64(len(ts.Samples))
		}
		w.Header().Set("X-Prometheus-Remote-Write-Samples-Written", strconv.FormatInt(samples, 10))
		w.WriteHeader(http.StatusNoContent)

		if receivedSamples.Add(samples) >= numSeries {
			doneMu.Lock()
			if doneCh != nil {
				close(doneCh)
				doneCh = nil
			}
			doneMu.Unlock()
		}
	}))
	b.Cleanup(srv.Close)

	rwCfgYAML := []byte(fmt.Sprintf(`remote_write:
  - url: %s
    protobuf_message: %s
    queue_config:
      batch_send_deadline: 20ms
      max_samples_per_send: 2000
      min_shards: 4
      max_shards: 4
`, srv.URL, remote.WriteV2MessageType))
	extLabels := labels.FromStrings("replica", "0")

	for _, mode := range ruleBenchModes {
		b.Run("mode="+mode.name, func(b *testing.B) {
			lsets, metas := makeRuleBenchDataset(numSeries, mode)

			var (
				lastHeapBytes int64
				lastWALBytes  int64
				lastWireBytes int64
			)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				dir := b.TempDir()
				receivedSamples.Store(0)
				wireBytes.Store(0)
				waitCh := make(chan struct{})
				doneMu.Lock()
				doneCh = waitCh
				doneMu.Unlock()

				heapBefore := heapAllocBytes()
				store, err := openStatelessRuleStorage(
					log.NewNopLogger(),
					prometheus.NewRegistry(),
					dir,
					rwCfgYAML,
					extLabels,
					newBenchAgentOpts(mode.enableMetadataWAL),
					mode.enableTypeAndUnitLabels,
				)
				testutil.Ok(b, err)
				ts := time.Now().Add(time.Second).UnixMilli()
				b.StartTimer()

				app := store.AppenderV2(b.Context())
				for i := range numSeries {
					_, err := app.Append(0, lsets[i], 0, ts, float64(i), nil, nil, storage.AOptions{Metadata: metas[i]})
					if err != nil {
						b.Fatal(err)
					}
				}
				if err := app.Commit(); err != nil {
					b.Fatal(err)
				}

				select {
				case <-waitCh:
				case <-time.After(15 * time.Second):
					b.Fatalf("timed out waiting for %d samples, got %d", numSeries, receivedSamples.Load())
				}

				b.StopTimer()
				heapAfter := heapAllocBytes()
				if heapAfter > heapBefore {
					lastHeapBytes = int64(heapAfter - heapBefore)
				}
				lastWALBytes = dirBytes(b, filepath.Join(dir, "wal"))
				lastWireBytes = wireBytes.Load()
				testutil.Ok(b, store.Close())
				b.StartTimer()
			}
			b.StopTimer()

			b.ReportMetric(float64(lastHeapBytes)/float64(numSeries), "heap-bytes/series")
			b.ReportMetric(float64(lastWALBytes)/float64(numSeries), "wal-bytes/series")
			b.ReportMetric(float64(lastWireBytes)/float64(numSeries), "rw2-wire-bytes/series")
		})
	}
}
