// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

const benchSeries = 100_000

// benchSeriesLabels mimics high-cardinality labels, some whose values share long prefixes
// (pod names, instances) and some random (pod UIDs, container IDs), next to a few
// low-cardinality labels.
func benchSeriesLabels(i int) labels.Labels {
	h := uint64(i+1) * 0x9E3779B97F4A7C15
	return labels.FromStrings(
		"pod_uid", fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", uint32(h>>32), uint16(h>>16), uint16(h), uint16(h>>48)^0x4000, (h*0xBF58476D1CE4E5B9)>>16),
		"__name__", fmt.Sprintf("metric_%d", i%50),
		"cluster", fmt.Sprintf("cluster-%d", i%8),
		"namespace", fmt.Sprintf("namespace-team-%03d", i%300),
		"pod", fmt.Sprintf("receiver-%d", i),
		"container_id", "containerd://"+randomHex(uint64(i), 64),
		"instance", fmt.Sprintf("ip-10-%d-%d-%d.ec2.internal:9100", i>>16&255, i>>8&255, i&255),
	)
}

// randomHex returns n deterministic pseudo-random hex characters derived from seed.
func randomHex(seed uint64, n int) string {
	var sb strings.Builder
	for sb.Len() < n {
		// splitmix64
		seed += 0x9E3779B97F4A7C15
		z := seed
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		z ^= z >> 31
		fmt.Fprintf(&sb, "%016x", z)
	}
	return sb.String()[:n]
}

func prepareBenchHeader(b *testing.B) (dir string, id ulid.ULID, names []string, values map[string][]string) {
	ctx := context.Background()
	dir = b.TempDir()
	series := make([]labels.Labels, benchSeries)
	for i := range series {
		series[i] = benchSeriesLabels(i)
	}
	bkt, err := filesystem.NewBucket(filepath.Join(dir, "bkt"))
	testutil.Ok(b, err)
	b.Cleanup(func() { _ = bkt.Close() })

	id, err = e2eutil.CreateBlock(ctx, dir, series, 1, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc, nil)
	testutil.Ok(b, err)
	testutil.Ok(b, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(dir, id.String()), metadata.NoneFunc))
	_, err = WriteBinary(ctx, bkt, id, filepath.Join(dir, id.String(), block.IndexHeaderFilename), dummyHistogram)
	testutil.Ok(b, err)

	br, err := NewBinaryReader(ctx, log.NewNopLogger(), nil, dir, id, 32, NewBinaryReaderMetrics(nil))
	testutil.Ok(b, err)
	defer func() { testutil.Ok(b, br.Close()) }()
	names, err = br.LabelNames()
	testutil.Ok(b, err)
	values = map[string][]string{}
	for _, n := range names {
		vs, err := br.LabelValues(n)
		testutil.Ok(b, err)
		// LabelValues strings point into the mmap, which Close unmaps.
		for _, v := range vs {
			values[n] = append(values[n], strings.Clone(v))
		}
	}
	return dir, id, names, values
}

func heapAfterGC() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// BenchmarkBinaryReader_InMemoryPostingOffsets reports the heap a reader keeps for its
// sampled postings offset table, and the allocations needed to build it.
func BenchmarkBinaryReader_InMemoryPostingOffsets(b *testing.B) {
	dir, id, _, _ := prepareBenchHeader(b)
	ctx := context.Background()
	for _, sampling := range []int{32, 64} {
		b.Run(fmt.Sprintf("sampling=%d", sampling), func(b *testing.B) {
			var retained float64
			b.ReportAllocs()
			for b.Loop() {
				before := heapAfterGC()
				br, err := NewBinaryReader(ctx, log.NewNopLogger(), nil, dir, id, sampling, NewBinaryReaderMetrics(nil))
				testutil.Ok(b, err)
				retained = float64(heapAfterGC() - before)
				testutil.Ok(b, br.Close())
			}
			b.ReportMetric(retained, "retained-B/reader")
		})
	}
}

// BenchmarkBinaryReader_PostingsOffset looks up existing and missing values of every label.
func BenchmarkBinaryReader_PostingsOffset(b *testing.B) {
	dir, id, names, values := prepareBenchHeader(b)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(1))
	type q struct{ name, value string }
	qs := make([]q, 4096)
	for i := range qs {
		n := names[rng.Intn(len(names))]
		v := values[n][rng.Intn(len(values[n]))]
		if i%10 == 0 {
			v += "-missing"
		}
		qs[i] = q{n, v}
	}
	for _, sampling := range []int{32, 64} {
		b.Run(fmt.Sprintf("sampling=%d", sampling), func(b *testing.B) {
			br, err := NewBinaryReader(ctx, log.NewNopLogger(), nil, dir, id, sampling, NewBinaryReaderMetrics(nil))
			testutil.Ok(b, err)
			defer func() { testutil.Ok(b, br.Close()) }()
			i := 0
			for b.Loop() {
				x := qs[i%len(qs)]
				_, _ = br.PostingsOffset(x.name, x.value)
				i++
			}
		})
	}
}

// BenchmarkBinaryReader_PostingsOffsets looks up sorted batches of values of one label,
// as set matchers and regex matchers do: runs of neighboring values, which cross sampled
// entries, and sparse sorted sets.
func BenchmarkBinaryReader_PostingsOffsets(b *testing.B) {
	dir, id, names, values := prepareBenchHeader(b)
	ctx := context.Background()
	type batch struct {
		name   string
		values []string
	}
	rng := rand.New(rand.NewSource(1))
	makeBatches := func(size int, dense bool) []batch {
		var out []batch
		for len(out) < 512 {
			n := names[rng.Intn(len(names))]
			vs := values[n]
			if len(vs) < 2*size {
				continue
			}
			var qs []string
			if dense {
				start := rng.Intn(len(vs) - size)
				qs = append(qs, vs[start:start+size]...)
			} else {
				for _, j := range rng.Perm(len(vs))[:size] {
					qs = append(qs, vs[j])
				}
				slices.Sort(qs)
			}
			out = append(out, batch{n, qs})
		}
		return out
	}
	cases := []struct {
		name    string
		batches []batch
	}{
		{"dense-run=256", makeBatches(256, true)},
		{"sparse=64", makeBatches(64, false)},
	}
	for _, sampling := range []int{32, 64} {
		br, err := NewBinaryReader(ctx, log.NewNopLogger(), nil, dir, id, sampling, NewBinaryReaderMetrics(nil))
		testutil.Ok(b, err)
		for _, c := range cases {
			b.Run(fmt.Sprintf("%s/sampling=%d", c.name, sampling), func(b *testing.B) {
				i := 0
				for b.Loop() {
					x := c.batches[i%len(c.batches)]
					_, _ = br.PostingsOffsets(x.name, x.values...)
					i++
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(len(c.batches[0].values)), "ns/value")
			})
		}
		testutil.Ok(b, br.Close())
	}
}
