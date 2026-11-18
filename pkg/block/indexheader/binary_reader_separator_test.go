// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestShortestSeparator(t *testing.T) {
	for _, tc := range []struct {
		prev, cur, exp string
	}{
		{prev: "a", cur: "b", exp: "b"},
		{prev: "abc", cur: "abx", exp: "abx"},
		{prev: "abc", cur: "abxyz", exp: "abx"},
		{prev: "ab", cur: "abc", exp: "abc"},
		{prev: "ab", cur: "abcdef", exp: "abc"},
		{prev: "", cur: "a", exp: "a"},
		{prev: "pod-1", cur: "pod-10", exp: "pod-10"},
		{prev: "service/eu/pod-0099", cur: "service/eu/pod-0100", exp: "service/eu/pod-01"},
		// Not a valid table order: the whole value is kept rather than panicking.
		{prev: "abc", cur: "abc", exp: "abc"},
		{prev: "abcd", cur: "abc", exp: "abc"},
	} {
		got := shortestSeparator([]byte(tc.prev), []byte(tc.cur))
		testutil.Equals(t, tc.exp, got, "prev=%q cur=%q", tc.prev, tc.cur)
		if tc.prev < tc.cur {
			testutil.Assert(t, tc.prev < got && got <= tc.cur, "separator %q not in (%q, %q]", got, tc.prev, tc.cur)
			testutil.Assert(t, strings.HasPrefix(tc.cur, got))
		}
	}
}

// separatorTestValues returns label values that stress separators: long shared prefixes,
// values that are prefixes of other values, and numeric runs that do not sort numerically.
func separatorTestValues() []string {
	var vs []string
	for i := range 300 {
		vs = append(vs, fmt.Sprintf("cluster-eu-west-1/namespace-observability/deployment-ingester/pod-%04d", i))
	}
	for i := range 200 {
		vs = append(vs, fmt.Sprintf("svc-%d", i)) // svc-1, svc-10, svc-100, ...
	}
	p := ""
	for _, c := range "abcdefghijklmnopqrstuvwxyz" {
		p += string(c)
		vs = append(vs, p) // a, ab, abc, ...
	}
	vs = append(vs, "z", "zz", "zzz", "é", "éé", "日本", "日本語")
	slices.Sort(vs)
	return slices.Compact(vs)
}

func TestBinaryReader_SeparatorsMatchIndex(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	values := separatorTestValues()
	var series []labels.Labels
	for i, v := range values {
		series = append(series, labels.FromStrings("__name__", "metric", "target", v, "shard", fmt.Sprintf("%d", i%7)))
	}

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	id, err := e2eutil.CreateBlock(ctx, tmpDir, series, 1, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, id.String()), metadata.NoneFunc))

	indexFile, err := fileutil.OpenMmapFile(filepath.Join(tmpDir, id.String(), block.IndexFilename))
	testutil.Ok(t, err)
	defer func() { _ = indexFile.Close() }()
	ib := realByteSlice(indexFile.Bytes())

	indexReader, err := index.NewReader(ib, index.DecodePostingsRaw)
	testutil.Ok(t, err)
	defer func() { _ = indexReader.Close() }()
	expRanges, err := indexReader.PostingsRanges()
	testutil.Ok(t, err)

	// Queries for values that do not exist but sit between, before or after existing ones.
	exists := map[string]bool{}
	var missing []string
	for _, v := range values {
		exists[v] = true
	}
	for _, v := range values {
		for _, q := range []string{v + "\x00", v + "0", v[:len(v)/2], v[:len(v)-1], "!" + v, v + "~~~~"} {
			if !exists[q] {
				missing = append(missing, q)
			}
		}
	}
	missing = append(missing, "", "\x00", "\U0010FFFF")

	fn := filepath.Join(tmpDir, id.String(), block.IndexHeaderFilename)
	_, err = WriteBinary(ctx, bkt, id, fn, dummyHistogram)
	testutil.Ok(t, err)

	for _, sampling := range []int{1, 2, 3, 7, 32} {
		t.Run(fmt.Sprintf("sampling=%d", sampling), func(t *testing.T) {
			br, err := NewBinaryReader(ctx, log.NewNopLogger(), nil, tmpDir, id, sampling, NewBinaryReaderMetrics(nil))
			testutil.Ok(t, err)
			defer func() { testutil.Ok(t, br.Close()) }()

			// Every existing value and LabelValues of every name match the index.
			compareIndexToHeader(t, ib, br)

			// Sampled keys: first and last of each name are full values, the others are
			// strict prefixes of their value, and keys stay strictly increasing.
			for name, e := range br.postings {
				if name == "" {
					continue
				}
				vals, err := br.LabelValues(name)
				testutil.Ok(t, err)
				testutil.Equals(t, vals[0], e.offsets[0].value, "first key of %q", name)
				testutil.Equals(t, vals[len(vals)-1], e.offsets[len(e.offsets)-1].value, "last key of %q", name)
				for i := 1; i < len(e.offsets); i++ {
					testutil.Assert(t, e.offsets[i-1].value < e.offsets[i].value, "keys of %q not increasing at %d", name, i)
				}
			}

			for _, q := range missing {
				_, err := br.PostingsOffset("target", q)
				testutil.Equals(t, NotFoundRangeErr, err, "missing value %q", q)
			}

			// Batched lookups mixing existing and missing values agree with single lookups
			// and with the index.
			rng := rand.New(rand.NewSource(int64(sampling)))
			all := append(slices.Clone(values), missing...)
			slices.Sort(all)
			for n := range 600 {
				var qs []string
				if n%2 == 0 {
					// Random sorted batch of existing and missing values.
					qs = make([]string, 1+rng.Intn(40))
					for i := range qs {
						qs[i] = all[rng.Intn(len(all))]
					}
				} else {
					// Run of neighbors with random gaps, crossing sampled entries.
					start, size := rng.Intn(len(all)), 1+rng.Intn(120)
					for j := start; j < len(all) && len(qs) < size; j += 1 + rng.Intn(3) {
						qs = append(qs, all[j])
					}
				}
				slices.Sort(qs)
				got, err := br.PostingsOffsets("target", qs...)
				testutil.Ok(t, err)
				testutil.Equals(t, len(qs), len(got))
				for i, q := range qs {
					if !exists[q] {
						testutil.Equals(t, NotFoundRange, got[i], "batched missing %q", q)
						continue
					}
					exp := expRanges[labels.Label{Name: "target", Value: q}]
					testutil.Equals(t, exp.Start, got[i].Start, "batched %q", q)
					testutil.Assert(t, exp.End <= got[i].End, "batched %q end %d before %d", q, got[i].End, exp.End)
					single, err := br.PostingsOffset("target", q)
					testutil.Ok(t, err)
					testutil.Equals(t, single, got[i], "batched vs single %q", q)
				}
			}
		})
	}
}
