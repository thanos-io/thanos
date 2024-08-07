// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"io"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/efficientgo/core/testutil"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
)

func TestStreamedSnappyMaximumDecodedLen(t *testing.T) {
	t.Run("compressed", func(t *testing.T) {
		b := make([]byte, 100)
		for i := 0; i < 100; i++ {
			b[i] = 0x42
		}

		snappyEncoded := &bytes.Buffer{}

		sw := s2.NewWriter(snappyEncoded, s2.WriterSnappyCompat(), s2.WriterBestCompression())

		_, err := sw.Write(b)
		testutil.Ok(t, err)

		testutil.Ok(t, sw.Close())

		maxLen, err := maximumDecodedLenSnappyStreamed(snappyEncoded.Bytes())
		testutil.Ok(t, err)
		t.Log(maxLen)
		testutil.Assert(t, maxLen == 100)
	})
	t.Run("random", func(t *testing.T) {
		for i := 10000; i < 30000; i++ {
			b := make([]byte, i)
			_, err := crand.Read(b)
			testutil.Ok(t, err)

			snappyEncoded := &bytes.Buffer{}

			sw := s2.NewWriter(snappyEncoded, s2.WriterSnappyCompat())

			_, err = sw.Write(b)
			testutil.Ok(t, err)

			testutil.Ok(t, sw.Close())

			maxLen, err := maximumDecodedLenSnappyStreamed(snappyEncoded.Bytes())
			testutil.Ok(t, err)
			testutil.Assert(t, maxLen > 100)
			testutil.Assert(t, maxLen < 30000)
		}
	})
}

func TestDiffVarintCodec(t *testing.T) {
	chunksDir := t.TempDir()

	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = chunksDir
	headOpts.ChunkRange = 1000
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, h.Close())
	}()

	appendTestData(t, h.Appender(context.Background()), 1e6)

	idx, err := h.Index()
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, idx.Close())
	}()

	ctx := context.TODO()
	postingsMap := map[string]index.Postings{
		"all":      allPostings(ctx, t, idx),
		`n="1"`:    matchPostings(ctx, t, idx, labels.MustNewMatcher(labels.MatchEqual, "n", "1"+storetestutil.LabelLongSuffix)),
		`j="foo"`:  matchPostings(ctx, t, idx, labels.MustNewMatcher(labels.MatchEqual, "j", "foo")),
		`j!="foo"`: matchPostings(ctx, t, idx, labels.MustNewMatcher(labels.MatchNotEqual, "j", "foo")),
		`i=~".*"`:  matchPostings(ctx, t, idx, labels.MustNewMatcher(labels.MatchRegexp, "i", ".*")),
		`i=~".+"`:  matchPostings(ctx, t, idx, labels.MustNewMatcher(labels.MatchRegexp, "i", ".+")),
		`i=~"1.+"`: matchPostings(ctx, t, idx, labels.MustNewMatcher(labels.MatchRegexp, "i", "1.+")),
		`i=~"^$"'`: matchPostings(ctx, t, idx, labels.MustNewMatcher(labels.MatchRegexp, "i", "^$")),
		`i!~""`:    matchPostings(ctx, t, idx, labels.MustNewMatcher(labels.MatchNotEqual, "i", "")),
		`n!="2"`:   matchPostings(ctx, t, idx, labels.MustNewMatcher(labels.MatchNotEqual, "n", "2"+storetestutil.LabelLongSuffix)),
		`i!~"2.*"`: matchPostings(ctx, t, idx, labels.MustNewMatcher(labels.MatchNotRegexp, "i", "^2.*$")),
	}

	codecs := map[string]struct {
		codingFunction   func(index.Postings, int) ([]byte, error)
		decodingFunction func([]byte, bool) (closeablePostings, error)
	}{
		"raw": {codingFunction: diffVarintEncodeNoHeader, decodingFunction: func(bytes []byte, disablePooling bool) (closeablePostings, error) {
			return newDiffVarintPostings(bytes, nil), nil
		}},
		"snappy":         {codingFunction: diffVarintSnappyEncode, decodingFunction: diffVarintSnappyDecode},
		"snappyStreamed": {codingFunction: diffVarintSnappyStreamedEncode, decodingFunction: diffVarintSnappyStreamedDecode},
	}

	for postingName, postings := range postingsMap {
		p, err := toUint64Postings(postings)
		testutil.Ok(t, err)

		for cname, codec := range codecs {
			name := cname + "/" + postingName

			t.Run(name, func(t *testing.T) {
				t.Log("postings entries:", p.len())
				t.Log("original size (4*entries):", 4*p.len(), "bytes")
				p.reset() // We reuse postings between runs, so we need to reset iterator.

				data, err := codec.codingFunction(p, p.len())
				testutil.Ok(t, err)

				t.Log("encoded size", len(data), "bytes")
				t.Logf("ratio: %0.3f", float64(len(data))/float64(4*p.len()))

				decodedPostings, err := codec.decodingFunction(data, false)
				testutil.Ok(t, err)

				p.reset()
				comparePostings(t, p, decodedPostings)
			})
		}
	}
}

func comparePostings(t *testing.T, p1, p2 index.Postings) {
	for p1.Next() {
		if !p2.Next() {
			t.Log("p1 has more values")
			t.Fail()
			return
		}

		if p1.At() != p2.At() {
			t.Logf("values differ: %d, %d", p1.At(), p2.At())
			t.Fail()
			return
		}
	}

	if p2.Next() {
		t.Log("p2 has more values")
		t.Fail()
		return
	}

	testutil.Ok(t, p1.Err())
	testutil.Ok(t, p2.Err())
}

func allPostings(ctx context.Context, t testing.TB, ix tsdb.IndexReader) index.Postings {
	k, v := index.AllPostingsKey()
	p, err := ix.Postings(ctx, k, v)
	testutil.Ok(t, err)
	return p
}

func matchPostings(ctx context.Context, t testing.TB, ix tsdb.IndexReader, m *labels.Matcher) index.Postings {
	vals, err := ix.LabelValues(ctx, m.Name)
	testutil.Ok(t, err)

	matching := []string(nil)
	for _, v := range vals {
		if m.Matches(v) {
			matching = append(matching, v)
		}
	}

	p, err := ix.Postings(ctx, m.Name, matching...)
	testutil.Ok(t, err)
	return p
}

func toUint64Postings(p index.Postings) (*uint64Postings, error) {
	var vals []storage.SeriesRef
	for p.Next() {
		vals = append(vals, p.At())
	}
	return &uint64Postings{vals: vals, ix: -1}, p.Err()
}

// Postings with no decoding step.
type uint64Postings struct {
	vals []storage.SeriesRef
	ix   int
}

func (p *uint64Postings) At() storage.SeriesRef {
	if p.ix < 0 || p.ix >= len(p.vals) {
		return 0
	}
	return p.vals[p.ix]
}

func (p *uint64Postings) Next() bool {
	if p.ix < len(p.vals)-1 {
		p.ix++
		return true
	}
	return false
}

func (p *uint64Postings) Seek(x storage.SeriesRef) bool {
	if p.At() >= x {
		return true
	}

	// We cannot do any search due to how values are stored,
	// so we simply advance until we find the right value.
	for p.Next() {
		if p.At() >= x {
			return true
		}
	}

	return false
}

func (p *uint64Postings) Err() error {
	return nil
}

func (p *uint64Postings) reset() {
	p.ix = -1
}

func (p *uint64Postings) len() int {
	return len(p.vals)
}

func BenchmarkPostingsEncodingDecoding(b *testing.B) {
	const max = 1000000
	r := rand.New(rand.NewSource(0))

	p := make([]storage.SeriesRef, max)

	for ix := 1; ix < len(p); ix++ {
		// Use normal distribution, with stddev=64 (i.e. most values are < 64).
		// This is very rough approximation of experiments with real blocks.v
		d := math.Abs(r.NormFloat64()*64) + 1

		p[ix] = p[ix-1] + storage.SeriesRef(d)
	}

	codecs := map[string]struct {
		codingFunction   func(index.Postings, int) ([]byte, error)
		decodingFunction func([]byte, bool) (closeablePostings, error)
	}{
		"raw": {codingFunction: diffVarintEncodeNoHeader, decodingFunction: func(bytes []byte, disablePooling bool) (closeablePostings, error) {
			return newDiffVarintPostings(bytes, nil), nil
		}},
		"snappy":         {codingFunction: diffVarintSnappyEncode, decodingFunction: diffVarintSnappyDecode},
		"snappyStreamed": {codingFunction: diffVarintSnappyStreamedEncode, decodingFunction: diffVarintSnappyStreamedDecode},
	}
	b.ReportAllocs()

	for _, count := range []int{10000, 100000, 1000000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			for codecName, codecFns := range codecs {
				b.Run(codecName, func(b *testing.B) {
					b.Run("encode", func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							ps := &uint64Postings{vals: p[:count]}

							_, err := codecFns.codingFunction(ps, ps.len())
							if err != nil {
								b.Fatal(err)
							}
						}
					})
					b.Run("decode", func(b *testing.B) {
						ps := &uint64Postings{vals: p[:count]}

						encoded, err := codecFns.codingFunction(ps, ps.len())
						if err != nil {
							b.Fatal(err)
						}
						b.ResetTimer()

						for i := 0; i < b.N; i++ {
							decoded, err := codecFns.decodingFunction(encoded, true)
							if err != nil {
								b.Fatal(err)
							}

							for decoded.Next() {
								var _ = decoded.At()
							}
							testutil.Ok(b, decoded.Err())
						}
					})
				})
			}
		})
	}
}

func FuzzSnappyStreamEncoding(f *testing.F) {
	f.Add(10, 123)

	f.Fuzz(func(t *testing.T, postingsCount, seedInit int) {
		if postingsCount <= 0 {
			return
		}
		r := rand.New(rand.NewSource(int64(seedInit)))
		p := make([]storage.SeriesRef, postingsCount)

		for ix := 1; ix < len(p); ix++ {
			d := math.Abs(r.NormFloat64()*math.MaxUint64) + 1

			p[ix] = p[ix-1] + storage.SeriesRef(d)
		}

		sort.Slice(p, func(i, j int) bool {
			return p[i] < p[j]
		})

		ps := &uint64Postings{vals: p}

		_, err := diffVarintSnappyStreamedEncode(ps, ps.len())
		testutil.Ok(t, err)
	})
}

func TestRegressionIssue6545(t *testing.T) {
	diffVarintPostings, err := os.ReadFile("6545postingsrepro")
	testutil.Ok(t, err)

	gotPostings := 0
	dvp := newDiffVarintPostings(diffVarintPostings, nil)
	decodedPostings := []storage.SeriesRef{}
	for dvp.Next() {
		decodedPostings = append(decodedPostings, dvp.At())
		gotPostings++
	}
	testutil.Ok(t, dvp.Err())
	testutil.Equals(t, 114024, gotPostings)

	dataToCache, err := snappyStreamedEncode(114024, diffVarintPostings)
	testutil.Ok(t, err)

	// Check that the original decompressor works well.
	sr := s2.NewReader(bytes.NewBuffer(dataToCache[3:]))
	readBytes, err := io.ReadAll(sr)
	testutil.Ok(t, err)
	testutil.Equals(t, readBytes, diffVarintPostings)

	dvp = newDiffVarintPostings(readBytes, nil)
	gotPostings = 0
	for dvp.Next() {
		gotPostings++
	}
	testutil.Equals(t, 114024, gotPostings)

	p, err := decodePostings(dataToCache)
	testutil.Ok(t, err)

	i := 0
	for p.Next() {
		post := p.At()
		testutil.Equals(t, uint64(decodedPostings[i]), uint64(post))
		i++
	}

	testutil.Ok(t, p.Err())
	testutil.Equals(t, 114024, i)
}
