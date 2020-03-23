// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestDiffVarintCodec(t *testing.T) {
	h, err := tsdb.NewHead(nil, nil, nil, 1000)
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, h.Close())
	}()

	appendTestData(t, h.Appender(), 1e6)

	idx, err := h.Index()
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, idx.Close())
	}()

	postingsMap := map[string]index.Postings{
		"all":      allPostings(t, idx),
		`n="1"`:    matchPostings(t, idx, labels.MustNewMatcher(labels.MatchEqual, "n", "1"+postingsBenchSuffix)),
		`j="foo"`:  matchPostings(t, idx, labels.MustNewMatcher(labels.MatchEqual, "j", "foo")),
		`j!="foo"`: matchPostings(t, idx, labels.MustNewMatcher(labels.MatchNotEqual, "j", "foo")),
		`i=~".*"`:  matchPostings(t, idx, labels.MustNewMatcher(labels.MatchRegexp, "i", ".*")),
		`i=~".+"`:  matchPostings(t, idx, labels.MustNewMatcher(labels.MatchRegexp, "i", ".+")),
		`i=~"1.+"`: matchPostings(t, idx, labels.MustNewMatcher(labels.MatchRegexp, "i", "1.+")),
		`i=~"^$"'`: matchPostings(t, idx, labels.MustNewMatcher(labels.MatchRegexp, "i", "^$")),
		`i!~""`:    matchPostings(t, idx, labels.MustNewMatcher(labels.MatchNotEqual, "i", "")),
		`n!="2"`:   matchPostings(t, idx, labels.MustNewMatcher(labels.MatchNotEqual, "n", "2"+postingsBenchSuffix)),
		`i!~"2.*"`: matchPostings(t, idx, labels.MustNewMatcher(labels.MatchNotRegexp, "i", "^2.*$")),
	}

	codecs := map[string]struct {
		codingFunction   func(index.Postings) ([]byte, error)
		decodingFunction func([]byte) (index.Postings, error)
	}{
		"raw":    {codingFunction: diffVarintEncodeNoHeader, decodingFunction: func(bytes []byte) (index.Postings, error) { return newDiffVarintPostings(bytes), nil }},
		"snappy": {codingFunction: diffVarintSnappyEncode, decodingFunction: diffVarintSnappyDecode},
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

				data, err := codec.codingFunction(p)
				testutil.Ok(t, err)

				t.Log("encoded size", len(data), "bytes")
				t.Logf("ratio: %0.3f", (float64(len(data)) / float64(4*p.len())))

				decodedPostings, err := codec.decodingFunction(data)
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

func allPostings(t testing.TB, ix tsdb.IndexReader) index.Postings {
	k, v := index.AllPostingsKey()
	p, err := ix.Postings(k, v)
	testutil.Ok(t, err)
	return p
}

func matchPostings(t testing.TB, ix tsdb.IndexReader, m *labels.Matcher) index.Postings {
	vals, err := ix.LabelValues(m.Name)
	testutil.Ok(t, err)

	matching := []string(nil)
	for _, v := range vals {
		if m.Matches(v) {
			matching = append(matching, v)
		}
	}

	p, err := ix.Postings(m.Name, matching...)
	testutil.Ok(t, err)
	return p
}

func toUint64Postings(p index.Postings) (*uint64Postings, error) {
	var vals []uint64
	for p.Next() {
		vals = append(vals, p.At())
	}
	return &uint64Postings{vals: vals, ix: -1}, p.Err()
}

// Postings with no decoding step.
type uint64Postings struct {
	vals []uint64
	ix   int
}

func (p *uint64Postings) At() uint64 {
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

func (p *uint64Postings) Seek(x uint64) bool {
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
