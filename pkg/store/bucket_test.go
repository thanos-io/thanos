package store

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

func TestBucketBlock_matches(t *testing.T) {
	makeMeta := func(mint, maxt int64, lset map[string]string) *block.Meta {
		return &block.Meta{
			BlockMeta: tsdb.BlockMeta{
				MinTime: mint,
				MaxTime: maxt,
			},
			Thanos: block.ThanosMeta{Labels: lset},
		}
	}
	cases := []struct {
		meta             *block.Meta
		mint, maxt       int64
		matchers         []labels.Matcher
		expBlockMatchers []labels.Matcher
		ok               bool
	}{
		{
			meta: makeMeta(100, 200, nil),
			mint: 0,
			maxt: 99,
			ok:   false,
		},
		{
			meta: makeMeta(100, 200, nil),
			mint: 0,
			maxt: 100,
			ok:   true,
		},
		{
			meta: makeMeta(100, 200, nil),
			mint: 201,
			maxt: 250,
			ok:   false,
		},
		{
			meta: makeMeta(100, 200, nil),
			mint: 200,
			maxt: 250,
			ok:   true,
		},
		{
			meta: makeMeta(100, 200, nil),
			mint: 150,
			maxt: 160,
			matchers: []labels.Matcher{
				labels.NewEqualMatcher("a", "b"),
			},
			expBlockMatchers: []labels.Matcher{
				labels.NewEqualMatcher("a", "b"),
			},
			ok: true,
		},
		{
			meta: makeMeta(100, 200, map[string]string{"a": "b"}),
			mint: 150,
			maxt: 160,
			matchers: []labels.Matcher{
				labels.NewEqualMatcher("a", "b"),
			},
			ok: true,
		},
		{
			meta: makeMeta(100, 200, map[string]string{"a": "b"}),
			mint: 150,
			maxt: 160,
			matchers: []labels.Matcher{
				labels.NewEqualMatcher("a", "c"),
			},
			ok: false,
		},
		{
			meta: makeMeta(100, 200, map[string]string{"a": "b"}),
			mint: 150,
			maxt: 160,
			matchers: []labels.Matcher{
				labels.NewEqualMatcher("a", "b"),
				labels.NewEqualMatcher("d", "e"),
			},
			expBlockMatchers: []labels.Matcher{
				labels.NewEqualMatcher("d", "e"),
			},
			ok: true,
		},
	}

	for i, c := range cases {
		b := &bucketBlock{meta: c.meta}
		blockMatchers, ok := b.blockMatchers(c.mint, c.maxt, c.matchers...)
		testutil.Assert(t, c.ok == ok, "test case %d failed", i)
		testutil.Equals(t, c.expBlockMatchers, blockMatchers)
	}
}
