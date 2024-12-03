// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"math"
	"strings"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"golang.org/x/exp/slices"

	"github.com/thanos-io/thanos/pkg/block/indexheader"
)

var emptyLazyPostings = &lazyExpandedPostings{postings: nil, matchers: nil}

// lazyExpandedPostings contains expanded postings (series IDs). If lazy posting expansion is
// enabled, it might contain matchers that can be lazily applied during series filtering time.
type lazyExpandedPostings struct {
	postings []storage.SeriesRef
	matchers []*labels.Matcher
}

func newLazyExpandedPostings(ps []storage.SeriesRef, matchers ...*labels.Matcher) *lazyExpandedPostings {
	return &lazyExpandedPostings{
		postings: ps,
		matchers: matchers,
	}
}

func (p *lazyExpandedPostings) lazyExpanded() bool {
	return p != nil && len(p.matchers) > 0
}

func optimizePostingsFetchByDownloadedBytes(r *bucketIndexReader, postingGroups []*postingGroup, seriesSize int64, seriesMatchRatio float64, lazyExpandedPostingSizeBytes prometheus.Counter) ([]*postingGroup, bool, error) {
	if len(postingGroups) <= 1 {
		return postingGroups, false, nil
	}
	// Collect posting cardinality of each posting group.
	for _, pg := range postingGroups {
		// A posting group can have either add keys or remove keys but not both the same time.
		vals := pg.addKeys
		if len(pg.removeKeys) > 0 {
			vals = pg.removeKeys
		}
		rngs, err := r.block.indexHeaderReader.PostingsOffsets(pg.name, vals...)
		if err != nil {
			return nil, false, errors.Wrapf(err, "postings offsets for %s", pg.name)
		}

		for _, rng := range rngs {
			if rng == indexheader.NotFoundRange {
				continue
			}
			if rng.End <= rng.Start {
				level.Error(r.logger).Log("msg", "invalid index range, fallback to non lazy posting optimization")
				return postingGroups, false, nil
			}
			// Each range starts from the #entries field which is 4 bytes.
			// Need to subtract it when calculating number of postings.
			// https://github.com/prometheus/prometheus/blob/v2.46.0/tsdb/docs/format/index.md.
			pg.cardinality += (rng.End - rng.Start - 4) / 4
		}
		// If the posting group adds keys, 0 cardinality means the posting doesn't exist.
		// If the posting group removes keys, no posting ranges found is fine as it is a noop.
		if len(pg.addKeys) > 0 && pg.cardinality == 0 {
			return nil, true, nil
		}
	}
	slices.SortFunc(postingGroups, func(a, b *postingGroup) int {
		if a.cardinality == b.cardinality {
			return strings.Compare(a.name, b.name)
		}
		return int(a.cardinality - b.cardinality)
	})

	/*
	   Algorithm of choosing what postings we need to fetch right now and what
	   postings we expand lazily.
	   Sort posting groups by cardinality, so we can iterate from posting group with the smallest posting size.
	   The algorithm focuses on fetching fewer data, including postings and series.

	   We need to fetch at least 1 posting group with add keys in order to fetch series. So if we only fetch the first posting group with add keys,
	   the data bytes we need to download is formula F1: P1 * 4 + P1 * S where P1 is the number of postings in group 1
	   and S is the size per series. 4 is the byte size per posting.

	   If we are going to fetch 2 posting groups, we can intersect the two postings to reduce series we need to download (hopefully).
	   Assuming for each intersection, the series matching ratio is R (0 < R < 1). Then the data bytes we need to download is
	   formula F2: P1 * 4 + P2 * 4 + P1 * S * R.
	   We can get formula F3 if we are going to fetch 3 posting groups:
	   F3: P1 * 4 + P2 * 4 + P3 * 4 + P1 * S * R^2.

	   Let's compare formula F2 and F1 first.
	   P1 * 4 + P2 * 4 + P1 * S * R < P1 * 4 + P1 * S
	   => P2 * 4 < P1 * S * (1 - R)
	   Left hand side is the posting group size and right hand side is basically the series size we don't need to fetch
	   by having the additional intersection. In order to fetch less data for F2 than F1, we just need to ensure that
	   the additional postings size is smaller.

	   Let's compare formula F3 and F2.
	   P1 * 4 + P2 * 4 + P3 * 4 + P1 * S * R^2 < P1 * 4 + P2 * 4 + P1 * S * R
	   => P3 * 4 < P1 * S * R * (1 - R)
	   Same as the previous formula.

	   Compare formula F4 (Cost to fetch up to 4 posting groups) and F3.
	   P4 * 4 < P1 * S * R^2 * (1 - R)

	   We can generalize this to formula: Pn * 4 < P1 * S * R^(n - 2) * (1 - R)

	   The idea of the algorithm:
	   By iterating the posting group in sorted order of cardinality, we need to make sure that by fetching the current posting group,
	   the total data fetched is smaller than the previous posting group. If so, then we continue to next posting group,
	   otherwise we stop.

	   This ensures that when we stop at one posting group, posting groups after it always need to fetch more data.
	   Based on formula Pn * 4 < P1 * S * R^(n - 2) * (1 - R), left hand side is always increasing while iterating to larger
	   posting groups while right hand side value is always decreasing as R < 1.
	*/
	var (
		negativeCardinalities int64
		i                     int
	)
	for i = range postingGroups {
		if postingGroups[i].addAll {
			negativeCardinalities += postingGroups[i].cardinality
			continue
		}
		break
	}
	// If the first posting group with add keys is already the last posting group
	// then there is no need to set up lazy expanded posting groups.
	if i >= len(postingGroups)-1 {
		return postingGroups, false, nil
	}

	// Assume only seriesMatchRatio postings will be matched every posting group.
	seriesMatched := postingGroups[i].cardinality - int64(math.Ceil(float64(negativeCardinalities)*seriesMatchRatio))
	i++ // Start from next posting group as we always need to fetch at least one posting group with add keys.
	for i < len(postingGroups) {
		pg := postingGroups[i]
		var (
			underfetchedSeries     int64
			underfetchedSeriesSize int64
		)
		// Unlikely but keep it as a safeguard. When estimated matching series
		// is <= 0, we stop early and mark rest of posting groups as lazy.
		if seriesMatched <= 0 {
			break
		}
		if pg.addAll {
			// For posting group that has negative matchers, we assume we can underfetch
			// min(pg.cardinality, current_series_matched) * match ratio series.
			if pg.cardinality > seriesMatched {
				underfetchedSeries = int64(math.Ceil(float64(seriesMatched) * seriesMatchRatio))
			} else {
				underfetchedSeries = int64(math.Ceil(float64(pg.cardinality) * seriesMatchRatio))
			}
			seriesMatched -= underfetchedSeries
			underfetchedSeriesSize = underfetchedSeries * seriesSize
		} else {
			underfetchedSeriesSize = seriesSize * int64(math.Ceil(float64(seriesMatched)*(1-seriesMatchRatio)))
			seriesMatched = int64(math.Ceil(float64(seriesMatched) * seriesMatchRatio))
		}

		// Need to fetch more data on postings than series we underfetch, stop here and lazy expanding rest of matchers.
		if pg.cardinality*4 > underfetchedSeriesSize {
			break
		}
		i++
	}
	for i < len(postingGroups) {
		postingGroups[i].lazy = true
		lazyExpandedPostingSizeBytes.Add(float64(4 * postingGroups[i].cardinality))
		i++
	}
	return postingGroups, false, nil
}

func fetchLazyExpandedPostings(
	ctx context.Context,
	postingGroups []*postingGroup,
	r *bucketIndexReader,
	bytesLimiter BytesLimiter,
	addAllPostings bool,
	lazyExpandedPostingEnabled bool,
	lazyExpandedPostingSizeBytes prometheus.Counter,
	tenant string,
) (*lazyExpandedPostings, error) {
	var (
		err               error
		emptyPostingGroup bool
	)
	/*
			There are several cases that we skip postings fetch optimization:
			- Lazy expanded posting disabled.
			- Add all postings. This means we don't have a posting group with any add keys.
		    - Block estimated series size not set which means we don't have a way to estimate series bytes downloaded.
			- `SeriesSize` not set for this block then we have no way to estimate series size.
			- Only one effective posting group available. We need to at least download postings from 1 posting group so no need to optimize.
	*/
	if lazyExpandedPostingEnabled && !addAllPostings &&
		r.block.estimatedSeriesSize > 0 && len(postingGroups) > 1 {
		postingGroups, emptyPostingGroup, err = optimizePostingsFetchByDownloadedBytes(
			r,
			postingGroups,
			int64(r.block.estimatedSeriesSize),
			0.5, // TODO(yeya24): Expose this as a flag.
			lazyExpandedPostingSizeBytes,
		)
		if err != nil {
			return nil, err
		}
		if emptyPostingGroup {
			return emptyLazyPostings, nil
		}
	}

	ps, matchers, err := fetchAndExpandPostingGroups(ctx, r, postingGroups, bytesLimiter, tenant)
	if err != nil {
		return nil, err
	}
	if len(ps) == 0 {
		return emptyLazyPostings, nil
	}
	return &lazyExpandedPostings{postings: ps, matchers: matchers}, nil
}

// keysToFetchFromPostingGroups returns label pairs (postings) to fetch
// and matchers we need to use for lazy posting expansion.
// Input `postingGroups` needs to be ordered by cardinality in case lazy
// expansion is enabled. When we find the first lazy posting group we can exit.
func keysToFetchFromPostingGroups(postingGroups []*postingGroup) ([]labels.Label, []*labels.Matcher) {
	var lazyMatchers []*labels.Matcher
	keys := make([]labels.Label, 0)
	i := 0
	for i < len(postingGroups) {
		pg := postingGroups[i]
		if pg.lazy {
			break
		}

		// Postings returned by fetchPostings will be in the same order as keys
		// so it's important that we iterate them in the same order later.
		// We don't have any other way of pairing keys and fetched postings.
		for _, key := range pg.addKeys {
			keys = append(keys, labels.Label{Name: pg.name, Value: key})
		}
		for _, key := range pg.removeKeys {
			keys = append(keys, labels.Label{Name: pg.name, Value: key})
		}
		i++
	}
	if i < len(postingGroups) {
		lazyMatchers = make([]*labels.Matcher, 0)
		for i < len(postingGroups) {
			lazyMatchers = append(lazyMatchers, postingGroups[i].matchers...)
			i++
		}
	}
	return keys, lazyMatchers
}

func fetchAndExpandPostingGroups(ctx context.Context, r *bucketIndexReader, postingGroups []*postingGroup, bytesLimiter BytesLimiter, tenant string) ([]storage.SeriesRef, []*labels.Matcher, error) {
	keys, lazyMatchers := keysToFetchFromPostingGroups(postingGroups)
	fetchedPostings, closeFns, err := r.fetchPostings(ctx, keys, bytesLimiter, tenant)
	defer func() {
		for _, closeFn := range closeFns {
			closeFn()
		}
	}()
	if err != nil {
		return nil, nil, errors.Wrap(err, "get postings")
	}

	// Get "add" and "remove" postings from groups. We iterate over postingGroups and their keys
	// again, and this is exactly the same order as before (when building the groups), so we can simply
	// use one incrementing index to fetch postings from returned slice.
	postingIndex := 0

	var groupAdds, groupRemovals []index.Postings
	for _, g := range postingGroups {
		if g.lazy {
			break
		}
		// We cannot add empty set to groupAdds, since they are intersected.
		if len(g.addKeys) > 0 {
			toMerge := make([]index.Postings, 0, len(g.addKeys))
			for _, l := range g.addKeys {
				toMerge = append(toMerge, checkNilPosting(g.name, l, fetchedPostings[postingIndex]))
				postingIndex++
			}

			groupAdds = append(groupAdds, index.Merge(ctx, toMerge...))
		}

		for _, l := range g.removeKeys {
			groupRemovals = append(groupRemovals, checkNilPosting(g.name, l, fetchedPostings[postingIndex]))
			postingIndex++
		}
	}

	result := index.Without(index.Intersect(groupAdds...), index.Merge(ctx, groupRemovals...))

	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	ps, err := ExpandPostingsWithContext(ctx, result)
	if err != nil {
		return nil, nil, errors.Wrap(err, "expand")
	}
	return ps, lazyMatchers, nil
}
