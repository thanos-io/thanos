// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dedup

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil/testiters"
)

type sample struct {
	t int64
	f float64
}

func (s sample) T() int64 {
	return s.t
}

func (s sample) F() float64 {
	return s.f
}

// TODO(rabenhorst): Needs to be implemented for native histogram support.
func (s sample) H() *histogram.Histogram {
	panic("not implemented")
}

func (s sample) FH() *histogram.FloatHistogram {
	panic("not implemented")
}

func (s sample) Type() chunkenc.ValueType {
	return chunkenc.ValFloat
}

func (s sample) Copy() chunks.Sample {
	c := sample{t: s.t, f: s.f}
	return c
}

type series struct {
	lset    labels.Labels
	samples []sample
}

func (s series) Labels() labels.Labels { return s.lset }
func (s series) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return newMockedSeriesIterator(s.samples)
}

// TODO(bwplotka): Reuse SeriesSets from chunk iterators from Prometheus.
type mockedSeriesSet struct {
	series []series
	cur    int
}

func (s *mockedSeriesSet) Next() bool {
	s.cur++
	return s.cur <= len(s.series)
}

func (s *mockedSeriesSet) At() storage.Series {
	return s.series[s.cur-1]
}
func (s *mockedSeriesSet) Err() error { return nil }

func (s *mockedSeriesSet) Warnings() annotations.Annotations { return nil }

type mockedSeriesIterator struct {
	cur     int
	samples []sample
}

func newMockedSeriesIterator(samples []sample) *mockedSeriesIterator {
	return &mockedSeriesIterator{samples: samples, cur: -1}
}

// TODO(rabenhorst): Native histogram support needs to be added, currently hardcoded to float.
func (s *mockedSeriesIterator) Seek(t int64) chunkenc.ValueType {
	s.cur = sort.Search(len(s.samples), func(n int) bool {
		return s.samples[n].t >= t
	})
	if s.cur < len(s.samples) {
		return chunkenc.ValFloat
	}
	return chunkenc.ValNone
}

func (s *mockedSeriesIterator) At() (t int64, v float64) {
	sample := s.samples[s.cur]
	return sample.t, sample.f
}

// TODO(rabenhorst): Needs to be implemented for native histogram support.
func (s *mockedSeriesIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	panic("not implemented")
}

func (s *mockedSeriesIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	panic("not implemented")
}

func (s *mockedSeriesIterator) AtT() int64 {
	return s.samples[s.cur].t
}

func (s *mockedSeriesIterator) Next() chunkenc.ValueType {
	s.cur++
	if s.cur < len(s.samples) {
		return chunkenc.ValFloat
	}

	return chunkenc.ValNone
}

func (s *mockedSeriesIterator) Err() error { return nil }

var expectedRealSeriesWithStaleMarkerDeduplicatedForRate = []sample{
	{t: 1587690005791, f: 461968}, {t: 1587690020791, f: 462151}, {t: 1587690035797, f: 462336}, {t: 1587690050791, f: 462650}, {t: 1587690065791, f: 462813}, {t: 1587690080791, f: 462987}, {t: 1587690095791, f: 463095}, {t: 1587690110791, f: 463247}, {t: 1587690125791, f: 463440}, {t: 1587690140791, f: 463642}, {t: 1587690155791, f: 463811}, {t: 1587690170791, f: 464027},
	{t: 1587690185791, f: 464308}, {t: 1587690200791, f: 464514}, {t: 1587690215791, f: 464798}, {t: 1587690230791, f: 465018}, {t: 1587690245791, f: 465215}, {t: 1587690260813, f: 465431}, {t: 1587690275791, f: 465651}, {t: 1587690290791, f: 465870}, {t: 1587690305791, f: 466070}, {t: 1587690320792, f: 466248}, {t: 1587690335791, f: 466506}, {t: 1587690350791, f: 466766},
	{t: 1587690365791, f: 466970}, {t: 1587690380791, f: 467123}, {t: 1587690395791, f: 467265}, {t: 1587690410791, f: 467383}, {t: 1587690425791, f: 467629}, {t: 1587690440791, f: 467931}, {t: 1587690455791, f: 468097}, {t: 1587690470791, f: 468281}, {t: 1587690485791, f: 468477}, {t: 1587690500791, f: 468649}, {t: 1587690515791, f: 468867}, {t: 1587690530791, f: 469150},
	{t: 1587690545791, f: 469268}, {t: 1587690560791, f: 469488}, {t: 1587690575791, f: 469742}, {t: 1587690590791, f: 469951}, {t: 1587690605791, f: 470131}, {t: 1587690620791, f: 470337}, {t: 1587690635791, f: 470631}, {t: 1587690650791, f: 470832}, {t: 1587690665791, f: 471077}, {t: 1587690680791, f: 471311}, {t: 1587690695791, f: 471473}, {t: 1587690710791, f: 471728},
	{t: 1587690725791, f: 472002}, {t: 1587690740791, f: 472158}, {t: 1587690755791, f: 472329}, {t: 1587690770791, f: 472722}, {t: 1587690785791, f: 472925}, {t: 1587690800791, f: 473220}, {t: 1587690815791, f: 473460}, {t: 1587690830791, f: 473748}, {t: 1587690845791, f: 473968}, {t: 1587690860791, f: 474261}, {t: 1587690875791, f: 474418}, {t: 1587690890791, f: 474726},
	{t: 1587690905791, f: 474913}, {t: 1587690920791, f: 475031}, {t: 1587690935791, f: 475284}, {t: 1587690950791, f: 475563}, {t: 1587690965791, f: 475762}, {t: 1587690980791, f: 475945}, {t: 1587690995791, f: 476302}, {t: 1587691010791, f: 476501}, {t: 1587691025791, f: 476849}, {t: 1587691040800, f: 477020}, {t: 1587691055791, f: 477280}, {t: 1587691070791, f: 477549},
	{t: 1587691085791, f: 477758}, {t: 1587691100817, f: 477960}, {t: 1587691115791, f: 478261}, {t: 1587691130791, f: 478559}, {t: 1587691145791, f: 478704}, {t: 1587691160804, f: 478950}, {t: 1587691175791, f: 479173}, {t: 1587691190791, f: 479368}, {t: 1587691205791, f: 479625}, {t: 1587691220805, f: 479866}, {t: 1587691235791, f: 480008}, {t: 1587691250791, f: 480155},
	{t: 1587691265791, f: 480472}, {t: 1587691280811, f: 480598}, {t: 1587691295791, f: 480771}, {t: 1587691310791, f: 480996}, {t: 1587691325791, f: 481200}, {t: 1587691340803, f: 481381}, {t: 1587691355791, f: 481584}, {t: 1587691370791, f: 481759}, {t: 1587691385791, f: 482003}, {t: 1587691400803, f: 482189}, {t: 1587691415791, f: 482457}, {t: 1587691430791, f: 482623},
	{t: 1587691445791, f: 482768}, {t: 1587691460804, f: 483036}, {t: 1587691475791, f: 483322}, {t: 1587691490791, f: 483566}, {t: 1587691505791, f: 483709}, {t: 1587691520807, f: 483838}, {t: 1587691535791, f: 484091}, {t: 1587691550791, f: 484236}, {t: 1587691565791, f: 484454}, {t: 1587691580816, f: 484710}, {t: 1587691595791, f: 484978}, {t: 1587691610791, f: 485271},
	{t: 1587691625791, f: 485476}, {t: 1587691640792, f: 485640}, {t: 1587691655791, f: 485921}, {t: 1587691670791, f: 486201}, {t: 1587691685791, f: 486555}, {t: 1587691700791, f: 486691}, {t: 1587691715791, f: 486831}, {t: 1587691730791, f: 487033}, {t: 1587691745791, f: 487268}, {t: 1587691760803, f: 487370}, {t: 1587691775791, f: 487571}, {t: 1587691790791, f: 487787},
	{t: 1587691805791, f: 488036}, {t: 1587691820791, f: 488241}, {t: 1587691835791, f: 488411}, {t: 1587691850791, f: 488625}, {t: 1587691865791, f: 488868}, {t: 1587691880791, f: 489005}, {t: 1587691895791, f: 489237}, {t: 1587691910791, f: 489545}, {t: 1587691925791, f: 489750}, {t: 1587691940791, f: 489899}, {t: 1587691955791, f: 490048}, {t: 1587691970791, f: 490364},
	{t: 1587691985791, f: 490485}, {t: 1587692000791, f: 490722}, {t: 1587692015791, f: 490866}, {t: 1587692030791, f: 491025}, {t: 1587692045791, f: 491286}, {t: 1587692060816, f: 491543}, {t: 1587692075791, f: 491787}, {t: 1587692090791, f: 492065}, {t: 1587692105791, f: 492223}, {t: 1587692120816, f: 492501}, {t: 1587692135791, f: 492767}, {t: 1587692150791, f: 492955},
	{t: 1587692165791, f: 493194}, {t: 1587692180792, f: 493402}, {t: 1587692195791, f: 493647}, {t: 1587692210791, f: 493897}, {t: 1587692225791, f: 494117}, {t: 1587692240805, f: 494356}, {t: 1587692255791, f: 494620}, {t: 1587692270791, f: 494762}, {t: 1587692285791, f: 495001}, {t: 1587692300805, f: 495222}, {t: 1587692315791, f: 495393}, {t: 1587692330791, f: 495662},
	{t: 1587692345791, f: 495875}, {t: 1587692360801, f: 496082}, {t: 1587692375791, f: 496196}, {t: 1587692390791, f: 496245}, {t: 1587692405791, f: 496295}, {t: 1587692420791, f: 496365}, {t: 1587692435791, f: 496401}, {t: 1587692450791, f: 496452}, {t: 1587692465791, f: 496491}, {t: 1587692480791, f: 496544}, {t: 1587692542149, f: 496544}, {t: 1587692557139, f: 496640},
	{t: 1587692572139, f: 496851}, {t: 1587692587139, f: 497047}, {t: 1587692602144, f: 497264}, {t: 1587692617139, f: 497529}, {t: 1587692632139, f: 497717}, {t: 1587692647139, f: 497945}, {t: 1587692662154, f: 498179}, {t: 1587692677139, f: 498466}, {t: 1587692692139, f: 498642}, {t: 1587692707139, f: 498839}, {t: 1587692722139, f: 499021}, {t: 1587692737139, f: 499177},
	{t: 1587692752139, f: 499345}, {t: 1587692767139, f: 499518}, {t: 1587692782149, f: 499726}, {t: 1587692797139, f: 499980}, {t: 1587692812139, f: 500196}, {t: 1587692827139, f: 500366}, {t: 1587692842139, f: 500524}, {t: 1587692857139, f: 500734}, {t: 1587692872139, f: 500966}, {t: 1587692887139, f: 501185}, {t: 1587692902139, f: 501253}, {t: 1587692917153, f: 501411},
	{t: 1587692932139, f: 501670}, {t: 1587692947139, f: 501857}, {t: 1587692962139, f: 502110}, {t: 1587692977155, f: 502287}, {t: 1587692992139, f: 502569}, {t: 1587693007139, f: 502749}, {t: 1587693022139, f: 502938}, {t: 1587693037139, f: 503197}, {t: 1587693052139, f: 503435}, {t: 1587693067139, f: 503637}, {t: 1587693082139, f: 503880}, {t: 1587693097139, f: 504034},
	{t: 1587693112139, f: 504186}, {t: 1587693127139, f: 504369}, {t: 1587693142139, f: 504597}, {t: 1587693157139, f: 504748}, {t: 1587693172139, f: 505063}, {t: 1587693187139, f: 505251}, {t: 1587693202139, f: 505443}, {t: 1587693217139, f: 505642}, {t: 1587693232139, f: 505943}, {t: 1587693247155, f: 506095}, {t: 1587693262139, f: 506316}, {t: 1587693277139, f: 506531},
	{t: 1587693292139, f: 506807}, {t: 1587693307139, f: 507017}, {t: 1587693322139, f: 507293}, {t: 1587693337139, f: 507537}, {t: 1587693352139, f: 507788}, {t: 1587693367139, f: 507998}, {t: 1587693382139, f: 508317}, {t: 1587693397139, f: 508577}, {t: 1587693412139, f: 508777}, {t: 1587693427139, f: 508989}, {t: 1587693442163, f: 509281}, {t: 1587693457139, f: 509484},
	{t: 1587693472139, f: 509720}, {t: 1587693487139, f: 509979}, {t: 1587693502139, f: 510189}, {t: 1587693517139, f: 510505}, {t: 1587693532139, f: 510661}, {t: 1587693547139, f: 510866}, {t: 1587693562139, f: 511131}, {t: 1587693577139, f: 511321}, {t: 1587693592139, f: 511495},
}

type chunkedSeries struct {
	lset   labels.Labels
	chunks []storepb.AggrChunk
}

type chunkedSeriesSet struct {
	series []chunkedSeries
	i      int
}

func newChunkedSeriesSet(s []chunkedSeries) *chunkedSeriesSet {
	return &chunkedSeriesSet{series: s, i: -1}
}

func (s *chunkedSeriesSet) Next() bool {
	if s.i >= len(s.series)-1 {
		return false
	}
	s.i++
	return true
}

func (*chunkedSeriesSet) Err() error {
	return nil
}

func (s *chunkedSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	return s.series[s.i].lset, s.series[s.i].chunks
}

func toChunkedSeriesSlice(t testing.TB, set storepb.SeriesSet) []chunkedSeries {
	var ret []chunkedSeries
	for set.Next() {
		lset, chunks := set.At()
		ret = append(ret, chunkedSeries{
			lset: lset, chunks: chunks,
		})
	}
	testutil.Ok(t, set.Err())
	return ret
}

func TestOverlapSplitSet(t *testing.T) {
	input := []chunkedSeries{
		{
			lset: labels.FromStrings("a", "1_empty"),
		},
		{
			lset:   labels.FromStrings("a", "2_nonoverlap"),
			chunks: []storepb.AggrChunk{{MinTime: 0, MaxTime: 20}, {MinTime: 21, MaxTime: 100}, {MinTime: 110, MaxTime: 300}},
		},
		{
			lset:   labels.FromStrings("a", "3_tworeplicas"),
			chunks: []storepb.AggrChunk{{MinTime: 0, MaxTime: 20}, {MinTime: 0, MaxTime: 30}, {MinTime: 21, MaxTime: 50}, {MinTime: 31, MaxTime: 60}, {MinTime: 100, MaxTime: 160}},
		},
		{
			lset:   labels.FromStrings("a", "4_nonoverlap"),
			chunks: []storepb.AggrChunk{{MinTime: 50, MaxTime: 55}, {MinTime: 56, MaxTime: 100}},
		},
		{
			lset:   labels.FromStrings("a", "5_minimaloverlap"),
			chunks: []storepb.AggrChunk{{MinTime: 50, MaxTime: 55}, {MinTime: 55, MaxTime: 100}},
		},
		{
			lset: labels.FromStrings("a", "6_fourreplica"),
			chunks: []storepb.AggrChunk{{MinTime: 0, MaxTime: 20}, {MinTime: 0, MaxTime: 30}, {MinTime: 1, MaxTime: 15}, {MinTime: 2, MaxTime: 36}, {MinTime: 16, MaxTime: 200},
				{MinTime: 21, MaxTime: 50}, {MinTime: 31, MaxTime: 60}, {MinTime: 100, MaxTime: 160}},
		},
	}
	exp := []chunkedSeries{
		{
			lset: labels.FromStrings("a", "1_empty"),
		},
		{
			lset:   labels.FromStrings("a", "2_nonoverlap"),
			chunks: []storepb.AggrChunk{{MinTime: 0, MaxTime: 20}, {MinTime: 21, MaxTime: 100}, {MinTime: 110, MaxTime: 300}},
		},
		{
			lset:   labels.FromStrings("a", "3_tworeplicas"),
			chunks: []storepb.AggrChunk{{MinTime: 0, MaxTime: 20}, {MinTime: 21, MaxTime: 50}, {MinTime: 100, MaxTime: 160}},
		},
		{
			lset:   labels.FromStrings("a", "3_tworeplicas"),
			chunks: []storepb.AggrChunk{{MinTime: 0, MaxTime: 30}, {MinTime: 31, MaxTime: 60}},
		},
		{
			lset:   labels.FromStrings("a", "4_nonoverlap"),
			chunks: []storepb.AggrChunk{{MinTime: 50, MaxTime: 55}, {MinTime: 56, MaxTime: 100}},
		},
		{
			lset:   labels.FromStrings("a", "5_minimaloverlap"),
			chunks: []storepb.AggrChunk{{MinTime: 50, MaxTime: 55}},
		},
		{
			lset:   labels.FromStrings("a", "5_minimaloverlap"),
			chunks: []storepb.AggrChunk{{MinTime: 55, MaxTime: 100}},
		},
		{
			lset:   labels.FromStrings("a", "6_fourreplica"),
			chunks: []storepb.AggrChunk{{MinTime: 0, MaxTime: 20}, {MinTime: 21, MaxTime: 50}, {MinTime: 100, MaxTime: 160}},
		},
		{
			lset:   labels.FromStrings("a", "6_fourreplica"),
			chunks: []storepb.AggrChunk{{MinTime: 0, MaxTime: 30}, {MinTime: 31, MaxTime: 60}},
		},
		{
			lset:   labels.FromStrings("a", "6_fourreplica"),
			chunks: []storepb.AggrChunk{{MinTime: 1, MaxTime: 15}, {MinTime: 16, MaxTime: 200}},
		},
		{
			lset:   labels.FromStrings("a", "6_fourreplica"),
			chunks: []storepb.AggrChunk{{MinTime: 2, MaxTime: 36}},
		},
	}

	got := toChunkedSeriesSlice(t, NewOverlapSplit(newChunkedSeriesSet(input)))
	testutil.Equals(t, exp, got)
}

func TestDedupSeriesSet(t *testing.T) {
	for _, tcase := range []struct {
		name      string
		input     []series
		exp       []series
		isCounter bool
	}{
		{
			name: "Single dedup label",
			input: []series{
				{
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{60000, 3}, {70000, 4}},
				}, {
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{200000, 5}, {210000, 6}},
				}, {
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.FromStrings("a", "1", "c", "3", "d", "4"),
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.FromStrings("a", "1", "c", "4"),
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.FromStrings("a", "2", "c", "3"),
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.FromStrings("a", "2", "c", "3"),
					samples: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []series{
				{
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}, {200000, 5}, {210000, 6}},
				},
				{
					lset:    labels.FromStrings("a", "1", "c", "3", "d", "4"),
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.FromStrings("a", "1", "c", "4"),
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.FromStrings("a", "2", "c", "3"),
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
		},
		{
			name: "Multi dedup label",
			input: []series{
				{
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{60000, 3}, {70000, 4}},
				}, {
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{200000, 5}, {210000, 6}},
				}, {
					lset:    labels.FromStrings("a", "1", "c", "3", "d", "4"),
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.FromStrings("a", "1", "c", "4"),
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.FromStrings("a", "2", "c", "3"),
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.FromStrings("a", "2", "c", "3"),
					samples: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []series{
				{
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}, {200000, 5}, {210000, 6}},
				},
				{
					lset:    labels.FromStrings("a", "1", "c", "3", "d", "4"),
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.FromStrings("a", "1", "c", "4"),
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.FromStrings("a", "2", "c", "3"),
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
		},
		{
			name: "Multi dedup label - some series don't have all dedup labels",
			input: []series{
				{
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []series{
				{
					lset:    labels.FromStrings("a", "1", "c", "3"),
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
		},
		{
			// Regression test against https://github.com/thanos-io/thanos/issues/2401.
			// Two counter series, when one (initially chosen) series is having hiccup (few dropped samples), while second is live.
			// This also happens when 2 replicas scrape in different time (they usually do) and one sees later counter value then the other.
			// Now, depending on what replica we look, we can see totally different counter value in total where total means
			// after accounting for counter resets. We account for that in downsample.CounterSeriesIterator, mainly because
			// we handle downsample Counter Aggregations specially (for detecting resets between chunks).
			name:      "Regression test against 2401",
			isCounter: true,
			input: []series{
				{
					lset: labels.FromStrings("a", "1"),
					samples: []sample{
						{10000, 8.0}, // Smaller timestamp, this will be chosen. CurrValue = 8.0.
						{20000, 9.0}, // Same. CurrValue = 9.0.
						// {Gap} app reset. No sample, because stale marker but removed by downsample.CounterSeriesIterator.
						{50001, 9 + 1.0}, // Next after 20000+1 has a bit higher than timestamp then in second series. Penalty 5000 will be added.
						{60000, 9 + 2.0},
						{70000, 9 + 3.0},
						{80000, 9 + 4.0},
						{90000, 9 + 5.0}, // This should be now taken, and we expect 14 to be correct value now.
						{100000, 9 + 6.0},
					},
				}, {
					lset: labels.FromStrings("a", "1"),
					samples: []sample{
						{10001, 8.0}, // Penalty 5000 will be added.
						// 20001 was app reset. No sample, because stale marker but removed by downsample.CounterSeriesIterator. Penalty 2 * (20000 - 10000) will be added.
						// 30001 no sample. Within penalty, ignored.
						{45001, 8 + 0.5}, // Smaller timestamp, this will be chosen. CurrValue = 8.5 which is smaller than last chosen value.
						{55001, 8 + 1.5},
						{65001, 8 + 2.5},
						// {Gap} app reset. No sample, because stale marker but removed by downsample.CounterSeriesIterator.
					},
				},
			},
			exp: []series{
				{
					lset:    labels.FromStrings("a", "1"),
					samples: []sample{{10000, 8}, {20000, 9}, {45001, 9}, {55001, 10}, {65001, 11}, {90000, 14}, {100000, 15}},
				},
			},
		},
		{
			// Same thing but not for counter should not adjust anything.
			name:      "Regression test with no counter adjustment",
			isCounter: false,
			input: []series{
				{
					lset: labels.FromStrings("a", "1"),
					samples: []sample{
						{10000, 8.0}, {20000, 9.0}, {50001, 9 + 1.0}, {60000, 9 + 2.0}, {70000, 9 + 3.0}, {80000, 9 + 4.0}, {90000, 9 + 5.0}, {100000, 9 + 6.0},
					},
				}, {
					lset: labels.FromStrings("a", "1"),
					samples: []sample{
						{10001, 8.0}, {45001, 8 + 0.5}, {55001, 8 + 1.5}, {65001, 8 + 2.5},
					},
				},
			},
			exp: []series{
				{
					lset:    labels.FromStrings("a", "1"),
					samples: []sample{{10000, 8}, {20000, 9}, {45001, 8.5}, {55001, 9.5}, {65001, 10.5}, {90000, 14}, {100000, 15}},
				},
			},
		},
		{
			// Regression test on real data against https://github.com/thanos-io/thanos/issues/2401.
			// Real data with stale marker after downsample.CounterSeriesIterator (required for downsampling + rate).
			name:      "Regression test on real data against 2401",
			isCounter: true,
			input: []series{
				{
					lset: labels.FromStrings("a", "1"),
					samples: []sample{
						{t: 1587690005791, f: 461968}, {t: 1587690020791, f: 462151}, {t: 1587690035797, f: 462336}, {t: 1587690050791, f: 462650}, {t: 1587690065791, f: 462813}, {t: 1587690080791, f: 462987}, {t: 1587690095791, f: 463095}, {t: 1587690110791, f: 463247}, {t: 1587690125791, f: 463440}, {t: 1587690140791, f: 463642}, {t: 1587690155791, f: 463811},
						{t: 1587690170791, f: 464027}, {t: 1587690185791, f: 464308}, {t: 1587690200791, f: 464514}, {t: 1587690215791, f: 464798}, {t: 1587690230791, f: 465018}, {t: 1587690245791, f: 465215}, {t: 1587690260813, f: 465431}, {t: 1587690275791, f: 465651}, {t: 1587690290791, f: 465870}, {t: 1587690305791, f: 466070}, {t: 1587690320792, f: 466248},
						{t: 1587690335791, f: 466506}, {t: 1587690350791, f: 466766}, {t: 1587690365791, f: 466970}, {t: 1587690380791, f: 467123}, {t: 1587690395791, f: 467265}, {t: 1587690410791, f: 467383}, {t: 1587690425791, f: 467629}, {t: 1587690440791, f: 467931}, {t: 1587690455791, f: 468097}, {t: 1587690470791, f: 468281}, {t: 1587690485791, f: 468477},
						{t: 1587690500791, f: 468649}, {t: 1587690515791, f: 468867}, {t: 1587690530791, f: 469150}, {t: 1587690545791, f: 469268}, {t: 1587690560791, f: 469488}, {t: 1587690575791, f: 469742}, {t: 1587690590791, f: 469951}, {t: 1587690605791, f: 470131}, {t: 1587690620791, f: 470337}, {t: 1587690635791, f: 470631}, {t: 1587690650791, f: 470832},
						{t: 1587690665791, f: 471077}, {t: 1587690680791, f: 471311}, {t: 1587690695791, f: 471473}, {t: 1587690710791, f: 471728}, {t: 1587690725791, f: 472002}, {t: 1587690740791, f: 472158}, {t: 1587690755791, f: 472329}, {t: 1587690770791, f: 472722}, {t: 1587690785791, f: 472925}, {t: 1587690800791, f: 473220}, {t: 1587690815791, f: 473460},
						{t: 1587690830791, f: 473748}, {t: 1587690845791, f: 473968}, {t: 1587690860791, f: 474261}, {t: 1587690875791, f: 474418}, {t: 1587690890791, f: 474726}, {t: 1587690905791, f: 474913}, {t: 1587690920791, f: 475031}, {t: 1587690935791, f: 475284}, {t: 1587690950791, f: 475563}, {t: 1587690965791, f: 475762}, {t: 1587690980791, f: 475945},
						{t: 1587690995791, f: 476302}, {t: 1587691010791, f: 476501}, {t: 1587691025791, f: 476849}, {t: 1587691040800, f: 477020}, {t: 1587691055791, f: 477280}, {t: 1587691070791, f: 477549}, {t: 1587691085791, f: 477758}, {t: 1587691100817, f: 477960}, {t: 1587691115791, f: 478261}, {t: 1587691130791, f: 478559}, {t: 1587691145791, f: 478704},
						{t: 1587691160804, f: 478950}, {t: 1587691175791, f: 479173}, {t: 1587691190791, f: 479368}, {t: 1587691205791, f: 479625}, {t: 1587691220805, f: 479866}, {t: 1587691235791, f: 480008}, {t: 1587691250791, f: 480155}, {t: 1587691265791, f: 480472}, {t: 1587691280811, f: 480598}, {t: 1587691295791, f: 480771}, {t: 1587691310791, f: 480996},
						{t: 1587691325791, f: 481200}, {t: 1587691340803, f: 481381}, {t: 1587691355791, f: 481584}, {t: 1587691370791, f: 481759}, {t: 1587691385791, f: 482003}, {t: 1587691400803, f: 482189}, {t: 1587691415791, f: 482457}, {t: 1587691430791, f: 482623}, {t: 1587691445791, f: 482768}, {t: 1587691460804, f: 483036}, {t: 1587691475791, f: 483322},
						{t: 1587691490791, f: 483566}, {t: 1587691505791, f: 483709}, {t: 1587691520807, f: 483838}, {t: 1587691535791, f: 484091}, {t: 1587691550791, f: 484236}, {t: 1587691565791, f: 484454}, {t: 1587691580816, f: 484710}, {t: 1587691595791, f: 484978}, {t: 1587691610791, f: 485271}, {t: 1587691625791, f: 485476}, {t: 1587691640792, f: 485640},
						{t: 1587691655791, f: 485921}, {t: 1587691670791, f: 486201}, {t: 1587691685791, f: 486555}, {t: 1587691700791, f: 486691}, {t: 1587691715791, f: 486831}, {t: 1587691730791, f: 487033}, {t: 1587691745791, f: 487268}, {t: 1587691760803, f: 487370}, {t: 1587691775791, f: 487571}, {t: 1587691790791, f: 487787}, {t: 1587691805791, f: 488036},
						{t: 1587691820791, f: 488241}, {t: 1587691835791, f: 488411}, {t: 1587691850791, f: 488625}, {t: 1587691865791, f: 488868}, {t: 1587691880791, f: 489005}, {t: 1587691895791, f: 489237}, {t: 1587691910791, f: 489545}, {t: 1587691925791, f: 489750}, {t: 1587691940791, f: 489899}, {t: 1587691955791, f: 490048}, {t: 1587691970791, f: 490364},
						{t: 1587691985791, f: 490485}, {t: 1587692000791, f: 490722}, {t: 1587692015791, f: 490866}, {t: 1587692030791, f: 491025}, {t: 1587692045791, f: 491286}, {t: 1587692060816, f: 491543}, {t: 1587692075791, f: 491787}, {t: 1587692090791, f: 492065}, {t: 1587692105791, f: 492223}, {t: 1587692120816, f: 492501}, {t: 1587692135791, f: 492767},
						{t: 1587692150791, f: 492955}, {t: 1587692165791, f: 493194}, {t: 1587692180792, f: 493402}, {t: 1587692195791, f: 493647}, {t: 1587692210791, f: 493897}, {t: 1587692225791, f: 494117}, {t: 1587692240805, f: 494356}, {t: 1587692255791, f: 494620}, {t: 1587692270791, f: 494762}, {t: 1587692285791, f: 495001}, {t: 1587692300805, f: 495222},
						{t: 1587692315791, f: 495393}, {t: 1587692330791, f: 495662}, {t: 1587692345791, f: 495875}, {t: 1587692360801, f: 496082}, {t: 1587692375791, f: 496196}, {t: 1587692390791, f: 496245}, {t: 1587692405791, f: 496295}, {t: 1587692420791, f: 496365}, {t: 1587692435791, f: 496401}, {t: 1587692450791, f: 496452}, {t: 1587692465791, f: 496491},
						{t: 1587692480791, f: 496544}, {t: 1587692555791, f: 496619}, {t: 1587692570791, f: 496852}, {t: 1587692585791, f: 497052}, {t: 1587692600791, f: 497245}, {t: 1587692615791, f: 497529}, {t: 1587692630791, f: 497697}, {t: 1587692645791, f: 497909}, {t: 1587692660791, f: 498156}, {t: 1587692675803, f: 498466}, {t: 1587692690791, f: 498647},
						{t: 1587692705791, f: 498805}, {t: 1587692720791, f: 499013}, {t: 1587692735805, f: 499169}, {t: 1587692750791, f: 499345}, {t: 1587692765791, f: 499499}, {t: 1587692780791, f: 499731}, {t: 1587692795806, f: 499972}, {t: 1587692810791, f: 500201}, {t: 1587692825791, f: 500354}, {t: 1587692840791, f: 500512}, {t: 1587692855791, f: 500739},
						{t: 1587692870791, f: 500958}, {t: 1587692885791, f: 501190}, {t: 1587692900791, f: 501233}, {t: 1587692915791, f: 501391}, {t: 1587692930791, f: 501649}, {t: 1587692945791, f: 501853}, {t: 1587692960791, f: 502065}, {t: 1587692975791, f: 502239}, {t: 1587692990810, f: 502554}, {t: 1587693005791, f: 502754}, {t: 1587693020791, f: 502938},
						{t: 1587693035791, f: 503141}, {t: 1587693050791, f: 503416}, {t: 1587693065791, f: 503642}, {t: 1587693080791, f: 503873}, {t: 1587693095791, f: 504014}, {t: 1587693110791, f: 504178}, {t: 1587693125821, f: 504374}, {t: 1587693140791, f: 504578}, {t: 1587693155791, f: 504753}, {t: 1587693170791, f: 505043}, {t: 1587693185791, f: 505232},
						{t: 1587693200791, f: 505437}, {t: 1587693215791, f: 505596}, {t: 1587693230791, f: 505923}, {t: 1587693245791, f: 506088}, {t: 1587693260791, f: 506307}, {t: 1587693275791, f: 506518}, {t: 1587693290791, f: 506786}, {t: 1587693305791, f: 507008}, {t: 1587693320803, f: 507260}, {t: 1587693335791, f: 507519}, {t: 1587693350791, f: 507776},
						{t: 1587693365791, f: 508003}, {t: 1587693380791, f: 508322}, {t: 1587693395804, f: 508551}, {t: 1587693410791, f: 508750}, {t: 1587693425791, f: 508994}, {t: 1587693440791, f: 509237}, {t: 1587693455791, f: 509452}, {t: 1587693470791, f: 509702}, {t: 1587693485791, f: 509971}, {t: 1587693500791, f: 510147}, {t: 1587693515791, f: 510471},
						{t: 1587693530816, f: 510666}, {t: 1587693545791, f: 510871}, {t: 1587693560791, f: 511123}, {t: 1587693575791, f: 511303}, {t: 1587693590791, f: 511500},
					},
				}, {
					lset: labels.FromStrings("a", "1"),
					samples: []sample{
						{t: 1587690007139, f: 461993}, {t: 1587690022139, f: 462164}, {t: 1587690037139, f: 462409}, {t: 1587690052139, f: 462662}, {t: 1587690067139, f: 462824}, {t: 1587690082139, f: 462987}, {t: 1587690097155, f: 463108}, {t: 1587690112139, f: 463261}, {t: 1587690127139, f: 463465}, {t: 1587690142139, f: 463642},
						{t: 1587690157139, f: 463823}, {t: 1587690172139, f: 464065}, {t: 1587690187139, f: 464333}, {t: 1587690202139, f: 464566}, {t: 1587690217139, f: 464811}, {t: 1587690232140, f: 465032}, {t: 1587690247139, f: 465229}, {t: 1587690262139, f: 465445}, {t: 1587690277139, f: 465700}, {t: 1587690292139, f: 465884},
						{t: 1587690307139, f: 466083}, {t: 1587690322139, f: 466250}, {t: 1587690337150, f: 466534}, {t: 1587690352139, f: 466791}, {t: 1587690367139, f: 466970}, {t: 1587690382139, f: 467149}, {t: 1587690397139, f: 467265}, {t: 1587690412139, f: 467383}, {t: 1587690427139, f: 467647}, {t: 1587690442139, f: 467943},
						{t: 1587690457139, f: 468121}, {t: 1587690472139, f: 468294}, {t: 1587690487139, f: 468545}, {t: 1587690502139, f: 468676}, {t: 1587690517139, f: 468879}, {t: 1587690532139, f: 469154}, {t: 1587690547139, f: 469281}, {t: 1587690562139, f: 469512}, {t: 1587690577139, f: 469783}, {t: 1587690592139, f: 469964},
						{t: 1587690607139, f: 470171}, {t: 1587690622139, f: 470355}, {t: 1587690637139, f: 470656}, {t: 1587690652139, f: 470845}, {t: 1587690667139, f: 471077}, {t: 1587690682139, f: 471315}, {t: 1587690697139, f: 471535}, {t: 1587690712139, f: 471766}, {t: 1587690727139, f: 472002}, {t: 1587690742139, f: 472171},
						{t: 1587690757139, f: 472354}, {t: 1587690772139, f: 472736}, {t: 1587690787139, f: 472948}, {t: 1587690802139, f: 473259}, {t: 1587690817139, f: 473460}, {t: 1587690832139, f: 473753}, {t: 1587690847139, f: 474007}, {t: 1587690862139, f: 474286}, {t: 1587690877139, f: 474423}, {t: 1587690892139, f: 474788},
						{t: 1587690907139, f: 474925}, {t: 1587690922139, f: 475031}, {t: 1587690937139, f: 475316}, {t: 1587690952139, f: 475573}, {t: 1587690967139, f: 475784}, {t: 1587690982139, f: 475992}, {t: 1587690997139, f: 476341}, {t: 1587691012139, f: 476541}, {t: 1587691027139, f: 476890}, {t: 1587691042139, f: 477033},
						{t: 1587691057139, f: 477305}, {t: 1587691072139, f: 477577}, {t: 1587691087139, f: 477771}, {t: 1587691102139, f: 478012}, {t: 1587691117139, f: 478296}, {t: 1587691132139, f: 478559}, {t: 1587691147139, f: 478744}, {t: 1587691162139, f: 478950}, {t: 1587691177139, f: 479201}, {t: 1587691192139, f: 479388},
						{t: 1587691207139, f: 479638}, {t: 1587691222154, f: 479907}, {t: 1587691237139, f: 480008}, {t: 1587691252139, f: 480167}, {t: 1587691267139, f: 480472}, {t: 1587691282157, f: 480615}, {t: 1587691297139, f: 480771}, {t: 1587691312139, f: 481027}, {t: 1587691327139, f: 481212}, {t: 1587691342159, f: 481395},
						{t: 1587691357139, f: 481598}, {t: 1587691372139, f: 481786}, {t: 1587691387139, f: 482003}, {t: 1587691402141, f: 482236}, {t: 1587691417139, f: 482508}, {t: 1587691432139, f: 482636}, {t: 1587691447139, f: 482780}, {t: 1587691462139, f: 483059}, {t: 1587691477139, f: 483357}, {t: 1587691492139, f: 483566},
						{t: 1587691507139, f: 483711}, {t: 1587691522139, f: 483838}, {t: 1587691537139, f: 484091}, {t: 1587691552139, f: 484254}, {t: 1587691567139, f: 484479}, {t: 1587691582139, f: 484748}, {t: 1587691597139, f: 484978}, {t: 1587691612139, f: 485271}, {t: 1587691627139, f: 485488}, {t: 1587691642139, f: 485700},
						{t: 1587691657139, f: 485945}, {t: 1587691672139, f: 486228}, {t: 1587691687139, f: 486588}, {t: 1587691702139, f: 486691}, {t: 1587691717139, f: 486881}, {t: 1587691732139, f: 487046}, {t: 1587691747139, f: 487291}, {t: 1587691762177, f: 487410}, {t: 1587691777139, f: 487571}, {t: 1587691792139, f: 487799},
						{t: 1587691807139, f: 488050}, {t: 1587691822139, f: 488241}, {t: 1587691837139, f: 488424}, {t: 1587691852139, f: 488629}, {t: 1587691867139, f: 488875}, {t: 1587691882139, f: 489017}, {t: 1587691897139, f: 489254}, {t: 1587691912139, f: 489545}, {t: 1587691927139, f: 489778}, {t: 1587691942139, f: 489912},
						{t: 1587691957139, f: 490084}, {t: 1587691972139, f: 490364}, {t: 1587691987139, f: 490510}, {t: 1587692002139, f: 490744}, {t: 1587692017139, f: 490880}, {t: 1587692032139, f: 491025}, {t: 1587692047139, f: 491297}, {t: 1587692062155, f: 491557}, {t: 1587692077139, f: 491839}, {t: 1587692092139, f: 492065},
						{t: 1587692107139, f: 492234}, {t: 1587692122139, f: 492526}, {t: 1587692137139, f: 492767}, {t: 1587692152139, f: 492967}, {t: 1587692167139, f: 493218}, {t: 1587692182139, f: 493442}, {t: 1587692197139, f: 493647}, {t: 1587692212139, f: 493920}, {t: 1587692227139, f: 494170}, {t: 1587692242139, f: 494358},
						{t: 1587692257139, f: 494632}, {t: 1587692272139, f: 494800}, {t: 1587692287139, f: 495026}, {t: 1587692302139, f: 495222}, {t: 1587692317139, f: 495433}, {t: 1587692332139, f: 495677}, {t: 1587692347139, f: 495901}, {t: 1587692362139, f: 496107}, {t: 1587692377139, f: 496196}, {t: 1587692392139, f: 496245},
						{t: 1587692407139, f: 496300}, {t: 1587692422159, f: 496365}, {t: 1587692437139, f: 496401}, {t: 1587692452139, f: 496452}, {t: 1587692467139, f: 496532}, {t: 1587692542149, f: 496537}, {t: 1587692557139, f: 496633}, {t: 1587692572139, f: 496844}, {t: 1587692587139, f: 497040}, {t: 1587692602144, f: 497257},
						{t: 1587692617139, f: 497522}, {t: 1587692632139, f: 497710}, {t: 1587692647139, f: 497938}, {t: 1587692662154, f: 498172}, {t: 1587692677139, f: 498459}, {t: 1587692692139, f: 498635}, {t: 1587692707139, f: 498832}, {t: 1587692722139, f: 499014}, {t: 1587692737139, f: 499170}, {t: 1587692752139, f: 499338},
						{t: 1587692767139, f: 499511}, {t: 1587692782149, f: 499719}, {t: 1587692797139, f: 499973}, {t: 1587692812139, f: 500189}, {t: 1587692827139, f: 500359}, {t: 1587692842139, f: 500517}, {t: 1587692857139, f: 500727}, {t: 1587692872139, f: 500959}, {t: 1587692887139, f: 501178}, {t: 1587692902139, f: 501246},
						{t: 1587692917153, f: 501404}, {t: 1587692932139, f: 501663}, {t: 1587692947139, f: 501850}, {t: 1587692962139, f: 502103}, {t: 1587692977155, f: 502280}, {t: 1587692992139, f: 502562}, {t: 1587693007139, f: 502742}, {t: 1587693022139, f: 502931}, {t: 1587693037139, f: 503190}, {t: 1587693052139, f: 503428},
						{t: 1587693067139, f: 503630}, {t: 1587693082139, f: 503873}, {t: 1587693097139, f: 504027}, {t: 1587693112139, f: 504179}, {t: 1587693127139, f: 504362}, {t: 1587693142139, f: 504590}, {t: 1587693157139, f: 504741}, {t: 1587693172139, f: 505056}, {t: 1587693187139, f: 505244}, {t: 1587693202139, f: 505436},
						{t: 1587693217139, f: 505635}, {t: 1587693232139, f: 505936}, {t: 1587693247155, f: 506088}, {t: 1587693262139, f: 506309}, {t: 1587693277139, f: 506524}, {t: 1587693292139, f: 506800}, {t: 1587693307139, f: 507010}, {t: 1587693322139, f: 507286}, {t: 1587693337139, f: 507530}, {t: 1587693352139, f: 507781},
						{t: 1587693367139, f: 507991}, {t: 1587693382139, f: 508310}, {t: 1587693397139, f: 508570}, {t: 1587693412139, f: 508770}, {t: 1587693427139, f: 508982}, {t: 1587693442163, f: 509274}, {t: 1587693457139, f: 509477}, {t: 1587693472139, f: 509713}, {t: 1587693487139, f: 509972}, {t: 1587693502139, f: 510182},
						{t: 1587693517139, f: 510498}, {t: 1587693532139, f: 510654}, {t: 1587693547139, f: 510859}, {t: 1587693562139, f: 511124}, {t: 1587693577139, f: 511314}, {t: 1587693592139, f: 511488},
					},
				},
			},
			exp: []series{
				{
					lset:    labels.FromStrings("a", "1"),
					samples: expectedRealSeriesWithStaleMarkerDeduplicatedForRate,
				},
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			// If it is a counter then pass a function which expects a counter.
			f := ""
			if tcase.isCounter {
				f = "rate"
			}
			dedupSet := NewSeriesSet(&mockedSeriesSet{series: tcase.input}, f, AlgorithmPenalty)
			var ats []storage.Series
			for dedupSet.Next() {
				ats = append(ats, dedupSet.At())
			}
			testutil.Ok(t, dedupSet.Err())
			testutil.Equals(t, len(tcase.exp), len(ats))

			for i, s := range ats {
				testutil.Equals(t, tcase.exp[i].lset, s.Labels(), "labels mismatch for series %v", i)
				res := expandSeries(t, s.Iterator(nil))
				testutil.Equals(t, tcase.exp[i].samples, res, "values mismatch for series :%v", i)
			}
		})
	}
}

func TestDedupSeriesIterator(t *testing.T) {
	// The deltas between timestamps should be at least 10000 to not be affected
	// by the initial penalty of 5000, that will cause the second iterator to seek
	// ahead this far at least once.
	cases := []struct {
		a, b, exp []sample
	}{
		{ // Generally prefer the first series.
			a:   []sample{{10000, 10}, {20000, 11}, {30000, 12}, {40000, 13}},
			b:   []sample{{10000, 20}, {20000, 21}, {30000, 22}, {40000, 23}},
			exp: []sample{{10000, 10}, {20000, 11}, {30000, 12}, {40000, 13}},
		},
		{ // Prefer b if it starts earlier.
			a:   []sample{{10100, 1}, {20100, 1}, {30100, 1}, {40100, 1}},
			b:   []sample{{10000, 2}, {20000, 2}, {30000, 2}, {40000, 2}},
			exp: []sample{{10000, 2}, {20000, 2}, {30000, 2}, {40000, 2}},
		},
		{ // Don't switch series on a single delta sized gap.
			a:   []sample{{10000, 1}, {20000, 1}, {40000, 1}},
			b:   []sample{{10000, 2}, {20000, 2}, {30000, 2}, {40000, 2}},
			exp: []sample{{10000, 1}, {20000, 1}, {40000, 1}},
		},
		{
			a:   []sample{{10000, 1}, {20000, 1}, {40000, 1}},
			b:   []sample{{15000, 2}, {25000, 2}, {35000, 2}, {45000, 2}},
			exp: []sample{{10000, 1}, {20000, 1}, {40000, 1}},
		},
		{ // Once the gap gets bigger than 2 deltas, switch and stay with the new series.
			a:   []sample{{10000, 1}, {20000, 1}, {30000, 1}, {60000, 1}, {70000, 1}},
			b:   []sample{{10100, 2}, {20100, 2}, {30100, 2}, {40100, 2}, {50100, 2}, {60100, 2}},
			exp: []sample{{10000, 1}, {20000, 1}, {30000, 1}, {50100, 2}, {60100, 2}},
		},
	}
	for i, c := range cases {
		t.Logf("case %d:", i)
		it := newDedupSeriesIterator(
			noopAdjustableSeriesIterator{newMockedSeriesIterator(c.a)},
			noopAdjustableSeriesIterator{newMockedSeriesIterator(c.b)},
		)
		res := expandSeries(t, noopAdjustableSeriesIterator{it})
		testutil.Equals(t, c.exp, res)
	}
}

func TestDedupSeriesIterator_NativeHistograms(t *testing.T) {
	hs := tsdbutil.GenerateTestHistograms(1)

	casesMixed := []struct {
		a   []sample
		b   []*testiters.HistogramPair
		exp []any
	}{
		{
			a:   []sample{{t: 0, f: 0}},
			b:   []*testiters.HistogramPair{{T: 10000, H: hs[0]}, {T: 20000, H: hs[0]}, {T: 30000, H: hs[0]}, {T: 40000, H: hs[0]}},
			exp: []any{&sample{t: 0, f: 0}, &testiters.HistogramPair{T: 10000, H: hs[0]}, &testiters.HistogramPair{T: 20000, H: hs[0]}, &testiters.HistogramPair{T: 30000, H: hs[0]}, &testiters.HistogramPair{T: 40000, H: hs[0]}},
		},
	}

	for i, c := range casesMixed {
		t.Run(fmt.Sprintf("mixed-%d", i), func(t *testing.T) {
			t.Parallel()
			it := newDedupSeriesIterator(
				noopAdjustableSeriesIterator{testiters.NewHistogramIterator(c.b)},
				noopAdjustableSeriesIterator{newMockedSeriesIterator(c.a)},
			)
			res := expandHistogramSeries(t, noopAdjustableSeriesIterator{it})
			require.EqualValues(t, c.exp, res)

			it = newDedupSeriesIterator(
				noopAdjustableSeriesIterator{newMockedSeriesIterator(c.a)},
				noopAdjustableSeriesIterator{testiters.NewHistogramIterator(c.b)},
			)
			res = expandHistogramSeries(t, noopAdjustableSeriesIterator{it})
			require.EqualValues(t, c.exp, res)
		})
	}
}

func BenchmarkDedupSeriesIterator(b *testing.B) {
	run := func(b *testing.B, s1, s2 []sample) {
		it := newDedupSeriesIterator(
			noopAdjustableSeriesIterator{newMockedSeriesIterator(s1)},
			noopAdjustableSeriesIterator{newMockedSeriesIterator(s2)},
		)
		b.ResetTimer()
		var total int64

		for it.Next() != chunkenc.ValNone {
			t, _ := it.At()
			total += t
		}
		fmt.Fprint(io.Discard, total)
	}
	b.Run("equal", func(b *testing.B) {
		var s1, s2 []sample

		for i := 0; b.Loop(); i++ {
			s1 = append(s1, sample{t: int64(i * 10000), f: 1})
		}
		for i := 0; b.Loop(); i++ {
			s2 = append(s2, sample{t: int64(i * 10000), f: 2})
		}
		run(b, s1, s2)
	})
	b.Run("fixed-delta", func(b *testing.B) {
		var s1, s2 []sample

		for i := 0; b.Loop(); i++ {
			s1 = append(s1, sample{t: int64(i * 10000), f: 1})
		}
		for i := 0; b.Loop(); i++ {
			s2 = append(s2, sample{t: int64(i*10000) + 10, f: 2})
		}
		run(b, s1, s2)
	})
	b.Run("minor-rand-delta", func(b *testing.B) {
		var s1, s2 []sample

		for i := 0; b.Loop(); i++ {
			s1 = append(s1, sample{t: int64(i*10000) + rand.Int63n(5000), f: 1})
		}
		for i := 0; b.Loop(); i++ {
			s2 = append(s2, sample{t: int64(i*10000) + +rand.Int63n(5000), f: 2})
		}
		run(b, s1, s2)
	})
}

const hackyStaleMarker = float64(-99999999)

func expandSeries(t testing.TB, it chunkenc.Iterator) (res []sample) {
	for it.Next() != chunkenc.ValNone {
		t, v := it.At()
		// Nan != Nan, so substitute for another value.
		// This is required for testutil.Equals to work deterministically.
		if math.IsNaN(v) {
			v = hackyStaleMarker
		}
		res = append(res, sample{t, v})
	}
	testutil.Ok(t, it.Err())
	return res
}

func expandHistogramSeries(t testing.TB, it chunkenc.Iterator) (res []any) {
	for {
		nextVal := it.Next()
		if nextVal == chunkenc.ValNone {
			break
		}

		if nextVal == chunkenc.ValHistogram {
			t, h := it.AtHistogram(nil)
			res = append(res, &testiters.HistogramPair{T: t, H: h})
		} else {
			t, f := it.At()
			res = append(res, &sample{t: t, f: f})
		}
	}
	testutil.Ok(t, it.Err())
	return res
}
