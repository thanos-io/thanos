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

	"github.com/efficientgo/core/testutil"
)

type sample struct {
	t int64
	v float64
}

func (s sample) T() int64 {
	return s.t
}

func (s sample) V() float64 {
	return s.v
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

type series struct {
	lset    labels.Labels
	samples []sample
}

func (s series) Labels() labels.Labels { return s.lset }
func (s series) Iterator() chunkenc.Iterator {
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

func (s *mockedSeriesSet) Warnings() storage.Warnings { return nil }

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
	return sample.t, sample.v
}

// TODO(rabenhorst): Needs to be implemented for native histogram support.
func (s *mockedSeriesIterator) AtHistogram() (int64, *histogram.Histogram) {
	panic("not implemented")
}

func (s *mockedSeriesIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
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
	{t: 1587690005791, v: 461968}, {t: 1587690020791, v: 462151}, {t: 1587690035797, v: 462336}, {t: 1587690050791, v: 462650}, {t: 1587690065791, v: 462813}, {t: 1587690080791, v: 462987}, {t: 1587690095791, v: 463095}, {t: 1587690110791, v: 463247}, {t: 1587690125791, v: 463440}, {t: 1587690140791, v: 463642}, {t: 1587690155791, v: 463811}, {t: 1587690170791, v: 464027},
	{t: 1587690185791, v: 464308}, {t: 1587690200791, v: 464514}, {t: 1587690215791, v: 464798}, {t: 1587690230791, v: 465018}, {t: 1587690245791, v: 465215}, {t: 1587690260813, v: 465431}, {t: 1587690275791, v: 465651}, {t: 1587690290791, v: 465870}, {t: 1587690305791, v: 466070}, {t: 1587690320792, v: 466248}, {t: 1587690335791, v: 466506}, {t: 1587690350791, v: 466766},
	{t: 1587690365791, v: 466970}, {t: 1587690380791, v: 467123}, {t: 1587690395791, v: 467265}, {t: 1587690410791, v: 467383}, {t: 1587690425791, v: 467629}, {t: 1587690440791, v: 467931}, {t: 1587690455791, v: 468097}, {t: 1587690470791, v: 468281}, {t: 1587690485791, v: 468477}, {t: 1587690500791, v: 468649}, {t: 1587690515791, v: 468867}, {t: 1587690530791, v: 469150},
	{t: 1587690545791, v: 469268}, {t: 1587690560791, v: 469488}, {t: 1587690575791, v: 469742}, {t: 1587690590791, v: 469951}, {t: 1587690605791, v: 470131}, {t: 1587690620791, v: 470337}, {t: 1587690635791, v: 470631}, {t: 1587690650791, v: 470832}, {t: 1587690665791, v: 471077}, {t: 1587690680791, v: 471311}, {t: 1587690695791, v: 471473}, {t: 1587690710791, v: 471728},
	{t: 1587690725791, v: 472002}, {t: 1587690740791, v: 472158}, {t: 1587690755791, v: 472329}, {t: 1587690770791, v: 472722}, {t: 1587690785791, v: 472925}, {t: 1587690800791, v: 473220}, {t: 1587690815791, v: 473460}, {t: 1587690830791, v: 473748}, {t: 1587690845791, v: 473968}, {t: 1587690860791, v: 474261}, {t: 1587690875791, v: 474418}, {t: 1587690890791, v: 474726},
	{t: 1587690905791, v: 474913}, {t: 1587690920791, v: 475031}, {t: 1587690935791, v: 475284}, {t: 1587690950791, v: 475563}, {t: 1587690965791, v: 475762}, {t: 1587690980791, v: 475945}, {t: 1587690995791, v: 476302}, {t: 1587691010791, v: 476501}, {t: 1587691025791, v: 476849}, {t: 1587691040800, v: 477020}, {t: 1587691055791, v: 477280}, {t: 1587691070791, v: 477549},
	{t: 1587691085791, v: 477758}, {t: 1587691100817, v: 477960}, {t: 1587691115791, v: 478261}, {t: 1587691130791, v: 478559}, {t: 1587691145791, v: 478704}, {t: 1587691160804, v: 478950}, {t: 1587691175791, v: 479173}, {t: 1587691190791, v: 479368}, {t: 1587691205791, v: 479625}, {t: 1587691220805, v: 479866}, {t: 1587691235791, v: 480008}, {t: 1587691250791, v: 480155},
	{t: 1587691265791, v: 480472}, {t: 1587691280811, v: 480598}, {t: 1587691295791, v: 480771}, {t: 1587691310791, v: 480996}, {t: 1587691325791, v: 481200}, {t: 1587691340803, v: 481381}, {t: 1587691355791, v: 481584}, {t: 1587691370791, v: 481759}, {t: 1587691385791, v: 482003}, {t: 1587691400803, v: 482189}, {t: 1587691415791, v: 482457}, {t: 1587691430791, v: 482623},
	{t: 1587691445791, v: 482768}, {t: 1587691460804, v: 483036}, {t: 1587691475791, v: 483322}, {t: 1587691490791, v: 483566}, {t: 1587691505791, v: 483709}, {t: 1587691520807, v: 483838}, {t: 1587691535791, v: 484091}, {t: 1587691550791, v: 484236}, {t: 1587691565791, v: 484454}, {t: 1587691580816, v: 484710}, {t: 1587691595791, v: 484978}, {t: 1587691610791, v: 485271},
	{t: 1587691625791, v: 485476}, {t: 1587691640792, v: 485640}, {t: 1587691655791, v: 485921}, {t: 1587691670791, v: 486201}, {t: 1587691685791, v: 486555}, {t: 1587691700791, v: 486691}, {t: 1587691715791, v: 486831}, {t: 1587691730791, v: 487033}, {t: 1587691745791, v: 487268}, {t: 1587691760803, v: 487370}, {t: 1587691775791, v: 487571}, {t: 1587691790791, v: 487787},
	{t: 1587691805791, v: 488036}, {t: 1587691820791, v: 488241}, {t: 1587691835791, v: 488411}, {t: 1587691850791, v: 488625}, {t: 1587691865791, v: 488868}, {t: 1587691880791, v: 489005}, {t: 1587691895791, v: 489237}, {t: 1587691910791, v: 489545}, {t: 1587691925791, v: 489750}, {t: 1587691940791, v: 489899}, {t: 1587691955791, v: 490048}, {t: 1587691970791, v: 490364},
	{t: 1587691985791, v: 490485}, {t: 1587692000791, v: 490722}, {t: 1587692015791, v: 490866}, {t: 1587692030791, v: 491025}, {t: 1587692045791, v: 491286}, {t: 1587692060816, v: 491543}, {t: 1587692075791, v: 491787}, {t: 1587692090791, v: 492065}, {t: 1587692105791, v: 492223}, {t: 1587692120816, v: 492501}, {t: 1587692135791, v: 492767}, {t: 1587692150791, v: 492955},
	{t: 1587692165791, v: 493194}, {t: 1587692180792, v: 493402}, {t: 1587692195791, v: 493647}, {t: 1587692210791, v: 493897}, {t: 1587692225791, v: 494117}, {t: 1587692240805, v: 494356}, {t: 1587692255791, v: 494620}, {t: 1587692270791, v: 494762}, {t: 1587692285791, v: 495001}, {t: 1587692300805, v: 495222}, {t: 1587692315791, v: 495393}, {t: 1587692330791, v: 495662},
	{t: 1587692345791, v: 495875}, {t: 1587692360801, v: 496082}, {t: 1587692375791, v: 496196}, {t: 1587692390791, v: 496245}, {t: 1587692405791, v: 496295}, {t: 1587692420791, v: 496365}, {t: 1587692435791, v: 496401}, {t: 1587692450791, v: 496452}, {t: 1587692465791, v: 496491}, {t: 1587692480791, v: 496544}, {t: 1587692542149, v: 496544}, {t: 1587692557139, v: 496640},
	{t: 1587692572139, v: 496851}, {t: 1587692587139, v: 497047}, {t: 1587692602144, v: 497264}, {t: 1587692617139, v: 497529}, {t: 1587692632139, v: 497717}, {t: 1587692647139, v: 497945}, {t: 1587692662154, v: 498179}, {t: 1587692677139, v: 498466}, {t: 1587692692139, v: 498642}, {t: 1587692707139, v: 498839}, {t: 1587692722139, v: 499021}, {t: 1587692737139, v: 499177},
	{t: 1587692752139, v: 499345}, {t: 1587692767139, v: 499518}, {t: 1587692782149, v: 499726}, {t: 1587692797139, v: 499980}, {t: 1587692812139, v: 500196}, {t: 1587692827139, v: 500366}, {t: 1587692842139, v: 500524}, {t: 1587692857139, v: 500734}, {t: 1587692872139, v: 500966}, {t: 1587692887139, v: 501185}, {t: 1587692902139, v: 501253}, {t: 1587692917153, v: 501411},
	{t: 1587692932139, v: 501670}, {t: 1587692947139, v: 501857}, {t: 1587692962139, v: 502110}, {t: 1587692977155, v: 502287}, {t: 1587692992139, v: 502569}, {t: 1587693007139, v: 502749}, {t: 1587693022139, v: 502938}, {t: 1587693037139, v: 503197}, {t: 1587693052139, v: 503435}, {t: 1587693067139, v: 503637}, {t: 1587693082139, v: 503880}, {t: 1587693097139, v: 504034},
	{t: 1587693112139, v: 504186}, {t: 1587693127139, v: 504369}, {t: 1587693142139, v: 504597}, {t: 1587693157139, v: 504748}, {t: 1587693172139, v: 505063}, {t: 1587693187139, v: 505251}, {t: 1587693202139, v: 505443}, {t: 1587693217139, v: 505642}, {t: 1587693232139, v: 505943}, {t: 1587693247155, v: 506095}, {t: 1587693262139, v: 506316}, {t: 1587693277139, v: 506531},
	{t: 1587693292139, v: 506807}, {t: 1587693307139, v: 507017}, {t: 1587693322139, v: 507293}, {t: 1587693337139, v: 507537}, {t: 1587693352139, v: 507788}, {t: 1587693367139, v: 507998}, {t: 1587693382139, v: 508317}, {t: 1587693397139, v: 508577}, {t: 1587693412139, v: 508777}, {t: 1587693427139, v: 508989}, {t: 1587693442163, v: 509281}, {t: 1587693457139, v: 509484},
	{t: 1587693472139, v: 509720}, {t: 1587693487139, v: 509979}, {t: 1587693502139, v: 510189}, {t: 1587693517139, v: 510505}, {t: 1587693532139, v: 510661}, {t: 1587693547139, v: 510866}, {t: 1587693562139, v: 511131}, {t: 1587693577139, v: 511321}, {t: 1587693592139, v: 511495},
}

func TestDedupSeriesSet(t *testing.T) {
	tests := []struct {
		name        string
		input       []series
		exp         []series
		dedupLabels map[string]struct{}
		isCounter   bool
	}{
		{
			name: "Single dedup label",
			input: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-1"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-2"}},
					samples: []sample{{60000, 3}, {70000, 4}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}},
					samples: []sample{{200000, 5}, {210000, 6}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}, {Name: "replica", Value: "replica-1"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}},
					samples: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}, {200000, 5}, {210000, 6}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
			dedupLabels: map[string]struct{}{
				"replica": {},
			},
		},
		{
			// Regression tests against: https://github.com/thanos-io/thanos/issues/2645.
			// We were panicking on requests with more replica labels than overall labels in any series.
			name: "Regression tests against #2645",
			input: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-1"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-2"}},
					samples: []sample{{60000, 3}, {70000, 4}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}},
					samples: []sample{{100000, 10}, {150000, 20}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				},
			},
			exp: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}, {100000, 10}, {150000, 20}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				},
			},
			dedupLabels: map[string]struct{}{"replica": {}, "replica2": {}, "replica3": {}, "replica4": {}, "replica5": {}, "replica6": {}, "replica7": {}},
		},
		{
			name: "Multi dedup label",
			input: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-1"}, {Name: "replicaA", Value: "replica-1"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-2"}, {Name: "replicaA", Value: "replica-2"}},
					samples: []sample{{60000, 3}, {70000, 4}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}, {Name: "replicaA", Value: "replica-3"}},
					samples: []sample{{200000, 5}, {210000, 6}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}, {Name: "replica", Value: "replica-1"}, {Name: "replicaA", Value: "replica-1"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}, {Name: "replicaA", Value: "replica-3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}, {Name: "replicaA", Value: "replica-3"}},
					samples: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}, {200000, 5}, {210000, 6}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
			dedupLabels: map[string]struct{}{
				"replica":  {},
				"replicaA": {},
			},
		},
		{
			name: "Multi dedup label - some series don't have all dedup labels",
			input: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-1"}, {Name: "replicaA", Value: "replica-1"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-2"}},
					samples: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
			dedupLabels: map[string]struct{}{
				"replica":  {},
				"replicaA": {},
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
					lset: labels.Labels{{Name: "replica", Value: "01"}},
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
					lset: labels.Labels{{Name: "replica", Value: "02"}},
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
					lset:    labels.Labels{},
					samples: []sample{{10000, 8}, {20000, 9}, {45001, 9}, {55001, 10}, {65001, 11}, {90000, 14}, {100000, 15}},
				},
			},
			dedupLabels: map[string]struct{}{
				"replica": {},
			},
		},
		{
			// Same thing but not for counter should not adjust anything.
			name:      "Regression test with no counter adjustment",
			isCounter: false,
			input: []series{
				{
					lset: labels.Labels{{Name: "replica", Value: "01"}},
					samples: []sample{
						{10000, 8.0}, {20000, 9.0}, {50001, 9 + 1.0}, {60000, 9 + 2.0}, {70000, 9 + 3.0}, {80000, 9 + 4.0}, {90000, 9 + 5.0}, {100000, 9 + 6.0},
					},
				}, {
					lset: labels.Labels{{Name: "replica", Value: "02"}},
					samples: []sample{
						{10001, 8.0}, {45001, 8 + 0.5}, {55001, 8 + 1.5}, {65001, 8 + 2.5},
					},
				},
			},
			exp: []series{
				{
					lset:    labels.Labels{},
					samples: []sample{{10000, 8}, {20000, 9}, {45001, 8.5}, {55001, 9.5}, {65001, 10.5}, {90000, 14}, {100000, 15}},
				},
			},
			dedupLabels: map[string]struct{}{"replica": {}},
		},
		{
			// Regression test on real data against https://github.com/thanos-io/thanos/issues/2401.
			// Real data with stale marker after downsample.CounterSeriesIterator (required for downsampling + rate).
			name:      "Regression test on real data against 2401",
			isCounter: true,
			input: []series{
				{
					lset: labels.Labels{{Name: "replica", Value: "01"}},
					samples: []sample{
						{t: 1587690007139, v: 461993}, {t: 1587690022139, v: 462164}, {t: 1587690037139, v: 462409}, {t: 1587690052139, v: 462662}, {t: 1587690067139, v: 462824}, {t: 1587690082139, v: 462987}, {t: 1587690097155, v: 463108}, {t: 1587690112139, v: 463261}, {t: 1587690127139, v: 463465}, {t: 1587690142139, v: 463642},
						{t: 1587690157139, v: 463823}, {t: 1587690172139, v: 464065}, {t: 1587690187139, v: 464333}, {t: 1587690202139, v: 464566}, {t: 1587690217139, v: 464811}, {t: 1587690232140, v: 465032}, {t: 1587690247139, v: 465229}, {t: 1587690262139, v: 465445}, {t: 1587690277139, v: 465700}, {t: 1587690292139, v: 465884},
						{t: 1587690307139, v: 466083}, {t: 1587690322139, v: 466250}, {t: 1587690337150, v: 466534}, {t: 1587690352139, v: 466791}, {t: 1587690367139, v: 466970}, {t: 1587690382139, v: 467149}, {t: 1587690397139, v: 467265}, {t: 1587690412139, v: 467383}, {t: 1587690427139, v: 467647}, {t: 1587690442139, v: 467943},
						{t: 1587690457139, v: 468121}, {t: 1587690472139, v: 468294}, {t: 1587690487139, v: 468545}, {t: 1587690502139, v: 468676}, {t: 1587690517139, v: 468879}, {t: 1587690532139, v: 469154}, {t: 1587690547139, v: 469281}, {t: 1587690562139, v: 469512}, {t: 1587690577139, v: 469783}, {t: 1587690592139, v: 469964},
						{t: 1587690607139, v: 470171}, {t: 1587690622139, v: 470355}, {t: 1587690637139, v: 470656}, {t: 1587690652139, v: 470845}, {t: 1587690667139, v: 471077}, {t: 1587690682139, v: 471315}, {t: 1587690697139, v: 471535}, {t: 1587690712139, v: 471766}, {t: 1587690727139, v: 472002}, {t: 1587690742139, v: 472171},
						{t: 1587690757139, v: 472354}, {t: 1587690772139, v: 472736}, {t: 1587690787139, v: 472948}, {t: 1587690802139, v: 473259}, {t: 1587690817139, v: 473460}, {t: 1587690832139, v: 473753}, {t: 1587690847139, v: 474007}, {t: 1587690862139, v: 474286}, {t: 1587690877139, v: 474423}, {t: 1587690892139, v: 474788},
						{t: 1587690907139, v: 474925}, {t: 1587690922139, v: 475031}, {t: 1587690937139, v: 475316}, {t: 1587690952139, v: 475573}, {t: 1587690967139, v: 475784}, {t: 1587690982139, v: 475992}, {t: 1587690997139, v: 476341}, {t: 1587691012139, v: 476541}, {t: 1587691027139, v: 476890}, {t: 1587691042139, v: 477033},
						{t: 1587691057139, v: 477305}, {t: 1587691072139, v: 477577}, {t: 1587691087139, v: 477771}, {t: 1587691102139, v: 478012}, {t: 1587691117139, v: 478296}, {t: 1587691132139, v: 478559}, {t: 1587691147139, v: 478744}, {t: 1587691162139, v: 478950}, {t: 1587691177139, v: 479201}, {t: 1587691192139, v: 479388},
						{t: 1587691207139, v: 479638}, {t: 1587691222154, v: 479907}, {t: 1587691237139, v: 480008}, {t: 1587691252139, v: 480167}, {t: 1587691267139, v: 480472}, {t: 1587691282157, v: 480615}, {t: 1587691297139, v: 480771}, {t: 1587691312139, v: 481027}, {t: 1587691327139, v: 481212}, {t: 1587691342159, v: 481395},
						{t: 1587691357139, v: 481598}, {t: 1587691372139, v: 481786}, {t: 1587691387139, v: 482003}, {t: 1587691402141, v: 482236}, {t: 1587691417139, v: 482508}, {t: 1587691432139, v: 482636}, {t: 1587691447139, v: 482780}, {t: 1587691462139, v: 483059}, {t: 1587691477139, v: 483357}, {t: 1587691492139, v: 483566},
						{t: 1587691507139, v: 483711}, {t: 1587691522139, v: 483838}, {t: 1587691537139, v: 484091}, {t: 1587691552139, v: 484254}, {t: 1587691567139, v: 484479}, {t: 1587691582139, v: 484748}, {t: 1587691597139, v: 484978}, {t: 1587691612139, v: 485271}, {t: 1587691627139, v: 485488}, {t: 1587691642139, v: 485700},
						{t: 1587691657139, v: 485945}, {t: 1587691672139, v: 486228}, {t: 1587691687139, v: 486588}, {t: 1587691702139, v: 486691}, {t: 1587691717139, v: 486881}, {t: 1587691732139, v: 487046}, {t: 1587691747139, v: 487291}, {t: 1587691762177, v: 487410}, {t: 1587691777139, v: 487571}, {t: 1587691792139, v: 487799},
						{t: 1587691807139, v: 488050}, {t: 1587691822139, v: 488241}, {t: 1587691837139, v: 488424}, {t: 1587691852139, v: 488629}, {t: 1587691867139, v: 488875}, {t: 1587691882139, v: 489017}, {t: 1587691897139, v: 489254}, {t: 1587691912139, v: 489545}, {t: 1587691927139, v: 489778}, {t: 1587691942139, v: 489912},
						{t: 1587691957139, v: 490084}, {t: 1587691972139, v: 490364}, {t: 1587691987139, v: 490510}, {t: 1587692002139, v: 490744}, {t: 1587692017139, v: 490880}, {t: 1587692032139, v: 491025}, {t: 1587692047139, v: 491297}, {t: 1587692062155, v: 491557}, {t: 1587692077139, v: 491839}, {t: 1587692092139, v: 492065},
						{t: 1587692107139, v: 492234}, {t: 1587692122139, v: 492526}, {t: 1587692137139, v: 492767}, {t: 1587692152139, v: 492967}, {t: 1587692167139, v: 493218}, {t: 1587692182139, v: 493442}, {t: 1587692197139, v: 493647}, {t: 1587692212139, v: 493920}, {t: 1587692227139, v: 494170}, {t: 1587692242139, v: 494358},
						{t: 1587692257139, v: 494632}, {t: 1587692272139, v: 494800}, {t: 1587692287139, v: 495026}, {t: 1587692302139, v: 495222}, {t: 1587692317139, v: 495433}, {t: 1587692332139, v: 495677}, {t: 1587692347139, v: 495901}, {t: 1587692362139, v: 496107}, {t: 1587692377139, v: 496196}, {t: 1587692392139, v: 496245},
						{t: 1587692407139, v: 496300}, {t: 1587692422159, v: 496365}, {t: 1587692437139, v: 496401}, {t: 1587692452139, v: 496452}, {t: 1587692467139, v: 496532}, {t: 1587692542149, v: 496537}, {t: 1587692557139, v: 496633}, {t: 1587692572139, v: 496844}, {t: 1587692587139, v: 497040}, {t: 1587692602144, v: 497257},
						{t: 1587692617139, v: 497522}, {t: 1587692632139, v: 497710}, {t: 1587692647139, v: 497938}, {t: 1587692662154, v: 498172}, {t: 1587692677139, v: 498459}, {t: 1587692692139, v: 498635}, {t: 1587692707139, v: 498832}, {t: 1587692722139, v: 499014}, {t: 1587692737139, v: 499170}, {t: 1587692752139, v: 499338},
						{t: 1587692767139, v: 499511}, {t: 1587692782149, v: 499719}, {t: 1587692797139, v: 499973}, {t: 1587692812139, v: 500189}, {t: 1587692827139, v: 500359}, {t: 1587692842139, v: 500517}, {t: 1587692857139, v: 500727}, {t: 1587692872139, v: 500959}, {t: 1587692887139, v: 501178}, {t: 1587692902139, v: 501246},
						{t: 1587692917153, v: 501404}, {t: 1587692932139, v: 501663}, {t: 1587692947139, v: 501850}, {t: 1587692962139, v: 502103}, {t: 1587692977155, v: 502280}, {t: 1587692992139, v: 502562}, {t: 1587693007139, v: 502742}, {t: 1587693022139, v: 502931}, {t: 1587693037139, v: 503190}, {t: 1587693052139, v: 503428},
						{t: 1587693067139, v: 503630}, {t: 1587693082139, v: 503873}, {t: 1587693097139, v: 504027}, {t: 1587693112139, v: 504179}, {t: 1587693127139, v: 504362}, {t: 1587693142139, v: 504590}, {t: 1587693157139, v: 504741}, {t: 1587693172139, v: 505056}, {t: 1587693187139, v: 505244}, {t: 1587693202139, v: 505436},
						{t: 1587693217139, v: 505635}, {t: 1587693232139, v: 505936}, {t: 1587693247155, v: 506088}, {t: 1587693262139, v: 506309}, {t: 1587693277139, v: 506524}, {t: 1587693292139, v: 506800}, {t: 1587693307139, v: 507010}, {t: 1587693322139, v: 507286}, {t: 1587693337139, v: 507530}, {t: 1587693352139, v: 507781},
						{t: 1587693367139, v: 507991}, {t: 1587693382139, v: 508310}, {t: 1587693397139, v: 508570}, {t: 1587693412139, v: 508770}, {t: 1587693427139, v: 508982}, {t: 1587693442163, v: 509274}, {t: 1587693457139, v: 509477}, {t: 1587693472139, v: 509713}, {t: 1587693487139, v: 509972}, {t: 1587693502139, v: 510182},
						{t: 1587693517139, v: 510498}, {t: 1587693532139, v: 510654}, {t: 1587693547139, v: 510859}, {t: 1587693562139, v: 511124}, {t: 1587693577139, v: 511314}, {t: 1587693592139, v: 511488},
					},
				}, {
					lset: labels.Labels{{Name: "replica", Value: "02"}},
					samples: []sample{
						{t: 1587690005791, v: 461968}, {t: 1587690020791, v: 462151}, {t: 1587690035797, v: 462336}, {t: 1587690050791, v: 462650}, {t: 1587690065791, v: 462813}, {t: 1587690080791, v: 462987}, {t: 1587690095791, v: 463095}, {t: 1587690110791, v: 463247}, {t: 1587690125791, v: 463440}, {t: 1587690140791, v: 463642}, {t: 1587690155791, v: 463811},
						{t: 1587690170791, v: 464027}, {t: 1587690185791, v: 464308}, {t: 1587690200791, v: 464514}, {t: 1587690215791, v: 464798}, {t: 1587690230791, v: 465018}, {t: 1587690245791, v: 465215}, {t: 1587690260813, v: 465431}, {t: 1587690275791, v: 465651}, {t: 1587690290791, v: 465870}, {t: 1587690305791, v: 466070}, {t: 1587690320792, v: 466248},
						{t: 1587690335791, v: 466506}, {t: 1587690350791, v: 466766}, {t: 1587690365791, v: 466970}, {t: 1587690380791, v: 467123}, {t: 1587690395791, v: 467265}, {t: 1587690410791, v: 467383}, {t: 1587690425791, v: 467629}, {t: 1587690440791, v: 467931}, {t: 1587690455791, v: 468097}, {t: 1587690470791, v: 468281}, {t: 1587690485791, v: 468477},
						{t: 1587690500791, v: 468649}, {t: 1587690515791, v: 468867}, {t: 1587690530791, v: 469150}, {t: 1587690545791, v: 469268}, {t: 1587690560791, v: 469488}, {t: 1587690575791, v: 469742}, {t: 1587690590791, v: 469951}, {t: 1587690605791, v: 470131}, {t: 1587690620791, v: 470337}, {t: 1587690635791, v: 470631}, {t: 1587690650791, v: 470832},
						{t: 1587690665791, v: 471077}, {t: 1587690680791, v: 471311}, {t: 1587690695791, v: 471473}, {t: 1587690710791, v: 471728}, {t: 1587690725791, v: 472002}, {t: 1587690740791, v: 472158}, {t: 1587690755791, v: 472329}, {t: 1587690770791, v: 472722}, {t: 1587690785791, v: 472925}, {t: 1587690800791, v: 473220}, {t: 1587690815791, v: 473460},
						{t: 1587690830791, v: 473748}, {t: 1587690845791, v: 473968}, {t: 1587690860791, v: 474261}, {t: 1587690875791, v: 474418}, {t: 1587690890791, v: 474726}, {t: 1587690905791, v: 474913}, {t: 1587690920791, v: 475031}, {t: 1587690935791, v: 475284}, {t: 1587690950791, v: 475563}, {t: 1587690965791, v: 475762}, {t: 1587690980791, v: 475945},
						{t: 1587690995791, v: 476302}, {t: 1587691010791, v: 476501}, {t: 1587691025791, v: 476849}, {t: 1587691040800, v: 477020}, {t: 1587691055791, v: 477280}, {t: 1587691070791, v: 477549}, {t: 1587691085791, v: 477758}, {t: 1587691100817, v: 477960}, {t: 1587691115791, v: 478261}, {t: 1587691130791, v: 478559}, {t: 1587691145791, v: 478704},
						{t: 1587691160804, v: 478950}, {t: 1587691175791, v: 479173}, {t: 1587691190791, v: 479368}, {t: 1587691205791, v: 479625}, {t: 1587691220805, v: 479866}, {t: 1587691235791, v: 480008}, {t: 1587691250791, v: 480155}, {t: 1587691265791, v: 480472}, {t: 1587691280811, v: 480598}, {t: 1587691295791, v: 480771}, {t: 1587691310791, v: 480996},
						{t: 1587691325791, v: 481200}, {t: 1587691340803, v: 481381}, {t: 1587691355791, v: 481584}, {t: 1587691370791, v: 481759}, {t: 1587691385791, v: 482003}, {t: 1587691400803, v: 482189}, {t: 1587691415791, v: 482457}, {t: 1587691430791, v: 482623}, {t: 1587691445791, v: 482768}, {t: 1587691460804, v: 483036}, {t: 1587691475791, v: 483322},
						{t: 1587691490791, v: 483566}, {t: 1587691505791, v: 483709}, {t: 1587691520807, v: 483838}, {t: 1587691535791, v: 484091}, {t: 1587691550791, v: 484236}, {t: 1587691565791, v: 484454}, {t: 1587691580816, v: 484710}, {t: 1587691595791, v: 484978}, {t: 1587691610791, v: 485271}, {t: 1587691625791, v: 485476}, {t: 1587691640792, v: 485640},
						{t: 1587691655791, v: 485921}, {t: 1587691670791, v: 486201}, {t: 1587691685791, v: 486555}, {t: 1587691700791, v: 486691}, {t: 1587691715791, v: 486831}, {t: 1587691730791, v: 487033}, {t: 1587691745791, v: 487268}, {t: 1587691760803, v: 487370}, {t: 1587691775791, v: 487571}, {t: 1587691790791, v: 487787}, {t: 1587691805791, v: 488036},
						{t: 1587691820791, v: 488241}, {t: 1587691835791, v: 488411}, {t: 1587691850791, v: 488625}, {t: 1587691865791, v: 488868}, {t: 1587691880791, v: 489005}, {t: 1587691895791, v: 489237}, {t: 1587691910791, v: 489545}, {t: 1587691925791, v: 489750}, {t: 1587691940791, v: 489899}, {t: 1587691955791, v: 490048}, {t: 1587691970791, v: 490364},
						{t: 1587691985791, v: 490485}, {t: 1587692000791, v: 490722}, {t: 1587692015791, v: 490866}, {t: 1587692030791, v: 491025}, {t: 1587692045791, v: 491286}, {t: 1587692060816, v: 491543}, {t: 1587692075791, v: 491787}, {t: 1587692090791, v: 492065}, {t: 1587692105791, v: 492223}, {t: 1587692120816, v: 492501}, {t: 1587692135791, v: 492767},
						{t: 1587692150791, v: 492955}, {t: 1587692165791, v: 493194}, {t: 1587692180792, v: 493402}, {t: 1587692195791, v: 493647}, {t: 1587692210791, v: 493897}, {t: 1587692225791, v: 494117}, {t: 1587692240805, v: 494356}, {t: 1587692255791, v: 494620}, {t: 1587692270791, v: 494762}, {t: 1587692285791, v: 495001}, {t: 1587692300805, v: 495222},
						{t: 1587692315791, v: 495393}, {t: 1587692330791, v: 495662}, {t: 1587692345791, v: 495875}, {t: 1587692360801, v: 496082}, {t: 1587692375791, v: 496196}, {t: 1587692390791, v: 496245}, {t: 1587692405791, v: 496295}, {t: 1587692420791, v: 496365}, {t: 1587692435791, v: 496401}, {t: 1587692450791, v: 496452}, {t: 1587692465791, v: 496491},
						{t: 1587692480791, v: 496544}, {t: 1587692555791, v: 496619}, {t: 1587692570791, v: 496852}, {t: 1587692585791, v: 497052}, {t: 1587692600791, v: 497245}, {t: 1587692615791, v: 497529}, {t: 1587692630791, v: 497697}, {t: 1587692645791, v: 497909}, {t: 1587692660791, v: 498156}, {t: 1587692675803, v: 498466}, {t: 1587692690791, v: 498647},
						{t: 1587692705791, v: 498805}, {t: 1587692720791, v: 499013}, {t: 1587692735805, v: 499169}, {t: 1587692750791, v: 499345}, {t: 1587692765791, v: 499499}, {t: 1587692780791, v: 499731}, {t: 1587692795806, v: 499972}, {t: 1587692810791, v: 500201}, {t: 1587692825791, v: 500354}, {t: 1587692840791, v: 500512}, {t: 1587692855791, v: 500739},
						{t: 1587692870791, v: 500958}, {t: 1587692885791, v: 501190}, {t: 1587692900791, v: 501233}, {t: 1587692915791, v: 501391}, {t: 1587692930791, v: 501649}, {t: 1587692945791, v: 501853}, {t: 1587692960791, v: 502065}, {t: 1587692975791, v: 502239}, {t: 1587692990810, v: 502554}, {t: 1587693005791, v: 502754}, {t: 1587693020791, v: 502938},
						{t: 1587693035791, v: 503141}, {t: 1587693050791, v: 503416}, {t: 1587693065791, v: 503642}, {t: 1587693080791, v: 503873}, {t: 1587693095791, v: 504014}, {t: 1587693110791, v: 504178}, {t: 1587693125821, v: 504374}, {t: 1587693140791, v: 504578}, {t: 1587693155791, v: 504753}, {t: 1587693170791, v: 505043}, {t: 1587693185791, v: 505232},
						{t: 1587693200791, v: 505437}, {t: 1587693215791, v: 505596}, {t: 1587693230791, v: 505923}, {t: 1587693245791, v: 506088}, {t: 1587693260791, v: 506307}, {t: 1587693275791, v: 506518}, {t: 1587693290791, v: 506786}, {t: 1587693305791, v: 507008}, {t: 1587693320803, v: 507260}, {t: 1587693335791, v: 507519}, {t: 1587693350791, v: 507776},
						{t: 1587693365791, v: 508003}, {t: 1587693380791, v: 508322}, {t: 1587693395804, v: 508551}, {t: 1587693410791, v: 508750}, {t: 1587693425791, v: 508994}, {t: 1587693440791, v: 509237}, {t: 1587693455791, v: 509452}, {t: 1587693470791, v: 509702}, {t: 1587693485791, v: 509971}, {t: 1587693500791, v: 510147}, {t: 1587693515791, v: 510471},
						{t: 1587693530816, v: 510666}, {t: 1587693545791, v: 510871}, {t: 1587693560791, v: 511123}, {t: 1587693575791, v: 511303}, {t: 1587693590791, v: 511500},
					},
				},
			},
			exp: []series{
				{
					lset:    labels.Labels{},
					samples: expectedRealSeriesWithStaleMarkerDeduplicatedForRate,
				},
			},
			dedupLabels: map[string]struct{}{"replica": {}},
		},
	}

	for _, tcase := range tests {
		t.Run(tcase.name, func(t *testing.T) {
			// If it is a counter then pass a function which expects a counter.
			f := ""
			if tcase.isCounter {
				f = "rate"
			}
			dedupSet := NewSeriesSet(&mockedSeriesSet{series: tcase.input}, tcase.dedupLabels, f, false)
			var ats []storage.Series
			for dedupSet.Next() {
				ats = append(ats, dedupSet.At())
			}
			testutil.Ok(t, dedupSet.Err())
			testutil.Equals(t, len(tcase.exp), len(ats))

			for i, s := range ats {
				testutil.Equals(t, tcase.exp[i].lset, s.Labels(), "labels mismatch for series %v", i)
				res := expandSeries(t, s.Iterator())
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

		for i := 0; i < b.N; i++ {
			s1 = append(s1, sample{t: int64(i * 10000), v: 1})
		}
		for i := 0; i < b.N; i++ {
			s2 = append(s2, sample{t: int64(i * 10000), v: 2})
		}
		run(b, s1, s2)
	})
	b.Run("fixed-delta", func(b *testing.B) {
		var s1, s2 []sample

		for i := 0; i < b.N; i++ {
			s1 = append(s1, sample{t: int64(i * 10000), v: 1})
		}
		for i := 0; i < b.N; i++ {
			s2 = append(s2, sample{t: int64(i*10000) + 10, v: 2})
		}
		run(b, s1, s2)
	})
	b.Run("minor-rand-delta", func(b *testing.B) {
		var s1, s2 []sample

		for i := 0; i < b.N; i++ {
			s1 = append(s1, sample{t: int64(i*10000) + rand.Int63n(5000), v: 1})
		}
		for i := 0; i < b.N; i++ {
			s2 = append(s2, sample{t: int64(i*10000) + +rand.Int63n(5000), v: 2})
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

func TestPushdownSeriesIterator(t *testing.T) {
	cases := []struct {
		a, b, exp []sample
		function  string
		tcase     string
	}{
		{
			tcase:    "simple case",
			a:        []sample{{10000, 10}, {20000, 11}, {30000, 12}, {40000, 13}},
			b:        []sample{{10000, 20}, {20000, 21}, {30000, 22}, {40000, 23}},
			exp:      []sample{{10000, 20}, {20000, 21}, {30000, 22}, {40000, 23}},
			function: "max",
		},
		{
			tcase:    "gaps but catches up",
			a:        []sample{{10000, 10}, {20000, 11}, {30000, 12}, {40000, 13}},
			b:        []sample{{10000, 20}, {40000, 23}},
			exp:      []sample{{10000, 20}, {20000, 11}, {30000, 12}, {40000, 23}},
			function: "max",
		},
	}
	for _, c := range cases {
		t.Run(c.tcase, func(t *testing.T) {
			it := newPushdownSeriesIterator(
				noopAdjustableSeriesIterator{newMockedSeriesIterator(c.a)},
				noopAdjustableSeriesIterator{newMockedSeriesIterator(c.b)},
				c.function,
			)
			res := expandSeries(t, noopAdjustableSeriesIterator{it})
			testutil.Equals(t, c.exp, res)
		})

	}
}
