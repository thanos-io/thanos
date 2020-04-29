// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"

	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestQueryableCreator_MaxResolution(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()
	testProxy := &storeServer{resps: []*storepb.SeriesResponse{}}
	queryableCreator := NewQueryableCreator(nil, testProxy)

	oneHourMillis := int64(1*time.Hour) / int64(time.Millisecond)
	queryable := queryableCreator(false, nil, oneHourMillis, false, false)

	q, err := queryable.Querier(context.Background(), 0, 42)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, q.Close()) }()

	querierActual, ok := q.(*querier)

	testutil.Assert(t, ok == true, "expected it to be a querier")
	testutil.Assert(t, querierActual.maxResolutionMillis == oneHourMillis, "expected max source resolution to be 1 hour in milliseconds")

}

// Tests E2E how PromQL works with downsampled data.
func TestQuerier_DownsampledData(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()
	testProxy := &storeServer{
		resps: []*storepb.SeriesResponse{
			storeSeriesResponse(t, labels.FromStrings("__name__", "a", "zzz", "a", "aaa", "bbb"), []sample{{99, 1}, {199, 5}}),                   // Downsampled chunk from Store.
			storeSeriesResponse(t, labels.FromStrings("__name__", "a", "zzz", "b", "bbbb", "eee"), []sample{{99, 3}, {199, 8}}),                  // Downsampled chunk from Store.
			storeSeriesResponse(t, labels.FromStrings("__name__", "a", "zzz", "c", "qwe", "wqeqw"), []sample{{99, 5}, {199, 15}}),                // Downsampled chunk from Store.
			storeSeriesResponse(t, labels.FromStrings("__name__", "a", "zzz", "c", "htgtreytr", "vbnbv"), []sample{{99, 123}, {199, 15}}),        // Downsampled chunk from Store.
			storeSeriesResponse(t, labels.FromStrings("__name__", "a", "zzz", "d", "asdsad", "qweqwewq"), []sample{{22, 5}, {44, 8}, {199, 15}}), // Raw chunk from Sidecar.
			storeSeriesResponse(t, labels.FromStrings("__name__", "a", "zzz", "d", "asdsad", "qweqwebb"), []sample{{22, 5}, {44, 8}, {199, 15}}), // Raw chunk from Sidecar.
		},
	}

	q := NewQueryableCreator(nil, testProxy)(false, nil, 9999999, false, false)

	engine := promql.NewEngine(
		promql.EngineOpts{
			MaxConcurrent: 10,
			MaxSamples:    math.MaxInt32,
			Timeout:       10 * time.Second,
		},
	)

	// Minimal function to parse time.Time.
	ptm := func(in string) time.Time {
		fl, _ := strconv.ParseFloat(in, 64)
		s, ns := math.Modf(fl)
		return time.Unix(int64(s), int64(ns*float64(time.Second)))
	}

	st := ptm("0")
	ed := ptm("0.2")
	qry, err := engine.NewRangeQuery(
		q,
		"sum(a) by (zzz)",
		st,
		ed,
		100*time.Millisecond,
	)
	testutil.Ok(t, err)

	res := qry.Exec(context.Background())
	testutil.Ok(t, res.Err)
	m, err := res.Matrix()
	testutil.Ok(t, err)
	ser := []promql.Series(m)

	testutil.Assert(t, len(ser) == 4, "should return 4 series (got %d)", len(ser))

	exp := []promql.Series{
		{
			Metric: labels.FromStrings("zzz", "a"),
			Points: []promql.Point{
				{
					T: 100,
					V: 1,
				},
				{
					T: 200,
					V: 5,
				},
			},
		},
		{
			Metric: labels.FromStrings("zzz", "b"),
			Points: []promql.Point{
				{
					T: 100,
					V: 3,
				},
				{
					T: 200,
					V: 8,
				},
			},
		},
		{
			Metric: labels.FromStrings("zzz", "c"),
			// Test case: downsampling code adds all of the samples in the
			// 5 minute window of each series and pre-aggregates the data. However,
			// Prometheus engine code only takes the latest sample in each time window of
			// the retrieved data. Since we were operating in pre-aggregated data here, it lead
			// to overinflated values.
			Points: []promql.Point{
				{
					T: 100,
					V: 128,
				},
				{
					T: 200,
					V: 30,
				},
			},
		},
		{
			Metric: labels.FromStrings("zzz", "d"),
			// Test case: Prometheus engine in each time window selects the sample
			// which is closest to the boundaries and adds up the different dimensions.
			Points: []promql.Point{
				{
					T: 100,
					V: 16,
				},
				{
					T: 200,
					V: 30,
				},
			},
		},
	}

	if !reflect.DeepEqual(ser, exp) {
		t.Fatalf("response does not match, expected:\n%+v\ngot:\n%+v", exp, ser)
	}
}

func TestQuerier_Select(t *testing.T) {
	type series struct {
		lset    labels.Labels
		samples []sample
	}
	for _, tcase := range []struct {
		name     string
		storeAPI storepb.StoreServer

		mint, maxt    int64
		matchers      []*labels.Matcher
		replicaLabels []string
		dedup         bool
		hints         *storage.SelectParams

		expected        []series
		expectedWarning string
	}{
		{
			name: "select overlapping data with partial error",
			storeAPI: &storeServer{
				resps: []*storepb.SeriesResponse{
					storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
					storepb.NewWarnSeriesResponse(errors.New("partial error")),
					storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{5, 5}, {6, 6}, {7, 7}}),
					storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{5, 5}, {6, 66}}), // Overlap samples for some reason.
					storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{2, 2}, {3, 3}, {4, 4}}, []sample{{1, 1}, {2, 2}, {3, 3}}),
					storeSeriesResponse(t, labels.FromStrings("a", "c"), []sample{{100, 1}, {300, 3}, {400, 4}}),
				},
			},
			mint: 1, maxt: 300,
			expected: []series{
				{
					lset:    labels.FromStrings("a", "a"),
					samples: []sample{{2, 1}, {3, 2}, {5, 5}, {6, 6}, {7, 7}},
				},
				{
					lset:    labels.FromStrings("a", "b"),
					samples: []sample{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
				},
				{
					lset:    labels.FromStrings("a", "c"),
					samples: []sample{{100, 1}, {300, 3}},
				},
			},
			expectedWarning: "partial error",
		},
		{
			// Regression test against https://github.com/thanos-io/thanos/issues/2401: Overlapping chunks.
			// Thanks to @Superq and GitLab for real data reproducing this!
			// TODO(bwplotka): This still does not reproduce bug, waiting for more data (:
			name: "overlapping chunks",
			storeAPI: func() storepb.StoreServer {
				s, err := store.NewLocalStoreFromJSONMmappableFile(log.NewLogfmtLogger(os.Stderr), component.Debug, nil, "./testdata/issue2401-seriesresponses.json", store.ScanGRPCCurlProtoStreamMessages)
				testutil.Ok(t, err)
				return s
			}(),
			mint: 1582329611249,
			maxt: 1582331381245,
			matchers: []*labels.Matcher{{
				Value: "gitlab_transaction_cache_read_hit_count_total",
				Name:  "__name__",
				Type:  labels.MatchEqual,
			}},
			expected: []series{
				{
					lset: labels.FromStrings(
						"__name__", "gitlab_transaction_cache_read_hit_count_total",
						"action", "widget.json",
						"controller", "Projects::MergeRequests::ContentController",
						"env", "gprd",
						"environment", "gprd",
						"fqdn", "web-16-sv-gprd.c.gitlab-production.internal",
						"instance", "web-16-sv-gprd.c.gitlab-production.internal:8083",
						"job", "gitlab-rails",
						"monitor", "app",
						"provider", "gcp",
						"region", "us-east",
						"replica", "01",
						"shard", "default",
						"stage", "main",
						"tier", "sv",
						"type", "web",
					),
					samples: []sample{
						{t: 1582329611249, v: 3.395395e+06}, {t: 1582329626245, v: 3.39592e+06}, {t: 1582329641245, v: 3.396402e+06}, {t: 1582329656245, v: 3.396882e+06}, {t: 1582329671245, v: 3.397394e+06}, {t: 1582329686245, v: 3.397804e+06},
						{t: 1582329701245, v: 3.398296e+06}, {t: 1582329716245, v: 3.398878e+06}, {t: 1582329731245, v: 3.399373e+06}, {t: 1582329746245, v: 3.39992e+06}, {t: 1582329761245, v: 3.400436e+06}, {t: 1582329776245, v: 3.400974e+06},
						{t: 1582329791245, v: 3.401408e+06}, {t: 1582329806245, v: 3.401901e+06}, {t: 1582329821245, v: 3.402411e+06}, {t: 1582329836245, v: 3.402944e+06}, {t: 1582329851245, v: 3.403404e+06}, {t: 1582329866245, v: 3.403735e+06},
						{t: 1582329881245, v: 3.404301e+06}, {t: 1582329896245, v: 3.404818e+06}, {t: 1582329911245, v: 3.405327e+06}, {t: 1582329926245, v: 3.405843e+06}, {t: 1582329941245, v: 3.406297e+06}, {t: 1582329956245, v: 3.406737e+06},
						{t: 1582329971245, v: 3.407212e+06}, {t: 1582329986245, v: 3.407647e+06}, {t: 1582330001245, v: 3.407929e+06}, {t: 1582330016245, v: 3.408614e+06}, {t: 1582330031245, v: 3.409217e+06}, {t: 1582330046245, v: 3.409658e+06},
						{t: 1582330061245, v: 3.410276e+06}, {t: 1582330076245, v: 3.410795e+06}, {t: 1582330091245, v: 3.411509e+06}, {t: 1582330106245, v: 3.411974e+06}, {t: 1582330121245, v: 3.412368e+06}, {t: 1582330136245, v: 3.412863e+06},
						{t: 1582330151245, v: 3.413412e+06}, {t: 1582330166245, v: 3.413783e+06}, {t: 1582330181245, v: 3.41424e+06}, {t: 1582330196245, v: 3.414634e+06}, {t: 1582330211245, v: 3.415246e+06}, {t: 1582330226245, v: 3.415615e+06},
						{t: 1582330241245, v: 3.416287e+06}, {t: 1582330256245, v: 3.416753e+06}, {t: 1582330271245, v: 3.417229e+06}, {t: 1582330286245, v: 3.417748e+06}, {t: 1582330301245, v: 3.418185e+06}, {t: 1582330316245, v: 3.418707e+06},
						{t: 1582330331245, v: 3.419142e+06}, {t: 1582330346245, v: 3.419595e+06}, {t: 1582330361245, v: 3.420042e+06}, {t: 1582330376245, v: 3.420544e+06}, {t: 1582330391245, v: 3.420926e+06}, {t: 1582330406245, v: 3.421447e+06},
						{t: 1582330421245, v: 3.421899e+06}, {t: 1582330436245, v: 3.422238e+06}, {t: 1582330451245, v: 3.422619e+06}, {t: 1582330466245, v: 3.422958e+06}, {t: 1582330481251, v: 3.423455e+06}, {t: 1582330496245, v: 3.423991e+06},
						{t: 1582330511245, v: 3.424478e+06}, {t: 1582330526245, v: 3.425018e+06}, {t: 1582330541245, v: 3.425455e+06}, {t: 1582330556245, v: 3.42597e+06}, {t: 1582330571245, v: 3.426343e+06}, {t: 1582330586245, v: 3.426881e+06},
						{t: 1582330601245, v: 3.427435e+06}, {t: 1582330616245, v: 3.42803e+06}, {t: 1582330631261, v: 3.42861e+06}, {t: 1582330646245, v: 3.429152e+06}, {t: 1582330661245, v: 3.429711e+06}, {t: 1582330676245, v: 3.430061e+06},
						{t: 1582330691245, v: 3.430594e+06}, {t: 1582330706245, v: 3.431031e+06}, {t: 1582330721245, v: 3.431528e+06}, {t: 1582330736245, v: 3.432125e+06}, {t: 1582330751245, v: 3.432611e+06}, {t: 1582330766245, v: 3.433042e+06},
						{t: 1582330781245, v: 3.433544e+06}, {t: 1582330796245, v: 3.434109e+06}, {t: 1582330811245, v: 3.434386e+06}, {t: 1582330826245, v: 3.434891e+06}, {t: 1582330841245, v: 3.43544e+06}, {t: 1582330856245, v: 3.435977e+06},
						{t: 1582330871245, v: 3.436528e+06}, {t: 1582330886245, v: 3.437145e+06}, {t: 1582330901245, v: 3.437773e+06}, {t: 1582330916245, v: 3.438278e+06}, {t: 1582330931245, v: 3.438664e+06}, {t: 1582330946245, v: 3.439135e+06},
						{t: 1582330961245, v: 3.439635e+06}, {t: 1582330976245, v: 3.440136e+06}, {t: 1582330991245, v: 3.440572e+06}, {t: 1582331006245, v: 3.441014e+06}, {t: 1582331021256, v: 3.441373e+06}, {t: 1582331036245, v: 3.441745e+06},
						{t: 1582331051245, v: 3.442336e+06}, {t: 1582331066245, v: 3.44291e+06}, {t: 1582331081245, v: 3.443478e+06}, {t: 1582331096245, v: 3.443938e+06}, {t: 1582331111245, v: 3.444436e+06}, {t: 1582331126245, v: 3.445027e+06},
						{t: 1582331141245, v: 3.445382e+06}, {t: 1582331156245, v: 3.445853e+06}, {t: 1582331171262, v: 3.446356e+06}, {t: 1582331186245, v: 3.446899e+06}, {t: 1582331201245, v: 3.44743e+06}, {t: 1582331216245, v: 3.448002e+06},
						{t: 1582331231245, v: 3.448451e+06}, {t: 1582331246245, v: 3.448974e+06}, {t: 1582331261259, v: 3.449337e+06}, {t: 1582331276245, v: 3.449841e+06}, {t: 1582331291245, v: 3.450478e+06}, {t: 1582331306245, v: 3.450903e+06},
						{t: 1582331321245, v: 3.451404e+06}, {t: 1582331336245, v: 3.451778e+06}, {t: 1582331351245, v: 3.452278e+06}, {t: 1582331366245, v: 3.452711e+06}, {t: 1582331381245, v: 3.453142e+06}},
				},
				{
					lset: labels.FromStrings(
						"__name__", "gitlab_transaction_cache_read_hit_count_total",
						"action", "widget.json",
						"controller", "Projects::MergeRequests::ContentController",
						"env", "gprd",
						"environment", "gprd",
						"fqdn", "web-16-sv-gprd.c.gitlab-production.internal",
						"instance", "web-16-sv-gprd.c.gitlab-production.internal:8083",
						"job", "gitlab-rails",
						"monitor", "app",
						"provider", "gcp",
						"region", "us-east",
						"replica", "02",
						"shard", "default",
						"stage", "main",
						"tier", "sv",
						"type", "web",
					),
					samples: []sample{
						{t: 1582329618053, v: 3.395638e+06}, {t: 1582329633053, v: 3.396163e+06}, {t: 1582329648053, v: 3.396612e+06}, {t: 1582329663053, v: 3.397174e+06}, {t: 1582329678053, v: 3.397564e+06}, {t: 1582329693053, v: 3.398039e+06},
						{t: 1582329708053, v: 3.39847e+06}, {t: 1582329723053, v: 3.399088e+06}, {t: 1582329738053, v: 3.399668e+06}, {t: 1582329753053, v: 3.400198e+06}, {t: 1582329768053, v: 3.400705e+06}, {t: 1582329783053, v: 3.401209e+06},
						{t: 1582329798053, v: 3.401606e+06}, {t: 1582329813053, v: 3.402171e+06}, {t: 1582329828054, v: 3.402601e+06}, {t: 1582329843053, v: 3.403164e+06}, {t: 1582329858053, v: 3.40357e+06}, {t: 1582329873053, v: 3.404005e+06},
						{t: 1582329888053, v: 3.404553e+06}, {t: 1582329903053, v: 3.405076e+06}, {t: 1582329918053, v: 3.40554e+06}, {t: 1582329933053, v: 3.406012e+06}, {t: 1582329948053, v: 3.406466e+06}, {t: 1582329963053, v: 3.407003e+06},
						{t: 1582329978053, v: 3.407378e+06}, {t: 1582329993053, v: 3.407728e+06}, {t: 1582330008053, v: 3.40835e+06}, {t: 1582330023053, v: 3.408935e+06}, {t: 1582330038053, v: 3.409436e+06}, {t: 1582330053053, v: 3.409904e+06},
						{t: 1582330068053, v: 3.410449e+06}, {t: 1582330083053, v: 3.411078e+06}, {t: 1582330098053, v: 3.411737e+06}, {t: 1582330113053, v: 3.41214e+06}, {t: 1582330128053, v: 3.412598e+06}, {t: 1582330143053, v: 3.413069e+06},
						{t: 1582330158053, v: 3.4136e+06}, {t: 1582330173053, v: 3.413985e+06}, {t: 1582330188053, v: 3.414443e+06}, {t: 1582330203053, v: 3.414959e+06}, {t: 1582330218053, v: 3.415411e+06}, {t: 1582330233053, v: 3.415962e+06},
						{t: 1582330248064, v: 3.416517e+06}, {t: 1582330263053, v: 3.416999e+06}, {t: 1582330278053, v: 3.417466e+06}, {t: 1582330293053, v: 3.418001e+06}, {t: 1582330308053, v: 3.418434e+06}, {t: 1582330323053, v: 3.41892e+06},
						{t: 1582330338053, v: 3.419327e+06}, {t: 1582330353053, v: 3.419814e+06}, {t: 1582330368053, v: 3.420281e+06}, {t: 1582330383053, v: 3.420671e+06}, {t: 1582330398053, v: 3.421132e+06}, {t: 1582330413058, v: 3.421673e+06},
						{t: 1582330428053, v: 3.42204e+06}, {t: 1582330443053, v: 3.422405e+06}, {t: 1582330458053, v: 3.422753e+06}, {t: 1582330473053, v: 3.423144e+06}, {t: 1582330488067, v: 3.423663e+06}, {t: 1582330503053, v: 3.424217e+06},
						{t: 1582330518053, v: 3.424743e+06}, {t: 1582330533053, v: 3.425246e+06}, {t: 1582330548053, v: 3.425641e+06}, {t: 1582330563054, v: 3.426142e+06}, {t: 1582330578053, v: 3.426571e+06}, {t: 1582330593053, v: 3.427067e+06},
						{t: 1582330608053, v: 3.427776e+06}, {t: 1582330623053, v: 3.428342e+06}, {t: 1582330638053, v: 3.428903e+06}, {t: 1582330653077, v: 3.429352e+06}, {t: 1582330668053, v: 3.429892e+06}, {t: 1582330683053, v: 3.430238e+06},
						{t: 1582330698053, v: 3.430812e+06}, {t: 1582330713053, v: 3.431257e+06}, {t: 1582330728056, v: 3.431899e+06}, {t: 1582330743053, v: 3.432422e+06}, {t: 1582330758053, v: 3.432863e+06}, {t: 1582330773053, v: 3.433215e+06},
						{t: 1582330788053, v: 3.433822e+06}, {t: 1582330803053, v: 3.434217e+06}, {t: 1582330818053, v: 3.434559e+06}, {t: 1582330833053, v: 3.43516e+06}, {t: 1582330848053, v: 3.435718e+06}, {t: 1582330863053, v: 3.436289e+06},
						{t: 1582330878053, v: 3.436776e+06}, {t: 1582330893053, v: 3.437315e+06}, {t: 1582330908053, v: 3.437971e+06}, {t: 1582330923053, v: 3.438459e+06}, {t: 1582330938053, v: 3.438912e+06}, {t: 1582330953053, v: 3.439307e+06},
						{t: 1582330968053, v: 3.439861e+06}, {t: 1582330983053, v: 3.440302e+06}, {t: 1582330998053, v: 3.440744e+06}, {t: 1582331013053, v: 3.441185e+06}, {t: 1582331028053, v: 3.441574e+06}, {t: 1582331043053, v: 3.442031e+06},
						{t: 1582331058053, v: 3.442613e+06}, {t: 1582331073053, v: 3.443145e+06}, {t: 1582331088053, v: 3.443649e+06}, {t: 1582331103053, v: 3.444181e+06}, {t: 1582331118053, v: 3.444643e+06}, {t: 1582331133053, v: 3.445203e+06},
						{t: 1582331148053, v: 3.445629e+06}, {t: 1582331163053, v: 3.446097e+06}, {t: 1582331178053, v: 3.446645e+06}, {t: 1582331193053, v: 3.447223e+06}, {t: 1582331208054, v: 3.447765e+06}, {t: 1582331223053, v: 3.448241e+06},
						{t: 1582331238053, v: 3.448659e+06}, {t: 1582331253053, v: 3.449075e+06}, {t: 1582331268053, v: 3.449566e+06}, {t: 1582331283053, v: 3.450086e+06}, {t: 1582331298053, v: 3.450673e+06}, {t: 1582331313053, v: 3.451119e+06},
						{t: 1582331328053, v: 3.451611e+06}, {t: 1582331343053, v: 3.452079e+06}, {t: 1582331358053, v: 3.452434e+06}, {t: 1582331373053, v: 3.452943e+06},
					},
				},
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			defer leaktest.CheckTimeout(t, 10*time.Second)()

			q := newQuerier(context.Background(), nil, tcase.mint, tcase.maxt, tcase.replicaLabels, tcase.storeAPI, tcase.dedup, 0, true, false)
			defer func() { testutil.Ok(t, q.Close()) }()

			res, w, err := q.Select(tcase.hints, tcase.matchers...)
			testutil.Ok(t, err)
			if tcase.expectedWarning != "" {
				testutil.Equals(t, 1, len(w))
				testutil.Equals(t, tcase.expectedWarning, w[0].Error())
			}

			i := 0
			for res.Next() {
				testutil.Assert(t, i < len(tcase.expected), "more series than expected")
				testutil.Equals(t, tcase.expected[i].lset, res.At().Labels())

				samples := expandSeries(t, res.At().Iterator())
				testutil.Equals(t, tcase.expected[i].samples, samples, "samples for series %v does not match", i)
				i++
			}
			testutil.Ok(t, res.Err())
			testutil.Equals(t, len(tcase.expected), i)
		})
	}
}

func TestSortReplicaLabel(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	tests := []struct {
		input       []storepb.Series
		exp         []storepb.Series
		dedupLabels map[string]struct{}
	}{
		// 0 Single deduplication label.
		{
			input: []storepb.Series{
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-1"},
					{Name: "c", Value: "3"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-1"},
					{Name: "c", Value: "3"},
					{Name: "d", Value: "4"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-1"},
					{Name: "c", Value: "4"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-2"},
					{Name: "c", Value: "3"},
				}},
			},
			exp: []storepb.Series{
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "b", Value: "replica-1"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "b", Value: "replica-2"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "d", Value: "4"},
					{Name: "b", Value: "replica-1"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "4"},
					{Name: "b", Value: "replica-1"},
				}},
			},
			dedupLabels: map[string]struct{}{"b": {}},
		},
		// 1 Multi deduplication labels.
		{
			input: []storepb.Series{
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-1"},
					{Name: "b1", Value: "replica-1"},
					{Name: "c", Value: "3"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-1"},
					{Name: "b1", Value: "replica-1"},
					{Name: "c", Value: "3"},
					{Name: "d", Value: "4"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-1"},
					{Name: "b1", Value: "replica-1"},
					{Name: "c", Value: "4"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-2"},
					{Name: "b1", Value: "replica-2"},
					{Name: "c", Value: "3"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-2"},
					{Name: "c", Value: "3"},
				}},
			},
			exp: []storepb.Series{
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "b", Value: "replica-1"},
					{Name: "b1", Value: "replica-1"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "b", Value: "replica-2"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "b", Value: "replica-2"},
					{Name: "b1", Value: "replica-2"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "d", Value: "4"},
					{Name: "b", Value: "replica-1"},
					{Name: "b1", Value: "replica-1"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "4"},
					{Name: "b", Value: "replica-1"},
					{Name: "b1", Value: "replica-1"},
				}},
			},
			dedupLabels: map[string]struct{}{
				"b":  {},
				"b1": {},
			},
		},
	}
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			sortDedupLabels(test.input, test.dedupLabels)
			testutil.Equals(t, test.exp, test.input)
		})
	}
}

func expandSeries(t testing.TB, it storage.SeriesIterator) (res []sample) {
	for it.Next() {
		t, v := it.At()
		res = append(res, sample{t, v})
	}
	testutil.Ok(t, it.Err())
	return res
}

func TestDedupSeriesSet(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	tests := []struct {
		input []struct {
			lset []storepb.Label
			vals []sample
		}
		exp []struct {
			lset labels.Labels
			vals []sample
		}
		dedupLabels map[string]struct{}
	}{
		{ // 0 Single dedup label.
			input: []struct {
				lset []storepb.Label
				vals []sample
			}{
				{
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-1"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-2"}},
					vals: []sample{{60000, 3}, {70000, 4}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}},
					vals: []sample{{200000, 5}, {210000, 6}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}, {Name: "replica", Value: "replica-1"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}},
					vals: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []struct {
				lset labels.Labels
				vals []sample
			}{
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}, {200000, 5}, {210000, 6}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
			dedupLabels: map[string]struct{}{
				"replica": {},
			},
		},
		{ // 1 Multi dedup label.
			input: []struct {
				lset []storepb.Label
				vals []sample
			}{
				{
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-1"}, {Name: "replicaA", Value: "replica-1"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-2"}, {Name: "replicaA", Value: "replica-2"}},
					vals: []sample{{60000, 3}, {70000, 4}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}, {Name: "replicaA", Value: "replica-3"}},
					vals: []sample{{200000, 5}, {210000, 6}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}, {Name: "replica", Value: "replica-1"}, {Name: "replicaA", Value: "replica-1"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}, {Name: "replicaA", Value: "replica-3"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}, {Name: "replicaA", Value: "replica-3"}},
					vals: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []struct {
				lset labels.Labels
				vals []sample
			}{
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}, {200000, 5}, {210000, 6}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
			dedupLabels: map[string]struct{}{
				"replica":  {},
				"replicaA": {},
			},
		},
		{ // 2 Multi dedup label - some series don't have all dedup labels.
			input: []struct {
				lset []storepb.Label
				vals []sample
			}{
				{
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-1"}, {Name: "replicaA", Value: "replica-1"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-2"}},
					vals: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []struct {
				lset labels.Labels
				vals []sample
			}{
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
			dedupLabels: map[string]struct{}{
				"replica":  {},
				"replicaA": {},
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			var series []storepb.Series
			for _, c := range test.input {
				chk := chunkenc.NewXORChunk()
				app, _ := chk.Appender()
				for _, s := range c.vals {
					app.Append(s.t, s.v)
				}
				series = append(series, storepb.Series{
					Labels: c.lset,
					Chunks: []storepb.AggrChunk{
						{Raw: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: chk.Bytes()}},
					},
				})
			}
			set := &promSeriesSet{
				mint: 1,
				maxt: math.MaxInt64,
				set:  newStoreSeriesSet(series),
			}
			dedupSet := newDedupSeriesSet(set, test.dedupLabels)

			i := 0
			for dedupSet.Next() {
				testutil.Equals(t, test.exp[i].lset, dedupSet.At().Labels(), "labels mismatch at index:%v", i)
				res := expandSeries(t, dedupSet.At().Iterator())
				testutil.Equals(t, test.exp[i].vals, res, "values mismatch at index:%v", i)
				i++
			}
			testutil.Ok(t, dedupSet.Err())
		})
	}
}

func TestDedupSeriesIterator(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

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
			&SampleIterator{l: c.a, i: -1},
			&SampleIterator{l: c.b, i: -1},
		)
		res := expandSeries(t, it)
		testutil.Equals(t, c.exp, res)
	}
}

func BenchmarkDedupSeriesIterator(b *testing.B) {
	run := func(b *testing.B, s1, s2 []sample) {
		it := newDedupSeriesIterator(
			&SampleIterator{l: s1, i: -1},
			&SampleIterator{l: s2, i: -1},
		)
		b.ResetTimer()
		var total int64

		for it.Next() {
			t, _ := it.At()
			total += t
		}
		fmt.Fprint(ioutil.Discard, total)
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

type sample struct {
	t int64
	v float64
}

type SampleIterator struct {
	l []sample
	i int
}

func (s *SampleIterator) Err() error {
	return nil
}

func (s *SampleIterator) At() (int64, float64) {
	return s.l[s.i].t, s.l[s.i].v
}

func (s *SampleIterator) Next() bool {
	if s.i >= len(s.l) {
		return false
	}
	s.i++
	return true
}

func (s *SampleIterator) Seek(t int64) bool {
	if s.i < 0 {
		s.i = 0
	}
	for {
		if s.i >= len(s.l) {
			return false
		}
		if s.l[s.i].t >= t {
			return true
		}
		s.i++
	}
}

type storeServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.StoreServer

	resps []*storepb.SeriesResponse
}

func (s *storeServer) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	for _, resp := range s.resps {
		err := srv.Send(resp)
		if err != nil {
			return err
		}
	}
	return nil
}

// storeSeriesResponse creates test storepb.SeriesResponse that includes series with single chunk that stores all the given samples.
func storeSeriesResponse(t testing.TB, lset labels.Labels, smplChunks ...[]sample) *storepb.SeriesResponse {
	var s storepb.Series

	for _, l := range lset {
		s.Labels = append(s.Labels, storepb.Label{Name: l.Name, Value: l.Value})
	}

	for _, smpls := range smplChunks {
		c := chunkenc.NewXORChunk()
		a, err := c.Appender()
		testutil.Ok(t, err)

		for _, smpl := range smpls {
			a.Append(smpl.t, smpl.v)
		}

		ch := storepb.AggrChunk{
			MinTime: smpls[0].t,
			MaxTime: smpls[len(smpls)-1].t,
			Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: c.Bytes()},
		}

		s.Chunks = append(s.Chunks, ch)
	}
	return storepb.NewSeriesResponse(&s)
}
