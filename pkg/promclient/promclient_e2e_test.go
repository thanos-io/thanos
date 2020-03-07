// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package promclient

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestIsWALFileAccessible_e2e(t *testing.T) {
	e2eutil.ForeachPrometheus(t, func(t testing.TB, p *e2eutil.Prometheus) {
		testutil.Ok(t, p.Start())

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error { return IsWALDirAccessible(p.Dir()) }))

		testutil.NotOk(t, IsWALDirAccessible(path.Join(p.Dir(), "/non-existing")))
		testutil.NotOk(t, IsWALDirAccessible(path.Join(p.Dir(), "/../")))
	})
}

func TestExternalLabels_e2e(t *testing.T) {
	e2eutil.ForeachPrometheus(t, func(t testing.TB, p *e2eutil.Prometheus) {
		testutil.Ok(t, p.SetConfig(`
global:
  external_labels:
    region: eu-west
    az: 1
`))

		testutil.Ok(t, p.Start())

		u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
		testutil.Ok(t, err)

		ext, err := ExternalLabels(context.Background(), log.NewNopLogger(), u)
		testutil.Ok(t, err)

		testutil.Equals(t, 2, len(ext))
		testutil.Equals(t, "eu-west", ext.Get("region"))
		testutil.Equals(t, "1", ext.Get("az"))
	})
}

func TestConfiguredFlags_e2e(t *testing.T) {
	e2eutil.ForeachPrometheus(t, func(t testing.TB, p *e2eutil.Prometheus) {
		testutil.Ok(t, p.Start())

		u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
		testutil.Ok(t, err)

		flags, err := ConfiguredFlags(context.Background(), log.NewNopLogger(), u)
		testutil.Ok(t, err)

		testutil.Assert(t, flags.WebEnableAdminAPI, "")
		testutil.Assert(t, !flags.WebEnableLifecycle, "")
		testutil.Equals(t, p.Dir(), flags.TSDBPath)
		testutil.Equals(t, int64(2*time.Hour), int64(flags.TSDBMinTime))
		testutil.Equals(t, int64(4.8*float64(time.Hour)), int64(flags.TSDBMaxTime))
		testutil.Equals(t, int64(2*24*time.Hour), int64(flags.TSDBRetention))
	})
}

func TestSnapshot_e2e(t *testing.T) {
	e2eutil.ForeachPrometheus(t, func(t testing.TB, p *e2eutil.Prometheus) {
		now := time.Now()

		ctx := context.Background()
		// Create artificial block.
		id, err := e2eutil.CreateBlockWithTombstone(
			ctx,
			p.Dir(),
			[]labels.Labels{labels.FromStrings("a", "b")},
			10,
			timestamp.FromTime(now.Add(-6*time.Hour)),
			timestamp.FromTime(now.Add(-4*time.Hour)),
			nil,
			0,
		)
		testutil.Ok(t, err)

		testutil.Ok(t, p.Start())

		u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
		testutil.Ok(t, err)

		// Prometheus since 2.7.0 don't write empty blocks even if it's head block. So it's no matter passing skip_head true or false here
		// Pass skipHead = true to support all prometheus versions and assert that snapshot creates only one file
		// https://github.com/prometheus/tsdb/pull/374.
		dir, err := Snapshot(ctx, log.NewNopLogger(), u, true)
		testutil.Ok(t, err)

		_, err = os.Stat(path.Join(p.Dir(), dir, id.String()))
		testutil.Ok(t, err)

		files, err := ioutil.ReadDir(path.Join(p.Dir(), dir))
		testutil.Ok(t, err)

		for _, f := range files {
			_, err := ulid.Parse(f.Name())
			testutil.Ok(t, err)
		}

		testutil.Equals(t, 1, len(files))
	})
}

func TestRule_UnmarshalScalarResponse(t *testing.T) {
	var (
		scalarJSONResult              = []byte(`[1541196373.677,"1"]`)
		invalidLengthScalarJSONResult = []byte(`[1541196373.677,"1", "nonsense"]`)
		invalidDataScalarJSONResult   = []byte(`["foo","bar"]`)

		vectorResult   model.Vector
		expectedVector = model.Vector{&model.Sample{
			Metric:    model.Metric{},
			Value:     1,
			Timestamp: model.Time(1541196373677)}}
	)
	// Test valid input.
	vectorResult, err := convertScalarJSONToVector(scalarJSONResult)
	testutil.Ok(t, err)
	testutil.Equals(t, vectorResult.String(), expectedVector.String())

	// Test invalid length of scalar data structure.
	_, err = convertScalarJSONToVector(invalidLengthScalarJSONResult)
	testutil.NotOk(t, err)

	// Test invalid format of scalar data.
	_, err = convertScalarJSONToVector(invalidDataScalarJSONResult)
	testutil.NotOk(t, err)
}

func TestName(t *testing.T) {

	func TestPrometheusStore_SeriesLabels_e2e(t *testing.T) {
		t.Helper()

		defer leaktest.CheckTimeout(t, 10*time.Second)()

		p, err := e2eutil.NewPrometheus()
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, p.Stop()) }()

		baseT := timestamp.FromTime(time.Now()) / 1000 * 1000

		a := p.Appender()
		_, err = a.Add(labels.FromStrings("a", "b"), baseT+100, 1)
		testutil.Ok(t, err)
		_, err = a.Add(labels.FromStrings("a", "c", "job", "test"), baseT+200, 2)
		testutil.Ok(t, err)
		_, err = a.Add(labels.FromStrings("a", "d", "job", "test"), baseT+300, 3)
		testutil.Ok(t, err)
		_, err = a.Add(labels.FromStrings("job", "test"), baseT+400, 4)
		testutil.Ok(t, err)
		testutil.Ok(t, a.Commit())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testutil.Ok(t, p.Start())

		u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
		testutil.Ok(t, err)

		promStore, err := NewPrometheusStore(nil, nil, u, component.Sidecar,
			func() labels.Labels { return labels.FromStrings("region", "eu-west") },
			func() (int64, int64) { return math.MinInt64/1000 + 62135596801, math.MaxInt64/1000 - 62135596801 })
		testutil.Ok(t, err)

		for _, tcase := range []struct{
			req *storepb.SeriesRequest
			expected []storepb.Series
		} {
			res, err := promStore.Series(ctx, & []storepb.LabelMatcher{
		{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
		}, baseT, baseT+300)
			testutil.Ok(t, err)

			testutil.Equals(t, len(res), 1)
			testutil.Equals(t, labels.FromMap(res[0]), labels.Labels{{Name: "a", Value: "b"}})
		}
		{
			res, err := promStore.seriesLabels(ctx, []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "foo"},
			}, baseT, baseT+300)
			testutil.Ok(t, err)

			testutil.Equals(t, len(res), 0)
		}
		{
			res, err := promStore.seriesLabels(ctx, []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_NEQ, Name: "a", Value: "b"},
				{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "test"},
			}, baseT, baseT+300)
			testutil.Ok(t, err)

			// Get two metrics {a="c", job="test"} and {a="d", job="test"}.
			testutil.Equals(t, len(res), 2)
			for _, r := range res {
				testutil.Equals(t, labels.FromMap(r).Has("a"), true)
				testutil.Equals(t, labels.FromMap(r).Get("job"), "test")
			}
		}
		{
			res, err := promStore.seriesLabels(ctx, []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "test"},
			}, baseT, baseT+300)
			testutil.Ok(t, err)

			// Get two metrics.
			testutil.Equals(t, len(res), 2)
			for _, r := range res {
				testutil.Equals(t, labels.FromMap(r).Has("a"), true)
				testutil.Equals(t, labels.FromMap(r).Get("job"), "test")
			}
		}
		{
			res, err := promStore.seriesLabels(ctx, []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "test"},
			}, baseT+400, baseT+400)
			testutil.Ok(t, err)

			// In baseT + 400 we can just get one series.
			testutil.Equals(t, len(res), 1)
			testutil.Equals(t, len(res[0]), 1)
			testutil.Equals(t, labels.FromMap(res[0]).Get("job"), "test")
		}
		// This test case is to test when start time and end time is not specified.
		{
			minTime, maxTime := promStore.timestamps()
			res, err := promStore.seriesLabels(ctx, []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "test"},
			}, minTime, maxTime)
			testutil.Ok(t, err)
			testutil.Equals(t, len(res), 3)
			for _, r := range res {
				testutil.Equals(t, labels.FromMap(r).Get("job"), "test")
			}
		} {
			t.Run("", func(t *testing.T) {
				srv := newStoreSeriesServer(ctx)
				err = promStore.Series(tcase.request, srv)
				testutil.Ok(t, err)

				testutil.Equals(t, tcase.expected, srv.SeriesSet)
			})
		}
	}
}
