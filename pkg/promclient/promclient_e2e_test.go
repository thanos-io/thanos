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
	version "github.com/hashicorp/go-version"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/labels"
)

func TestIsWALFileAccesible_e2e(t *testing.T) {
	testutil.ForeachPrometheus(t, func(t testing.TB, p *testutil.Prometheus) {
		testutil.Ok(t, p.Start())
		defer func() { testutil.Ok(t, p.Stop()) }()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error { return IsWALDirAccesible(p.Dir()) }))

		testutil.NotOk(t, IsWALDirAccesible(path.Join(p.Dir(), "/non-existing")))
		testutil.NotOk(t, IsWALDirAccesible(path.Join(p.Dir(), "/../")))
	})
}

func TestExternalLabels_e2e(t *testing.T) {
	testutil.ForeachPrometheus(t, func(t testing.TB, p *testutil.Prometheus) {
		testutil.Ok(t, p.SetConfig(`
global:
  external_labels:
    region: eu-west
    az: 1
`))

		testutil.Ok(t, p.Start())
		defer func() { testutil.Ok(t, p.Stop()) }()

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
	testutil.ForeachPrometheus(t, func(t testing.TB, p *testutil.Prometheus) {
		testutil.Ok(t, p.Start())
		defer func() { testutil.Ok(t, p.Stop()) }()

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
	testutil.ForeachPrometheus(t, func(t testing.TB, p *testutil.Prometheus) {
		now := time.Now()

		ctx := context.Background()
		// Create artificial block.
		id, err := testutil.CreateBlockWithTombstone(
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
		defer func() { testutil.Ok(t, p.Stop()) }()

		u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
		testutil.Ok(t, err)

		// Prometheus since 2.7.0 don't write empty blocks even if it's head block. So it's no matter passing skip_head true or false here
		// Pass skipHead = true to support all prometheus versions and assert that snapshot creates only one file
		// https://github.com/prometheus/tsdb/pull/374
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
		invalidLengthScalarJSONResult = []byte(`[1541196373.677,"1", "nonsence"]`)
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
	vectorResult, err = convertScalarJSONToVector(invalidLengthScalarJSONResult)
	testutil.NotOk(t, err)

	// Test invalid format of scalar data.
	vectorResult, err = convertScalarJSONToVector(invalidDataScalarJSONResult)
	testutil.NotOk(t, err)
}

func TestParseVersion(t *testing.T) {
	promVersions := map[string]string{
		"":           promVersionResp(""),
		"2.2.0":      promVersionResp("2.2.0"),
		"2.3.0":      promVersionResp("2.3.0"),
		"2.3.0-rc.0": promVersionResp("2.3.0-rc.0"),
	}

	promMalformedVersions := map[string]string{
		"foo": promVersionResp("foo"),
		"bar": promVersionResp("bar"),
	}

	for v, resp := range promVersions {
		gotVersion, err := parseVersion([]byte(resp))
		testutil.Ok(t, err)
		expectVersion, _ := version.NewVersion(v)
		testutil.Equals(t, gotVersion, expectVersion)
	}

	for v, resp := range promMalformedVersions {
		gotVersion, err := parseVersion([]byte(resp))
		testutil.NotOk(t, err)
		expectVersion, _ := version.NewVersion(v)
		testutil.Equals(t, gotVersion, expectVersion)
	}
}

// promVersionResp returns the response of Prometheus /version endpoint.
func promVersionResp(ver string) string {
	return fmt.Sprintf(`{"version":"%s","revision":"","branch":"","buildUser":"","buildDate":"","goVersion":""}`, ver)
}
