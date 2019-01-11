package promclient

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"testing"
	"time"

	"github.com/prometheus/common/model"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestIsWALFileAccesible_e2e(t *testing.T) {
	p, err := testutil.NewPrometheus()
	testutil.Ok(t, err)

	testutil.Ok(t, p.Start())
	defer func() { testutil.Ok(t, p.Stop()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error { return IsWALDirAccesible(p.Dir()) }))

	testutil.NotOk(t, IsWALDirAccesible(path.Join(p.Dir(), "/non-existing")))
	testutil.NotOk(t, IsWALDirAccesible(path.Join(p.Dir(), "/../")))
}

func TestExternalLabels_e2e(t *testing.T) {
	p, err := testutil.NewPrometheus()
	testutil.Ok(t, err)

	err = p.SetConfig(`
global:
  external_labels:
    region: eu-west
    az: 1
`)
	testutil.Ok(t, err)

	testutil.Ok(t, p.Start())
	defer func() { testutil.Ok(t, p.Stop()) }()

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	ext, err := ExternalLabels(context.Background(), log.NewNopLogger(), u)
	testutil.Ok(t, err)

	testutil.Equals(t, 2, len(ext))
	testutil.Equals(t, "eu-west", ext.Get("region"))
	testutil.Equals(t, "1", ext.Get("az"))
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
