package main

import (
	"context"
	"net/url"
	"testing"

	"github.com/improbable-eng/thanos/pkg/discovery/dns"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

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

func TestRule_AlertmanagerResolveWithoutPort(t *testing.T) {
	mockResolver := mockResolver{
		resultIPs: map[string][]string{
			"alertmanager.com:9093": []string{"1.1.1.1:9300"},
		},
	}
	am := alertmanagerSet{resolver: mockResolver, addrs: []string{"dns+http://alertmanager.com"}}

	ctx := context.TODO()
	err := am.update(ctx)
	testutil.Ok(t, err)

	expected := []*url.URL{
		{
			Scheme: "http",
			Host:   "1.1.1.1:9300",
		},
	}
	gotURLs := am.get()
	testutil.Equals(t, expected, gotURLs)
}

func TestRule_AlertmanagerResolveWithPort(t *testing.T) {
	mockResolver := mockResolver{
		resultIPs: map[string][]string{
			"alertmanager.com:19093": []string{"1.1.1.1:9300"},
		},
	}
	am := alertmanagerSet{resolver: mockResolver, addrs: []string{"dns+http://alertmanager.com:19093"}}

	ctx := context.TODO()
	err := am.update(ctx)
	testutil.Ok(t, err)

	expected := []*url.URL{
		{
			Scheme: "http",
			Host:   "1.1.1.1:9300",
		},
	}
	gotURLs := am.get()
	testutil.Equals(t, expected, gotURLs)
}

type mockResolver struct {
	resultIPs map[string][]string
	err       error
}

func (m mockResolver) Resolve(ctx context.Context, name string, qtype dns.QType) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	if res, ok := m.resultIPs[name]; ok {
		return res, nil
	}
	return nil, errors.Errorf("mockResolver not found response for name: %s", name)
}
