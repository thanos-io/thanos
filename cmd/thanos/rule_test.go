package main

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
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
