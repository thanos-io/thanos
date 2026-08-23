// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"testing"

	"github.com/efficientgo/core/testutil"

	writev2 "github.com/thanos-io/thanos/pkg/store/storepb/prompb/io/prometheus/write/v2"
)

// TestTranslateV2ToV1SymbolRefs covers label references that do not address a
// valid entry in the request's symbols table. Both the references and the table
// arrive from the remote write client, so a mismatch has to be reported rather
// than indexed blindly.
func TestTranslateV2ToV1SymbolRefs(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  writev2.Request
	}{
		{
			name: "series label ref past end of symbols",
			req: writev2.Request{
				Symbols:    []string{"", "__name__"},
				Timeseries: []writev2.TimeSeries{{LabelsRefs: []uint32{1, 99}}},
			},
		},
		{
			name: "exemplar label ref past end of symbols",
			req: writev2.Request{
				Symbols: []string{"", "__name__", "thanos_v2_translate_test"},
				Timeseries: []writev2.TimeSeries{
					{
						LabelsRefs: []uint32{1, 2},
						Exemplars:  []writev2.Exemplar{{LabelsRefs: []uint32{1, 42}}},
					},
				},
			},
		},
		{
			name: "empty symbols table",
			req: writev2.Request{
				Timeseries: []writev2.TimeSeries{{LabelsRefs: []uint32{0, 1}}},
			},
		},
		{
			name: "odd number of label refs",
			req: writev2.Request{
				Symbols:    []string{"", "__name__"},
				Timeseries: []writev2.TimeSeries{{LabelsRefs: []uint32{1}}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := translateV2ToV1(tc.req)
			testutil.NotOk(t, err)
		})
	}
}

func TestTranslateV2ToV1(t *testing.T) {
	req := writev2.Request{
		Symbols: []string{"", "__name__", "thanos_v2_translate_test", "trace_id", "abc"},
		Timeseries: []writev2.TimeSeries{
			{
				LabelsRefs: []uint32{1, 2},
				Samples:    []writev2.Sample{{Value: 1.5, Timestamp: 10}},
				Exemplars:  []writev2.Exemplar{{LabelsRefs: []uint32{3, 4}, Value: 2.5, Timestamp: 20}},
			},
		},
	}

	out, err := translateV2ToV1(req)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(out.Timeseries))

	ts := out.Timeseries[0]
	testutil.Equals(t, "__name__", ts.Labels[0].Name)
	testutil.Equals(t, "thanos_v2_translate_test", ts.Labels[0].Value)
	testutil.Equals(t, 1, len(ts.Samples))
	testutil.Equals(t, 1.5, ts.Samples[0].Value)
	testutil.Equals(t, 1, len(ts.Exemplars))
	testutil.Equals(t, "trace_id", ts.Exemplars[0].Labels[0].Name)
	testutil.Equals(t, "abc", ts.Exemplars[0].Labels[0].Value)
}
