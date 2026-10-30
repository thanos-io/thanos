// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/client_golang/prometheus"
)

func TestRequestDurationBuckets(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		tail    []float64
	}{
		{name: "default", timeout: 5 * time.Second, tail: []float64{10, 20, 30}},
		{name: "short", timeout: time.Second, tail: []float64{10, 20, 30}},
		{name: "custom boundary", timeout: 8 * time.Second, tail: []float64{7, 8, 10, 20, 30}},
		{name: "long", timeout: 35 * time.Second, tail: []float64{7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			h := NewHandler(nil, &Options{Registry: reg, ForwardTimeout: tc.timeout})
			t.Cleanup(h.Close)
			h.router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/api/v1/receive", nil))
			families, err := reg.Gather()
			testutil.Ok(t, err)
			for _, family := range families {
				if family.GetName() != "http_request_duration_seconds" {
					continue
				}
				testutil.Equals(t, 1, len(family.Metric))
				var bounds []float64
				for _, bucket := range family.Metric[0].GetHistogram().GetBucket() {
					bounds = append(bounds, bucket.GetUpperBound())
				}
				want := []float64{0.001, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.25, 0.5, 0.75, 1, 2, 3, 4, 5}
				testutil.Equals(t, append(want, tc.tail...), bounds)
				return
			}
			t.Fatal("receive request duration histogram missing")
		})
	}
}
