// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package promclient offers helper client function for various API endpoints.

package promclient

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestExternalLabels(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response string
		err      bool
		labels   map[string]string
	}{
		{
			name:     "invalid payload",
			response: `{`,
			err:      true,
		},
		{
			name:     "unknown scrape protocol",
			response: `{"status":"success","data":{"yaml":"global:\n  scrape_interval: 1m\n  scrape_timeout: 10s\n  scrape_protocols:\n  - OpenMetricsText1.0.0\n  - OpenMetricsText0.0.1\n  - PrometheusText1.0.0\n  - PrometheusText0.0.4\n  - UnknownScrapeProto\n  evaluation_interval: 1m\n  external_labels:\n    az: \"1\"\n    region: eu-west\nruntime:\n  gogc: 75\n"}}`,
			labels: map[string]string{
				"region": "eu-west",
				"az":     "1",
			},
		},
		{
			name:     "no external labels",
			response: `{"status":"success","data":{"yaml":"global:\n  scrape_interval: 1m\n  scrape_timeout: 10s\n  scrape_protocols:\n  - OpenMetricsText1.0.0\n  - OpenMetricsText0.0.1\n  - PrometheusText1.0.0\n  - PrometheusText0.0.4\n  - UnknownScrapeProto\n  evaluation_interval: 1m\nruntime:\n  gogc: 75\n"}}`,
			labels:   map[string]string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintln(w, tc.response)
			}))
			defer ts.Close()

			u, err := url.Parse(ts.URL)
			testutil.Ok(t, err)

			ext, err := NewDefaultClient().ExternalLabels(context.Background(), u)
			if tc.err {
				testutil.NotOk(t, err)
				return
			}

			testutil.Ok(t, err)
			testutil.Equals(t, len(tc.labels), ext.Len())
			for k, v := range tc.labels {
				testutil.Equals(t, v, ext.Get(k))
			}
		})
	}
}
