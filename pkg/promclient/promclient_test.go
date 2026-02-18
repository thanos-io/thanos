// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package promclient offers helper client function for various API endpoints.

package promclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/common/model"
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

func TestFlags_UnmarshalJSON(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		flags                map[string]string
		expectedDelayCompact string
	}{
		{
			name: "upstream flag only",
			flags: map[string]string{
				"storage.tsdb.path":                    "/data",
				"storage.tsdb.retention":               "2d",
				"storage.tsdb.min-block-duration":      "2h",
				"storage.tsdb.max-block-duration":      "2h",
				"storage.tsdb.delay-compact-file.path": "thanos.shipper.json",
				"web.enable-admin-api":                 "true",
				"web.enable-lifecycle":                 "true",
			},
			expectedDelayCompact: "thanos.shipper.json",
		},
		{
			name: "CF compat flag only",
			flags: map[string]string{
				"storage.tsdb.path":                          "/data",
				"storage.tsdb.retention":                     "2d",
				"storage.tsdb.min-block-duration":            "2h",
				"storage.tsdb.max-block-duration":            "2h",
				"storage.tsdb.thanos-shipper-meta-file-name": "thanos.shipper.json",
				"web.enable-admin-api":                       "true",
				"web.enable-lifecycle":                       "true",
			},
			expectedDelayCompact: "thanos.shipper.json",
		},
		{
			name: "both present, upstream wins",
			flags: map[string]string{
				"storage.tsdb.path":                          "/data",
				"storage.tsdb.retention":                     "2d",
				"storage.tsdb.min-block-duration":            "2h",
				"storage.tsdb.max-block-duration":            "2h",
				"storage.tsdb.delay-compact-file.path":       "upstream.json",
				"storage.tsdb.thanos-shipper-meta-file-name": "cf-compat.json",
				"web.enable-admin-api":                       "true",
				"web.enable-lifecycle":                       "true",
			},
			expectedDelayCompact: "upstream.json",
		},
		{
			name: "neither present",
			flags: map[string]string{
				"storage.tsdb.path":               "/data",
				"storage.tsdb.retention":          "2d",
				"storage.tsdb.min-block-duration": "2h",
				"storage.tsdb.max-block-duration": "2h",
				"web.enable-admin-api":            "true",
				"web.enable-lifecycle":            "true",
			},
			expectedDelayCompact: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.flags)
			testutil.Ok(t, err)

			var flags Flags
			err = json.Unmarshal(b, &flags)
			testutil.Ok(t, err)

			testutil.Equals(t, tc.expectedDelayCompact, flags.TSDBDelayCompact)
			testutil.Equals(t, "/data", flags.TSDBPath)
			testutil.Equals(t, model.Duration(2*time.Hour), flags.TSDBMinTime)
			testutil.Equals(t, model.Duration(2*time.Hour), flags.TSDBMaxTime)
			testutil.Equals(t, true, flags.WebEnableAdminAPI)
			testutil.Equals(t, true, flags.WebEnableLifecycle)
		})
	}
}
