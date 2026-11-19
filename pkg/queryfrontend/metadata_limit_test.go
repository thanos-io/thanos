// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	cortexcache "github.com/thanos-io/thanos/internal/cortex/chunk/cache"
	"github.com/thanos-io/thanos/internal/cortex/frontend/transport"
	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
)

func TestMetadataLimitRoundTrip(t *testing.T) {
	for _, path := range []string{"/api/v1/labels", "/api/v1/label/__name__/values", "/api/v1/series"} {
		for _, split := range []time.Duration{0, time.Hour} {
			for _, cached := range []bool{false, true} {
				if cached && split == 0 {
					continue
				}
				t.Run(fmt.Sprintf("%s/split=%s/cache=%t", path, split, cached), func(t *testing.T) {
					cfg := Config{LabelsConfig: LabelsConfig{Limits: defaultLimits, SplitQueriesByInterval: split}, CortexHandlerConfig: &transport.HandlerConfig{}}
					if cached {
						cfg.LabelsConfig.ResultsCacheConfig = &queryrange.ResultsCacheConfig{CacheConfig: cortexcache.Config{Cache: cortexcache.NewMockCache()}}
					}
					tpw, err := NewTripperware(cfg, nil, log.NewNopLogger())
					testutil.Ok(t, err)
					var calls atomic.Int32
					expectedLimit := 0
					backend := newFakeRoundTripper()
					backend.setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						testutil.Ok(t, r.ParseForm())
						// Distinct results per split exercise truncation after merging.
						values := []string{r.FormValue("start") + "a", r.FormValue("start") + "b"}
						limit, _ := strconv.Atoi(r.FormValue("limit"))
						if expectedLimit != limit {
							t.Errorf("downstream limit: got %d, want %d", limit, expectedLimit)
						}
						warnings := []string{"backend warning"}
						if limit > 0 && len(values) > limit {
							values = values[:limit]
							warnings = append(warnings, "results truncated due to limit")
						}
						var data any = values
						if path == "/api/v1/series" {
							series := []map[string]string{}
							for _, v := range values {
								series = append(series, map[string]string{"__name__": v})
							}
							data = series
						}
						testutil.Ok(t, json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": data, "warnings": warnings}))
					}))
					rt := tpw(backend)
					// Warm with a limited request, then an unlimited request, then repeat
					// both. Limited results must never contaminate unlimited cache entries.
					for _, method := range []string{http.MethodGet, http.MethodPost} {
						for _, limit := range []string{"1", "0", "2", "1", "0", "", "-1", "bogus", "999999999999999999999999999999999"} {
							params := url.Values{"start": {"0"}, "end": {"7200"}, "match[]": {"up"}, "limit": {limit}}
							var req *http.Request
							if method == http.MethodGet {
								req = httptest.NewRequest(method, path+"?"+params.Encode(), nil)
							} else {
								req = httptest.NewRequest(method, path, strings.NewReader(params.Encode()))
								req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
							}
							req = req.WithContext(user.InjectOrgID(context.Background(), "tenant"))
							before := calls.Load()
							expectedLimit, _ = strconv.Atoi(limit)
							res, err := rt.RoundTrip(req)
							if limit == "-1" || limit == "bogus" || len(limit) > 20 {
								if err == nil {
									res.Body.Close()
									t.Fatalf("limit %q should be rejected", limit)
								}
								response, ok := httpgrpc.HTTPResponseFromError(err)
								testutil.Assert(t, ok)
								testutil.Equals(t, int32(http.StatusBadRequest), response.Code)
								testutil.Equals(t, before, calls.Load())
								continue
							}
							testutil.Ok(t, err)
							var got struct {
								Data     []json.RawMessage `json:"data"`
								Warnings []string          `json:"warnings"`
							}
							testutil.Ok(t, json.NewDecoder(res.Body).Decode(&got))
							testutil.Ok(t, res.Body.Close())
							n, _ := strconv.Atoi(limit)
							if n > 0 {
								testutil.Assert(t, calls.Load() > before)
								testutil.Equals(t, n, len(got.Data))
								testutil.Equals(t, n == 1 || split > 0 || cached, slices.Contains(got.Warnings, "results truncated due to limit"))
							} else {
								if cached && before > 0 && method == http.MethodPost {
									testutil.Equals(t, before, calls.Load())
								}
								expected := 2
								if split > 0 || cached {
									expected = 4
								}
								testutil.Equals(t, expected, len(got.Data))
							}
							testutil.Assert(t, slices.Contains(got.Warnings, "backend warning"))
						}
					}
				})
			}
		}
	}
}

func TestMetadataLimitMerge(t *testing.T) {
	for _, series := range []bool{false, true} {
		for _, limit := range []int{0, 1, 2, 3, 4} {
			t.Run(fmt.Sprintf("series=%t/limit=%d", series, limit), func(t *testing.T) {
				var req queryrange.Request = &ThanosLabelsRequest{Limit: limit}
				responses := []queryrange.Response{}
				for _, values := range [][]string{{"a", "b"}, {"a", "c"}} {
					var data any = values
					if series {
						req = &ThanosSeriesRequest{Limit: limit}
						data = []map[string]string{{"__name__": values[0]}, {"__name__": values[1]}}
					}
					raw, err := json.Marshal(map[string]any{"status": "success", "data": data, "warnings": []string{"backend warning"}})
					testutil.Ok(t, err)
					var res queryrange.Response = &ThanosLabelsResponse{}
					if series {
						res = &ThanosSeriesResponse{}
					}
					testutil.Ok(t, json.Unmarshal(raw, res))
					responses = append(responses, res)
				}
				original, err := json.Marshal(responses)
				testutil.Ok(t, err)
				merged, err := NewThanosLabelsCodec(true, 0).MergeResponse(req, responses...)
				testutil.Ok(t, err)
				var values []string
				var warnings []string
				switch res := merged.(type) {
				case *ThanosLabelsResponse:
					values = res.Data
					warnings = res.Warnings
				case *ThanosSeriesResponse:
					for _, s := range res.Data {
						values = append(values, s.PromLabels().Get("__name__"))
					}
					warnings = res.Warnings
				}
				want := []string{"a", "b", "c"}
				if limit > 0 && limit < len(want) {
					want = want[:limit]
				}
				testutil.Equals(t, want, values)
				expectedWarnings := []string{"backend warning"}
				if limit > 0 && limit < 3 {
					expectedWarnings = append(expectedWarnings, metadataLimitWarning)
				}
				testutil.Equals(t, expectedWarnings, warnings)
				after, err := json.Marshal(responses)
				testutil.Ok(t, err)
				testutil.Equals(t, string(original), string(after))
			})
		}
	}
}
