// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/labels"

	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type stubAlerts struct {
	alerts []*rulespb.AlertInstance
}

func (s stubAlerts) Active() []*rulespb.AlertInstance {
	return s.alerts
}

func newAlertsServer(alerts []*rulespb.AlertInstance) *httptest.Server {
	logger := log.NewNopLogger()
	rapi := NewRuleAPI(logger, nil, nil, stubAlerts{alerts: alerts}, true, nil)
	router := route.New()
	rapi.Register(
		router,
		&opentracing.NoopTracer{},
		logger,
		extpromhttp.NewNopInstrumentationMiddleware(),
		logging.NewHTTPServerMiddleware(logger),
	)
	return httptest.NewServer(router)
}

func TestAlertsEndpointMatchesPrometheusAPI(t *testing.T) {
	for _, tc := range []struct {
		name   string
		alerts []*rulespb.AlertInstance
	}{
		{
			name: "no alerts",
		},
		{
			name: "firing alert",
			alerts: []*rulespb.AlertInstance{
				{
					Labels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(
						labels.FromStrings("alertname", "Watchdog", "severity", "none"),
					)},
					Annotations:             labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.EmptyLabels())},
					State:                   rulespb.AlertState_FIRING,
					Value:                   "1e+00",
					PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newAlertsServer(tc.alerts)
			t.Cleanup(srv.Close)

			resp, err := http.Get(srv.URL + "/alerts")
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status %d body %s", resp.StatusCode, body)
			}

			var raw map[string]json.RawMessage
			if err := json.Unmarshal(body, &raw); err != nil {
				t.Fatalf("unmarshal response: %v body %s", err, body)
			}
			var data map[string]json.RawMessage
			if err := json.Unmarshal(raw["data"], &data); err != nil {
				t.Fatalf("unmarshal data: %v body %s", err, body)
			}
			if _, ok := data["Alerts"]; ok {
				t.Fatalf("response used Alerts instead of alerts: %s", body)
			}
			alertsRaw, ok := data["alerts"]
			if !ok {
				t.Fatalf("response missing alerts: %s", body)
			}
			if len(tc.alerts) == 0 && string(alertsRaw) != "[]" {
				t.Fatalf("alerts = %s, want []", alertsRaw)
			}

			var alerts []struct {
				Labels map[string]string `json:"labels"`
				State  string            `json:"state"`
				Value  string            `json:"value"`
			}
			if err := json.Unmarshal(alertsRaw, &alerts); err != nil {
				t.Fatalf("alerts is not an array: %v body %s", err, body)
			}
			if len(alerts) != len(tc.alerts) {
				t.Fatalf("got %d alerts, want %d: %s", len(alerts), len(tc.alerts), body)
			}
			if len(alerts) == 1 {
				if alerts[0].Labels["alertname"] != "Watchdog" || alerts[0].State != "firing" || alerts[0].Value != "1e+00" {
					t.Fatalf("unexpected alert: %s", body)
				}
			}
		})
	}
}
