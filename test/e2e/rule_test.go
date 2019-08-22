package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

const (
	testAlertRuleAbortOnPartialResponse = `
groups:
- name: example
  # Abort should be a default: partial_response_strategy: "ABORT"
  rules:
  - alert: TestAlert_AbortOnPartialResponse
    # It must be based on actual metrics otherwise call to StoreAPI would be not involved.
    expr: absent(some_metric)
    labels:
      severity: page
    annotations:
      summary: "I always complain, but I don't allow partial response in query."
`
	testAlertRuleWarnOnPartialResponse = `
groups:
- name: example
  partial_response_strategy: "WARN"
  rules:
  - alert: TestAlert_WarnOnPartialResponse
    # It must be based on actual metric, otherwise call to StoreAPI would be not involved.
    expr: absent(some_metric)
    labels:
      severity: page
    annotations:
      summary: "I always complain and allow partial response in query."
`
)

var (
	alertsToTest = []string{testAlertRuleAbortOnPartialResponse, testAlertRuleWarnOnPartialResponse}
)

func TestRule(t *testing.T) {
	a := newLocalAddresser()

	am := alertManager(a.New())
	qAddr := a.New()

	r1 := rule(a.New(), a.New(), alertsToTest, am.HTTP, []address{qAddr}, nil)
	r2 := rule(a.New(), a.New(), alertsToTest, am.HTTP, nil, []address{qAddr})

	q := querier(qAddr, a.New(), []address{r1.GRPC, r2.GRPC}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)

	exit, err := e2eSpinup(t, ctx, q, r1, r2, am)
	if err != nil {
		t.Errorf("spinup failed: %v", err)
		cancel()
		return
	}

	defer func() {
		cancel()
		<-exit
	}()

	expMetrics := []model.Metric{
		{
			"__name__":   "ALERTS",
			"severity":   "page",
			"alertname":  "TestAlert_AbortOnPartialResponse",
			"alertstate": "firing",
			"replica":    model.LabelValue(r1.HTTP.Port),
		},
		{
			"__name__":   "ALERTS",
			"severity":   "page",
			"alertname":  "TestAlert_AbortOnPartialResponse",
			"alertstate": "firing",
			"replica":    model.LabelValue(r2.HTTP.Port),
		},
		{
			"__name__":   "ALERTS",
			"severity":   "page",
			"alertname":  "TestAlert_WarnOnPartialResponse",
			"alertstate": "firing",
			"replica":    model.LabelValue(r1.HTTP.Port),
		},
		{
			"__name__":   "ALERTS",
			"severity":   "page",
			"alertname":  "TestAlert_WarnOnPartialResponse",
			"alertstate": "firing",
			"replica":    model.LabelValue(r2.HTTP.Port),
		},
	}
	expAlertLabels := []model.LabelSet{
		{
			"severity":  "page",
			"alertname": "TestAlert_AbortOnPartialResponse",
			"replica":   model.LabelValue(r1.HTTP.Port),
		},
		{
			"severity":  "page",
			"alertname": "TestAlert_AbortOnPartialResponse",
			"replica":   model.LabelValue(r2.HTTP.Port),
		},
		{
			"severity":  "page",
			"alertname": "TestAlert_WarnOnPartialResponse",
			"replica":   model.LabelValue(r1.HTTP.Port),
		},
		{
			"severity":  "page",
			"alertname": "TestAlert_WarnOnPartialResponse",
			"replica":   model.LabelValue(r2.HTTP.Port),
		},
	}

	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() (err error) {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}

		qtime := time.Now()

		// The time series written for the firing alerting rule must be queryable.
		res, warnings, err := promclient.QueryInstant(ctx, nil, urlParse(t, q.HTTP.URL()), "ALERTS", time.Now(), promclient.QueryOptions{
			Deduplicate: false,
		})
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			// we don't expect warnings.
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		if len(res) != len(expMetrics) {
			return errors.Errorf("unexpected result %v, expected %d", res, len(expMetrics))
		}

		for i, r := range res {
			if !r.Metric.Equal(expMetrics[i]) {
				return errors.Errorf("unexpected metric %s", r.Metric)
			}
			if int64(r.Timestamp) != timestamp.FromTime(qtime) {
				return errors.Errorf("unexpected timestamp %d", r.Timestamp)
			}
			if r.Value != 1 {
				return errors.Errorf("unexpected value %f", r.Value)
			}
		}

		// A notification must be sent to Alertmanager.
		alrts, err := queryAlertmanagerAlerts(ctx, am.HTTP.URL())
		if err != nil {
			return err
		}
		if len(alrts) != len(expAlertLabels) {
			return errors.Errorf("unexpected alerts length %d", len(alrts))
		}
		for i, a := range alrts {
			if !a.Labels.Equal(expAlertLabels[i]) {
				return errors.Errorf("unexpected labels %s", a.Labels)
			}
		}
		return nil
	}))

	// checks counter ensures we are not missing metrics.
	checks := 0
	// Check metrics to make sure we report correct ones that allow handling the AlwaysFiring not being triggered because of query issue.
	testutil.Ok(t, promclient.MetricValues(ctx, nil, urlParse(t, r1.HTTP.URL()), func(lset labels.Labels, val float64) error {
		switch lset.Get("__name__") {
		case "prometheus_rule_group_rules":
			checks++
			if val != 1 {
				return errors.Errorf("expected 1 loaded groups for strategy %s but found %v", lset.Get("strategy"), val)
			}
		}

		return nil
	}))
	testutil.Equals(t, 2, checks)
}

type failingStoreAPI struct{}

func (a *failingStoreAPI) Info(context.Context, *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	return &storepb.InfoResponse{
		MinTime: math.MinInt64,
		MaxTime: math.MaxInt64,
		Labels: []storepb.Label{
			{
				Name:  "magic",
				Value: "store_api",
			},
		},
		LabelSets: []storepb.LabelSet{
			{
				Labels: []storepb.Label{
					{
						Name:  "magic",
						Value: "store_api",
					},
				},
			},
			{
				Labels: []storepb.Label{
					{
						Name:  "magicmarker",
						Value: "store_api",
					},
				},
			},
		},
	}, nil
}

func (a *failingStoreAPI) Series(_ *storepb.SeriesRequest, _ storepb.Store_SeriesServer) error {
	return errors.New("I always fail. No reason. I am just offended StoreAPI. Don't touch me")
}

func (a *failingStoreAPI) LabelNames(context.Context, *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	return &storepb.LabelNamesResponse{}, nil
}

func (a *failingStoreAPI) LabelValues(context.Context, *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	return &storepb.LabelValuesResponse{}, nil
}

// Test Ruler behaviour on different storepb.PartialResponseStrategy when having partial response from single `failingStoreAPI`.
func TestRulePartialResponse(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_rulepartial_response")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	a := newLocalAddresser()
	qAddr := a.New()

	f := fakeStoreAPI(a.New(), &failingStoreAPI{})
	am := alertManager(a.New())
	r := ruleWithDir(a.New(), a.New(), dir, nil, am.HTTP, []address{qAddr}, nil)
	q := querier(qAddr, a.New(), []address{r.GRPC, f.GRPC}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	exit, err := e2eSpinup(t, ctx, am, f, q, r)
	if err != nil {
		t.Errorf("spinup failed: %v", err)
		cancel()
		return
	}

	defer func() {
		cancel()
		<-exit
	}()

	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() (err error) {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}

		// The time series written for the firing alerting rule must be queryable.
		res, warnings, err := promclient.QueryInstant(ctx, nil, urlParse(t, q.HTTP.URL()), "ALERTS", time.Now(), promclient.QueryOptions{
			Deduplicate: false,
		})
		if err != nil {
			return err
		}

		if len(warnings) != 1 {
			// We do expect warnings.
			return errors.Errorf("unexpected number of warnings, expected 1, got %s", warnings)
		}

		// This is tricky as for initial time (1 rule eval, we will have both alerts, as "No store match queries" will be there.
		if len(res) != 0 {
			return errors.Errorf("unexpected result length. expected %v, got %v", 0, res)
		}
		return nil
	}))

	// Add alerts to ruler, we want to add it only when Querier is rdy, otherwise we will get "no store match the query".
	for i, rule := range alertsToTest {
		testutil.Ok(t, ioutil.WriteFile(path.Join(dir, fmt.Sprintf("rules-%d.yaml", i)), []byte(rule), 0666))
	}

	resp, err := http.Post(r.HTTP.URL()+"/-/reload", "", nil)
	testutil.Ok(t, err)
	defer func() { _, _ = ioutil.ReadAll(resp.Body); _ = resp.Body.Close() }()
	testutil.Equals(t, http.StatusOK, resp.StatusCode)

	// We don't expect `AlwaysFiring` as it does NOT allow PartialResponse, so it will trigger `prometheus_rule_evaluation_failures_total` instead.
	expMetrics := []model.Metric{
		{
			"__name__":   "ALERTS",
			"severity":   "page",
			"alertname":  "TestAlert_WarnOnPartialResponse",
			"alertstate": "firing",
			"replica":    model.LabelValue(r.HTTP.Port),
		},
	}
	expAlertLabels := []model.LabelSet{
		{
			"severity":  "page",
			"alertname": "TestAlert_WarnOnPartialResponse",
			"replica":   model.LabelValue(r.HTTP.Port),
		},
	}

	expectedWarning := "receive series from Addr: " + f.GRPC.HostPort() + " LabelSets: [name:\"magic\" value:\"store_api\" ][name:\"magicmarker\" value:\"store_api\" ] Mint: -9223372036854775808 Maxt: 9223372036854775807: rpc error: code = Unknown desc = I always fail. No reason. I am just offended StoreAPI. Don't touch me"

	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() (err error) {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}

		qtime := time.Now()

		// The time series written for the firing alerting rule must be queryable.
		res, warnings, err := promclient.QueryInstant(ctx, nil, urlParse(t, q.HTTP.URL()), "ALERTS", time.Now(), promclient.QueryOptions{
			Deduplicate: false,
		})
		if err != nil {
			return err
		}

		if len(warnings) != 1 {
			// We do expect warnings.
			return errors.Errorf("unexpected number of warnings, expected 1, got %s", warnings)
		}

		if warnings[0] != expectedWarning {
			return errors.Errorf("unexpected warning, expected %s, got %s", expectedWarning, warnings[0])
		}

		// This is tricky as for initial time (1 rule eval, we will have both alerts, as "No store match queries" will be there.
		if len(res) != len(expMetrics) {
			return errors.Errorf("unexpected result length. expected %v, got %v", len(expMetrics), res)
		}

		for i, r := range res {
			if !r.Metric.Equal(expMetrics[i]) {
				return errors.Errorf("unexpected metric %s, expected %s", r.Metric, expMetrics[i])
			}
			if int64(r.Timestamp) != timestamp.FromTime(qtime) {
				return errors.Errorf("unexpected timestamp %d", r.Timestamp)
			}
			if r.Value != 1 {
				return errors.Errorf("unexpected value %f", r.Value)
			}
		}

		// A notification must be sent to Alertmanager.
		alrts, err := queryAlertmanagerAlerts(ctx, am.HTTP.URL())
		if err != nil {
			return err
		}
		if len(alrts) != len(expAlertLabels) {
			return errors.Errorf("unexpected alerts length %d", len(alrts))
		}
		for i, a := range alrts {
			if !a.Labels.Equal(expAlertLabels[i]) {
				return errors.Errorf("unexpected labels %s", a.Labels)
			}
		}
		return nil
	}))

	// checks counter ensures we are not missing metrics.
	checks := 0
	// Check metrics to make sure we report correct ones that allow handling the AlwaysFiring not being triggered because of query issue.
	testutil.Ok(t, promclient.MetricValues(ctx, nil, urlParse(t, r.HTTP.URL()), func(lset labels.Labels, val float64) error {
		switch lset.Get("__name__") {
		case "prometheus_rule_group_rules":
			checks++
			if val != 1 {
				return errors.Errorf("expected 1 loaded groups for strategy %s but found %v", lset.Get("strategy"), val)
			}
		case "prometheus_rule_evaluation_failures_total":
			if lset.Get("strategy") == "abort" {
				checks++
				if val <= 0 {
					return errors.Errorf("expected rule eval failures for abort strategy rule as we have failing storeAPI but found %v", val)
				}
			} else if lset.Get("strategy") == "warn" {
				checks++
				if val > 0 {
					return errors.Errorf("expected no rule eval failures for warm strategy rule but found %v", val)
				}
			}
		case "thanos_rule_evaluation_with_warnings_total":
			if lset.Get("strategy") == "warn" {
				checks++
				if val <= 0 {
					return errors.Errorf("expected rule eval with warnings for warn strategy rule as we have failing storeAPI but found %v", val)
				}
			} else if lset.Get("strategy") == "abort" {
				checks++
				if val > 0 {
					return errors.Errorf("expected rule eval with warnings 0 for abort strategy rule but found %v", val)
				}
			}
		}
		return nil
	}))
	testutil.Equals(t, 6, checks)
}

// TODO(bwplotka): Move to promclient.
func queryAlertmanagerAlerts(ctx context.Context, url string) ([]*model.Alert, error) {
	req, err := http.NewRequest("GET", url+"/api/v1/alerts", nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithLogOnErr(nil, resp.Body, "close body query alertmanager")

	var v struct {
		Data []*model.Alert `json:"data"`
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(body, &v); err != nil {
		return nil, err
	}

	sort.Slice(v.Data, func(i, j int) bool {
		return v.Data[i].Labels.Before(v.Data[j].Labels)
	})
	return v.Data, nil
}
