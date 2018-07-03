package e2e_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/timestamp"
)

// TestRuleComponent tests the basic interaction between the rule component
// and the querying layer.
// Rules are evaluated against the query layer and the query layer in return
// can access data written by the rules.
func TestRuleComponent(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_rule")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	const alwaysFireRule = `
groups:
- name: example
  rules:
  - alert: AlwaysFiring
    expr: vector(1)
    labels:
      severity: page
    annotations:
      summary: "I always complain"
`

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)

	exit, err := spinup(t, ctx, config{
		workDir:          dir,
		numQueries:       1,
		numRules:         2,
		numAlertmanagers: 1,
		rules:            alwaysFireRule,
	})
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
			"alertname":  "AlwaysFiring",
			"alertstate": "firing",
			"replica":    "1",
		},
		{
			"__name__":   "ALERTS",
			"severity":   "page",
			"alertname":  "AlwaysFiring",
			"alertstate": "firing",
			"replica":    "2",
		},
	}
	expAlertLabels := []model.LabelSet{
		{
			"severity":  "page",
			"alertname": "AlwaysFiring",
			"replica":   "1",
		},
		{
			"severity":  "page",
			"alertname": "AlwaysFiring",
			"replica":   "2",
		},
	}
	err = runutil.Retry(5*time.Second, ctx.Done(), func() error {
		select {
		case err := <-exit:
			t.Errorf("Some process exited unexpectedly: %v", err)
			return nil
		default:
		}

		qtime := time.Now()

		// The time series written for the firing alerting rule must be queryable.
		res, err := queryPrometheus(ctx, "http://"+queryHTTP(1), time.Now(), "ALERTS", false)
		if err != nil {
			return err
		}
		if len(res) != 2 {
			return errors.Errorf("unexpected result length %d", len(res))
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
		alrts, err := queryAlertmanagerAlerts(ctx, "http://localhost:29093")
		if err != nil {
			return err
		}
		if len(alrts) != 2 {
			return errors.Errorf("unexpected alerts length %d", len(alrts))
		}
		for i, a := range alrts {
			if !a.Labels.Equal(expAlertLabels[i]) {
				return errors.Errorf("unexpected labels %s", a.Labels)
			}
		}
		return nil
	})
	testutil.Ok(t, err)
}

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
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return nil, err
	}
	sort.Slice(v.Data, func(i, j int) bool {
		return v.Data[i].Labels.Before(v.Data[j].Labels)
	})
	return v.Data, nil
}
