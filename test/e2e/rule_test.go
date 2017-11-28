package e2e_test

import (
	"context"
	"io/ioutil"
	"os"
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
	defer os.RemoveAll(dir)

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

	closeFn := spinup(t, config{
		workDir:    dir,
		numQueries: 1,
		numRules:   2,
		rules:      alwaysFireRule,
	})
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

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
	err = runutil.Retry(time.Second, ctx.Done(), func() error {
		qtime := time.Now()

		res, err := queryPrometheus(ctx, "http://localhost:19491", time.Now(), "ALERTS")
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
		return nil
	})
	testutil.Ok(t, err)
}
