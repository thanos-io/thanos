// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package replicate

import (
	"fmt"
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/efficientgo/core/testutil"
	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
)

func TestReplicationRunDurationBuckets(t *testing.T) {
	app := kingpin.New("replicate", "")
	from := extflag.RegisterPathOrContent(app, "from", "source bucket")
	to := extflag.RegisterPathOrContent(app, "to", "destination bucket")
	_, err := app.Parse([]string{
		"--from", fmt.Sprintf("type: FILESYSTEM\nconfig:\n  directory: %s\n", t.TempDir()),
		"--to", fmt.Sprintf("type: FILESYSTEM\nconfig:\n  directory: %s\n", t.TempDir()),
	})
	testutil.Ok(t, err)

	reg := prometheus.NewRegistry()
	var g run.Group
	testutil.Ok(t, RunReplicate(&g, log.NewNopLogger(), reg, nil, "127.0.0.1:0", "", 0,
		nil, nil, nil, from, to, true, &minTimeDuration, &maxTimeDuration, nil, false))
	testutil.Ok(t, g.Run())
	families, err := reg.Gather()
	testutil.Ok(t, err)
	for _, family := range families {
		if family.GetName() != "thanos_replicate_replication_run_duration_seconds" {
			continue
		}
		testutil.Equals(t, 2, len(family.Metric))
		for _, metric := range family.Metric {
			var bounds []float64
			for _, bucket := range metric.GetHistogram().GetBucket() {
				bounds = append(bounds, bucket.GetUpperBound())
			}
			// Preserve all existing boundaries and measure beyond the 20s alert threshold.
			testutil.Equals(t, []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60}, bounds)
		}
		return
	}
	t.Fatal("replication run duration histogram missing")
}
