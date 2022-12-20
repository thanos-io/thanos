// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package model_test

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/model"
)

func TestTimeOrDurationValue(t *testing.T) {
	t.Run("positive", func(t *testing.T) {
		cmd := kingpin.New("test", "test")

		minTime := model.TimeOrDuration(cmd.Flag("min-time", "Start of time range limit to serve"))

		maxTime := model.TimeOrDuration(cmd.Flag("max-time", "End of time range limit to serve").
			Default("9999-12-31T23:59:59Z"))

		_, err := cmd.Parse([]string{"--min-time", "10s"})
		if err != nil {
			t.Fatal(err)
		}

		testutil.Equals(t, "10s", minTime.String())
		testutil.Equals(t, "9999-12-31 23:59:59 +0000 UTC", maxTime.String())

		prevTime := timestamp.FromTime(time.Now())
		afterTime := timestamp.FromTime(time.Now().Add(15 * time.Second))

		testutil.Assert(t, minTime.PrometheusTimestamp() > prevTime, "minTime prometheus timestamp is less than time now.")
		testutil.Assert(t, minTime.PrometheusTimestamp() < afterTime, "minTime prometheus timestamp is more than time now + 15s")

		testutil.Assert(t, maxTime.PrometheusTimestamp() == 253402300799000, "maxTime is not equal to 253402300799000")
	})

	t.Run("negative", func(t *testing.T) {
		cmd := kingpin.New("test-negative", "test-negative")
		var minTime model.TimeOrDurationValue
		cmd.Flag("min-time", "Start of time range limit to serve").SetValue(&minTime)
		_, err := cmd.Parse([]string{"--min-time=-10s"})
		if err != nil {
			t.Fatal(err)
		}
		testutil.Equals(t, "-10s", minTime.String())

		prevTime := timestamp.FromTime(time.Now().Add(-15 * time.Second))
		afterTime := timestamp.FromTime(time.Now())
		testutil.Assert(t, minTime.PrometheusTimestamp() > prevTime, "minTime prometheus timestamp is less than time now - 15s.")
		testutil.Assert(t, minTime.PrometheusTimestamp() < afterTime, "minTime prometheus timestamp is more than time now.")
	})
}
