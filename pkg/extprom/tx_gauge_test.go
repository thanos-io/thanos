// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extprom

import (
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestTxGaugeVec(t *testing.T) {
	g := NewTxGaugeVec(nil, prometheus.GaugeOpts{
		Name: "metric",
	}, []string{"a", "b"}, []string{"a1", "b1"}, []string{"a2", "b2"})

	for _, tcase := range []struct {
		name  string
		txUse func()
		exp   map[string]float64
	}{
		{
			name:  "nothing",
			txUse: func() {},
			exp: map[string]float64{
				"name:\"a\" value:\"a1\" ,name:\"b\" value:\"b1\" ": 0,
				"name:\"a\" value:\"a2\" ,name:\"b\" value:\"b2\" ": 0,
			},
		},
		{
			name: "change a=a1,b=b1",
			txUse: func() {
				g.WithLabelValues("a1", "b1").Inc()
				g.WithLabelValues("a1", "b1").Add(0.3)
			},
			exp: map[string]float64{
				"name:\"a\" value:\"a1\" ,name:\"b\" value:\"b1\" ": 1.3,
				"name:\"a\" value:\"a2\" ,name:\"b\" value:\"b2\" ": 0,
			},
		},
		{
			name: "change a=a1,b=b1 again, should return same result",
			txUse: func() {
				g.WithLabelValues("a1", "b1").Inc()
				g.WithLabelValues("a1", "b1").Add(-10)
				g.WithLabelValues("a1", "b1").Add(10.3)
			},
			exp: map[string]float64{
				"name:\"a\" value:\"a1\" ,name:\"b\" value:\"b1\" ": 1.3000000000000007, // Say hi to float comparisons.
				"name:\"a\" value:\"a2\" ,name:\"b\" value:\"b2\" ": 0,
			},
		},
		{
			name: "change a=a1,b=b1 again, should return same result",
			txUse: func() {
				g.WithLabelValues("a1", "b1").Inc()
				g.WithLabelValues("a1", "b1").Add(-10)
				g.WithLabelValues("a1", "b1").Set(1.3)
			},
			exp: map[string]float64{
				"name:\"a\" value:\"a1\" ,name:\"b\" value:\"b1\" ": 1.3,
				"name:\"a\" value:\"a2\" ,name:\"b\" value:\"b2\" ": 0,
			},
		},
		{
			name:  "nothing again",
			txUse: func() {},
			exp: map[string]float64{
				"name:\"a\" value:\"a1\" ,name:\"b\" value:\"b1\" ": 0,
				"name:\"a\" value:\"a2\" ,name:\"b\" value:\"b2\" ": 0,
			},
		},
		{
			name: "change a=aX,b=b1",
			txUse: func() {
				g.WithLabelValues("aX", "b1").Set(500.2)
			},
			exp: map[string]float64{
				"name:\"a\" value:\"a1\" ,name:\"b\" value:\"b1\" ": 0,
				"name:\"a\" value:\"a2\" ,name:\"b\" value:\"b2\" ": 0,
				"name:\"a\" value:\"aX\" ,name:\"b\" value:\"b1\" ": 500.2,
			},
		},
		{
			name: "change a=aX,b=b1",
			txUse: func() {
				g.WithLabelValues("aX", "b1").Set(500.2)
			},
			exp: map[string]float64{
				"name:\"a\" value:\"a1\" ,name:\"b\" value:\"b1\" ": 0,
				"name:\"a\" value:\"a2\" ,name:\"b\" value:\"b2\" ": 0,
				"name:\"a\" value:\"aX\" ,name:\"b\" value:\"b1\" ": 500.2,
			},
		},
		{
			name:  "nothing again",
			txUse: func() {},
			exp: map[string]float64{
				"name:\"a\" value:\"a1\" ,name:\"b\" value:\"b1\" ": 0,
				"name:\"a\" value:\"a2\" ,name:\"b\" value:\"b2\" ": 0,
			},
		},
		{
			name: "change 3 metrics",
			txUse: func() {
				g.WithLabelValues("a1", "b1").Inc()
				g.WithLabelValues("a2", "b2").Add(-2)
				g.WithLabelValues("a3", "b3").Set(1.1)
			},
			exp: map[string]float64{
				"name:\"a\" value:\"a1\" ,name:\"b\" value:\"b1\" ": 1,
				"name:\"a\" value:\"a2\" ,name:\"b\" value:\"b2\" ": -2,
				"name:\"a\" value:\"a3\" ,name:\"b\" value:\"b3\" ": 1.1,
			},
		},
		{
			name:  "nothing again",
			txUse: func() {},
			exp: map[string]float64{
				"name:\"a\" value:\"a1\" ,name:\"b\" value:\"b1\" ": 0,
				"name:\"a\" value:\"a2\" ,name:\"b\" value:\"b2\" ": 0,
			},
		},
	} {
		if ok := t.Run(tcase.name, func(t *testing.T) {
			g.ResetTx()

			tcase.txUse()
			g.Submit()

			testutil.Equals(t, tcase.exp, toFloat64(t, g))

		}); !ok {
			return
		}
	}
}

// toFloat64 is prometheus/client_golang/prometheus/testutil.ToFloat64 version that works with multiple labelnames.
// NOTE: Be careful on float comparison.
func toFloat64(t *testing.T, c prometheus.Collector) map[string]float64 {
	var (
		mChan = make(chan prometheus.Metric)
		exp   = map[string]float64{}
	)

	go func() {
		c.Collect(mChan)
		close(mChan)
	}()

	for m := range mChan {
		pb := &dto.Metric{}
		testutil.Ok(t, m.Write(pb))
		if pb.Gauge != nil {
			exp[lbToString(pb.GetLabel())] = pb.Gauge.GetValue()
			continue
		}
		if pb.Counter != nil {
			exp[lbToString(pb.GetLabel())] = pb.Counter.GetValue()
			continue
		}
		if pb.Untyped != nil {
			exp[lbToString(pb.GetLabel())] = pb.Untyped.GetValue()
		}
		panic(errors.Errorf("collected a non-gauge/counter/untyped metric: %s", pb))
	}

	return exp
}

func lbToString(pairs []*dto.LabelPair) string {
	var ret []string
	for _, r := range pairs {
		ret = append(ret, r.String())
	}
	return strings.Join(ret, ",")
}
