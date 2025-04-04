// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extprom

import (
	"sort"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/protobuf/proto"
)

func TestTxGaugeVec(t *testing.T) {
	g := NewTxGaugeVec(nil, prometheus.GaugeOpts{
		Name: "metric",
	}, []string{"a", "b"}, []string{"a1", "b1"}, []string{"a2", "b2"})

	strPtr := func(s string) *string {
		return &s
	}

	floatPtr := func(f float64) *float64 {
		return &f
	}

	for _, tcase := range []struct {
		name   string
		txUse  func()
		exp    map[string]float64
		expDto []proto.Message
	}{
		{
			name:  "nothing",
			txUse: func() {},
			expDto: []proto.Message{
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a1"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b1"),
						},
					},
				},
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a2"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b2"),
						},
					},
				},
			},
		},
		{
			name: "change a=a1,b=b1",
			txUse: func() {
				g.WithLabelValues("a1", "b1").Inc()
				g.WithLabelValues("a1", "b1").Add(0.3)
			},
			expDto: []proto.Message{
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(1.3),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a1"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b1"),
						},
					},
				},
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a2"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b2"),
						},
					},
				},
			},
		},
		{
			name: "change a=a1,b=b1 again, should return same result",
			txUse: func() {
				g.WithLabelValues("a1", "b1").Inc()
				g.WithLabelValues("a1", "b1").Add(-10)
				g.WithLabelValues("a1", "b1").Add(10.3)
			},
			expDto: []proto.Message{
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(1.3000000000000007),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a1"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b1"),
						},
					},
				},
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a2"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b2"),
						},
					},
				},
			},
		},
		{
			name: "change a=a1,b=b1 again, should return same result",
			txUse: func() {
				g.WithLabelValues("a1", "b1").Inc()
				g.WithLabelValues("a1", "b1").Add(-10)
				g.WithLabelValues("a1", "b1").Set(1.3)
			},
			expDto: []proto.Message{
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(1.3),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a1"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b1"),
						},
					},
				},
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a2"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b2"),
						},
					},
				},
			},
		},
		{
			name:  "nothing again",
			txUse: func() {},
			expDto: []proto.Message{
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a1"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b1"),
						},
					},
				},
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a2"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b2"),
						},
					},
				},
			},
		},
		{
			name: "change a=aX,b=b1",
			txUse: func() {
				g.WithLabelValues("aX", "b1").Set(500.2)
			},
			expDto: []proto.Message{
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a1"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b1"),
						},
					},
				},
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a2"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b2"),
						},
					},
				},
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(500.2),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("aX"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b1"),
						},
					},
				},
			},
		},
		{
			name: "change a=aX,b=b1",
			txUse: func() {
				g.WithLabelValues("aX", "b1").Set(500.2)
			},
			expDto: []proto.Message{
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a1"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b1"),
						},
					},
				},
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(500.2),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("aX"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b1"),
						},
					},
				},
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a2"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b2"),
						},
					},
				},
			},
		},
		{
			name:  "nothing again",
			txUse: func() {},
			expDto: []proto.Message{
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a1"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b1"),
						},
					},
				},
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a2"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b2"),
						},
					},
				},
			},
		},
		{
			name: "change 3 metrics",
			txUse: func() {
				g.WithLabelValues("a1", "b1").Inc()
				g.WithLabelValues("a2", "b2").Add(-2)
				g.WithLabelValues("a3", "b3").Set(1.1)
			},
			expDto: []proto.Message{
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(1),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a1"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b1"),
						},
					},
				},
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(-2),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a2"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b2"),
						},
					},
				},
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(1.1),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a3"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b3"),
						},
					},
				},
			},
		},
		{
			name:  "nothing again",
			txUse: func() {},
			expDto: []proto.Message{
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a1"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b1"),
						},
					},
				},
				&dto.Metric{
					Gauge: &dto.Gauge{
						Value: floatPtr(0),
					},
					Label: []*dto.LabelPair{
						{
							Name:  strPtr("a"),
							Value: strPtr("a2"),
						},
						{
							Name:  strPtr("b"),
							Value: strPtr("b2"),
						},
					},
				},
			},
		},
	} {
		if ok := t.Run(tcase.name, func(t *testing.T) {
			g.ResetTx()

			tcase.txUse()
			g.Submit()

			got := toProtoMessage(t, g)
			testutil.Equals(t, len(tcase.expDto), len(got))

			sortDtoMessages(got)
			sortDtoMessages(tcase.expDto)
			for i := 0; i < len(tcase.expDto); i++ {
				testutil.Equals(t, true, proto.Equal(got[i], tcase.expDto[i]))
			}
		}); !ok {
			return
		}
	}
}

func sortDtoMessages(msgs []proto.Message) {
	sort.Slice(msgs, func(i, j int) bool {
		m1 := msgs[i].(*dto.Metric)
		m2 := msgs[j].(*dto.Metric)

		lbls1 := labels.Labels{}
		for _, p := range m1.GetLabel() {
			lbls1 = append(lbls1, labels.Label{Name: *p.Name, Value: *p.Value})
		}
		lbls2 := labels.Labels{}
		for _, p := range m2.GetLabel() {
			lbls2 = append(lbls2, labels.Label{Name: *p.Name, Value: *p.Value})
		}

		return labels.Compare(lbls1, lbls2) < 0
	})
}

func toProtoMessage(t *testing.T, c prometheus.Collector) []proto.Message {
	var (
		mChan    = make(chan prometheus.Metric)
		messages = make([]proto.Message, 0)
	)

	go func() {
		c.Collect(mChan)
		close(mChan)
	}()

	for m := range mChan {
		pb := &dto.Metric{}
		testutil.Ok(t, m.Write(pb))

		messages = append(messages, pb)
	}

	return messages
}
