// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package labelpb

import (
	"fmt"
	ioutil "io/ioutil"
	"reflect"
	"sort"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/testutil"
)

var testLsetMap = map[string]string{
	"a":                           "1",
	"c":                           "2",
	"d":                           "dsfsdfsdfsdf123414234",
	"124134235423534534ffdasdfsf": "1",
	"":                            "",
	"b":                           "",
}

func TestLabelsToPromLabels_LabelsToPromLabels(t *testing.T) {
	testutil.Equals(t, labels.FromMap(testLsetMap), ZLabelsToPromLabels(ZLabelsFromPromLabels(labels.FromMap(testLsetMap))))

	lset := labels.FromMap(testLsetMap)
	for i := range ZLabelsFromPromLabels(lset) {
		if lset[i].Name != "a" {
			continue
		}
		lset[i].Value += "yolo"

	}
	m := lset.Map()
	testutil.Equals(t, "1yolo", m["a"])

	m["a"] = "1"
	testutil.Equals(t, testLsetMap, m)
}

func TestLabelMarshal_Unmarshal(t *testing.T) {
	l := ZLabelsFromPromLabels(labels.FromStrings("aaaaaaa", "bbbbb"))[0]
	b, err := (&l).Marshal()
	testutil.Ok(t, err)

	l2 := &ZLabel{}
	testutil.Ok(t, l2.Unmarshal(b))
	testutil.Equals(t, labels.FromStrings("aaaaaaa", "bbbbb"), ZLabelsToPromLabels([]ZLabel{*l2}))
}

func TestExtendLabels(t *testing.T) {
	testutil.Equals(t, labels.Labels{{Name: "a", Value: "1"}, {Name: "replica", Value: "01"}, {Name: "xb", Value: "2"}},
		ExtendSortedLabels(labels.Labels{{Name: "a", Value: "1"}, {Name: "xb", Value: "2"}}, labels.FromStrings("replica", "01")))

	testutil.Equals(t, labels.Labels{{Name: "replica", Value: "01"}},
		ExtendSortedLabels(labels.Labels{}, labels.FromStrings("replica", "01")))

	testutil.Equals(t, labels.Labels{{Name: "a", Value: "1"}, {Name: "replica", Value: "01"}, {Name: "xb", Value: "2"}},
		ExtendSortedLabels(labels.Labels{{Name: "a", Value: "1"}, {Name: "replica", Value: "NOT01"}, {Name: "xb", Value: "2"}}, labels.FromStrings("replica", "01")))

	testInjectExtLabels(testutil.NewTB(t))
}

func BenchmarkExtendLabels(b *testing.B) {
	testInjectExtLabels(testutil.NewTB(b))
}

var x labels.Labels

func testInjectExtLabels(tb testutil.TB) {
	in := labels.FromStrings(
		"__name__", "subscription_labels",
		"_id", "0dfsdfsdsfdsffd1e96-4432-9abe-e33436ea969a",
		"account", "1afsdfsddsfsdfsdfsdfsdfs",
		"ebs_account", "1asdasdad45",
		"email_domain", "asdasddgfkw.example.com",
		"endpoint", "metrics",
		"external_organization", "dfsdfsdf",
		"instance", "10.128.4.231:8080",
		"job", "sdd-acct-mngr-metrics",
		"managed", "false",
		"namespace", "production",
		"organization", "dasdadasdasasdasaaFGDSG",
		"pod", "sdd-acct-mngr-6669c947c8-xjx7f",
		"prometheus", "telemeter-production/telemeter",
		"prometheus_replica", "prometheus-telemeter-1",
		"risk", "5",
		"service", "sdd-acct-mngr-metrics",
		"support", "Self-Support", // Should be overwritten.
	)
	extLset := labels.FromStrings(
		"support", "Host-Support",
		"replica", "1",
		"tenant", "2342",
	)
	tb.ResetTimer()
	for i := 0; i < tb.N(); i++ {
		x = ExtendSortedLabels(in, extLset)

		if !tb.IsBenchmark() {
			testutil.Equals(tb, labels.FromStrings(
				"__name__", "subscription_labels",
				"_id", "0dfsdfsdsfdsffd1e96-4432-9abe-e33436ea969a",
				"account", "1afsdfsddsfsdfsdfsdfsdfs",
				"ebs_account", "1asdasdad45",
				"email_domain", "asdasddgfkw.example.com",
				"endpoint", "metrics",
				"external_organization", "dfsdfsdf",
				"instance", "10.128.4.231:8080",
				"job", "sdd-acct-mngr-metrics",
				"managed", "false",
				"namespace", "production",
				"organization", "dasdadasdasasdasaaFGDSG",
				"pod", "sdd-acct-mngr-6669c947c8-xjx7f",
				"prometheus", "telemeter-production/telemeter",
				"prometheus_replica", "prometheus-telemeter-1",
				"replica", "1",
				"risk", "5",
				"service", "sdd-acct-mngr-metrics",
				"support", "Host-Support",
				"tenant", "2342",
			), x)
		}
	}
	fmt.Fprint(ioutil.Discard, x)
}

var (
	zdest ZLabel
	dest  Label
)

func BenchmarkZLabelsMarshalUnmarshal(b *testing.B) {
	const (
		fmtLbl = "%07daaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"
		num    = 1000000
	)

	b.Run("Label", func(b *testing.B) {
		b.ReportAllocs()
		lbls := LabelSet{Labels: make([]Label, 0, num)}
		for i := 0; i < num; i++ {
			lbls.Labels = append(lbls.Labels, Label{Name: fmt.Sprintf(fmtLbl, i), Value: fmt.Sprintf(fmtLbl, i)})
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			data, err := lbls.Marshal()
			testutil.Ok(b, err)

			dest = Label{}
			testutil.Ok(b, (&dest).Unmarshal(data))
		}
	})

	b.Run("ZLabel", func(b *testing.B) {
		b.ReportAllocs()
		lbls := ZLabelSet{Labels: make([]ZLabel, 0, num)}
		for i := 0; i < num; i++ {
			lbls.Labels = append(lbls.Labels, ZLabel{Name: fmt.Sprintf(fmtLbl, i), Value: fmt.Sprintf(fmtLbl, i)})
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			data, err := lbls.Marshal()
			testutil.Ok(b, err)

			zdest = ZLabel{}
			testutil.Ok(b, (&zdest).Unmarshal(data))
		}
	})
}

var ret labels.Labels

func BenchmarkTransformWithAndWithoutCopy(b *testing.B) {
	const (
		fmtLbl = "%07daaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"
		num    = 1000000
	)

	b.Run("ZLabelsToPromLabels", func(b *testing.B) {
		b.ReportAllocs()
		lbls := make([]ZLabel, num)
		for i := 0; i < num; i++ {
			lbls[i] = ZLabel{Name: fmt.Sprintf(fmtLbl, i), Value: fmt.Sprintf(fmtLbl, i)}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ret = ZLabelsToPromLabels(lbls)
		}
	})
	b.Run("ZLabelsToPromLabelsWithRealloc", func(b *testing.B) {
		b.ReportAllocs()
		lbls := make([]ZLabel, num)
		for i := 0; i < num; i++ {
			lbls[i] = ZLabel{Name: fmt.Sprintf(fmtLbl, i), Value: fmt.Sprintf(fmtLbl, i)}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ReAllocZLabelsStrings(&lbls)
			ret = ZLabelsToPromLabels(lbls)
		}
	})
}

func TestSortZLabelSets(t *testing.T) {
	expectedResult := ZLabelSets{
		{
			Labels: ZLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":    "grpc_client_handled_total",
					"cluster":     "test",
					"grpc_code":   "OK",
					"grpc_method": "Info",
				}),
			),
		},
		{
			Labels: ZLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":    "grpc_client_handled_total",
					"cluster":     "test",
					"grpc_code":   "OK",
					"grpc_method": "LabelNames",
				}),
			),
		},
		{
			Labels: ZLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":  "grpc_client_handled_total",
					"cluster":   "test",
					"grpc_code": "OK",
					"aa":        "1",
					"bb":        "2",
					"cc":        "3",
					"dd":        "4",
					"ee":        "5",
				}),
			),
		},
		{
			Labels: ZLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":  "grpc_client_handled_total",
					"cluster":   "test",
					"grpc_code": "OK",
					"aa":        "1",
					"bb":        "2",
					"cc":        "3",
					"dd":        "4",
					"ee":        "5",
				}),
			),
		},
		{
			Labels: ZLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":    "grpc_server_handled_total",
					"cluster":     "test",
					"grpc_code":   "OK",
					"grpc_method": "Info",
				}),
			),
		},
		{
			Labels: ZLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__": "up",
					"instance": "localhost:10908",
				}),
			),
		},
	}

	list := ZLabelSets{
		{
			Labels: ZLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__": "up",
					"instance": "localhost:10908",
				}),
			),
		},
		{
			Labels: ZLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":    "grpc_server_handled_total",
					"cluster":     "test",
					"grpc_code":   "OK",
					"grpc_method": "Info",
				}),
			),
		},
		{
			Labels: ZLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":    "grpc_client_handled_total",
					"cluster":     "test",
					"grpc_code":   "OK",
					"grpc_method": "LabelNames",
				}),
			),
		},
		{
			Labels: ZLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":    "grpc_client_handled_total",
					"cluster":     "test",
					"grpc_code":   "OK",
					"grpc_method": "Info",
				}),
			),
		},
		{
			Labels: ZLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":  "grpc_client_handled_total",
					"cluster":   "test",
					"grpc_code": "OK",
					"aa":        "1",
					"bb":        "2",
					"cc":        "3",
					"dd":        "4",
					"ee":        "5",
				}),
			),
		},
		// This label set is the same as the previous one, which should correctly return 0 in Less() function.
		{
			Labels: ZLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"cluster":   "test",
					"__name__":  "grpc_client_handled_total",
					"grpc_code": "OK",
					"aa":        "1",
					"bb":        "2",
					"cc":        "3",
					"dd":        "4",
					"ee":        "5",
				}),
			),
		},
	}

	sort.Sort(list)
	reflect.DeepEqual(expectedResult, list)
}
