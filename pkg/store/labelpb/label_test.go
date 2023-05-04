// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package labelpb

import (
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/efficientgo/core/testutil"
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
	testutil.Equals(t, labels.FromMap(testLsetMap), ProtobufLabelsToPromLabels(ProtobufLabelsFromPromLabels(labels.FromMap(testLsetMap))))

	lset := labels.FromMap(testLsetMap)
	for i := range ProtobufLabelsFromPromLabels(lset) {
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

func TestExtendLabels(t *testing.T) {
	testutil.Equals(t, labels.Labels{{Name: "a", Value: "1"}, {Name: "replica", Value: "01"}, {Name: "xb", Value: "2"}},
		ExtendSortedLabels(labels.Labels{{Name: "a", Value: "1"}, {Name: "xb", Value: "2"}}, labels.FromStrings("replica", "01")))

	testutil.Equals(t, labels.Labels{{Name: "replica", Value: "01"}},
		ExtendSortedLabels(labels.Labels{}, labels.FromStrings("replica", "01")))

	testutil.Equals(t, labels.Labels{{Name: "a", Value: "1"}, {Name: "replica", Value: "01"}, {Name: "xb", Value: "2"}},
		ExtendSortedLabels(labels.Labels{{Name: "a", Value: "1"}, {Name: "replica", Value: "NOT01"}, {Name: "xb", Value: "2"}}, labels.FromStrings("replica", "01")))

	testInjectExtLabels(testutil.NewTB(t))
}

func TestValidateLabels(t *testing.T) {
	testCases := []struct {
		labelSet    []*Label
		expectedErr error
	}{
		{
			// No labels at all.
			labelSet:    []*Label{},
			expectedErr: ErrEmptyLabels,
		},
		{
			// Empty label.
			labelSet: []*Label{
				{
					Name:  "foo",
					Value: "bar",
				},
				{
					Name:  "",
					Value: "baz",
				},
			},
			expectedErr: ErrEmptyLabels,
		},
		{
			// Empty label (first label).
			labelSet: []*Label{
				{
					Name:  "",
					Value: "bar",
				},
				{
					Name:  "foo",
					Value: "baz",
				},
			},
			expectedErr: ErrEmptyLabels,
		},
		{
			// Empty label (empty value).
			labelSet: []*Label{
				{
					Name:  "foo",
					Value: "bar",
				},
				{
					Name:  "baz",
					Value: "",
				},
			},
			expectedErr: ErrEmptyLabels,
		},
		{
			// Out-of-order and duplicate label (out-of-order comes first).
			labelSet: []*Label{
				{
					Name:  "foo",
					Value: "bar",
				},
				{
					Name:  "test",
					Value: "baz",
				},
				{
					Name:  "foo",
					Value: "bar",
				},
			},
			expectedErr: ErrOutOfOrderLabels,
		},
		{
			// Out-of-order and duplicate label (out-of-order comes first).
			labelSet: []*Label{
				{
					Name:  "__test__",
					Value: "baz",
				},
				{
					Name:  "foo",
					Value: "bar",
				},
				{
					Name:  "foo",
					Value: "bar",
				},
				{
					Name:  "test",
					Value: "baz",
				},
			},
			expectedErr: ErrDuplicateLabels,
		},
		{
			// Empty and duplicate label (empty comes first).
			labelSet: []*Label{
				{
					Name:  "foo",
					Value: "bar",
				},
				{
					Name:  "",
					Value: "baz",
				},
				{
					Name:  "foo",
					Value: "bar",
				},
			},
			expectedErr: ErrEmptyLabels,
		},
		{
			// Wrong order.
			labelSet: []*Label{
				{
					Name:  "a",
					Value: "bar",
				},
				{
					Name:  "b",
					Value: "baz",
				},
				{
					Name:  "__name__",
					Value: "test",
				},
			},
			expectedErr: ErrOutOfOrderLabels,
		},
		{
			// Wrong order and duplicate (wrong order comes first).
			labelSet: []*Label{
				{
					Name:  "a",
					Value: "bar",
				},
				{
					Name:  "__name__",
					Value: "test",
				},
				{
					Name:  "a",
					Value: "bar",
				},
			},
			expectedErr: ErrOutOfOrderLabels,
		},
		{
			// All good.
			labelSet: []*Label{
				{
					Name:  "__name__",
					Value: "test",
				},
				{
					Name:  "a1",
					Value: "bar",
				},
				{
					Name:  "a2",
					Value: "baz",
				},
			},
			expectedErr: nil,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case %d", i+1), func(t *testing.T) {
			err := ValidateLabels(tc.labelSet)
			testutil.Equals(t, tc.expectedErr, err)
		})
	}

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
	fmt.Fprint(io.Discard, x)
}

var (
	zdest *Label
	dest  Label
)

func BenchmarkTransformWithAndWithoutCopyWithSingleLabel(b *testing.B) {
	benchmarkTransformWithAndWithoutCopy(b, 1)
}

func BenchmarkTransformWithAndWithoutCopyWith1000Labels(b *testing.B) {
	benchmarkTransformWithAndWithoutCopy(b, 1000)
}

func BenchmarkTransformWithAndWithoutCopyWith100000Labels(b *testing.B) {
	benchmarkTransformWithAndWithoutCopy(b, 100000)
}

var ret labels.Labels

func benchmarkTransformWithAndWithoutCopy(b *testing.B, num int) {
	const fmtLbl = "%07daaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"

	b.Run("ProtobufLabelsToPromLabels", func(b *testing.B) {
		b.ReportAllocs()
		lbls := make([]*Label, num)
		for i := 0; i < num; i++ {
			lbls[i] = &Label{Name: fmt.Sprintf(fmtLbl, i), Value: fmt.Sprintf(fmtLbl, i)}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ret = ProtobufLabelsToPromLabels(lbls)
		}
	})
	b.Run("ProtobufLabelsToPromLabelsWithRealloc", func(b *testing.B) {
		b.ReportAllocs()
		lbls := make([]*Label, num)
		for i := 0; i < num; i++ {
			lbls[i] = &Label{Name: fmt.Sprintf(fmtLbl, i), Value: fmt.Sprintf(fmtLbl, i)}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ReAllocZLabelsStrings(&lbls, true)
			ret = ProtobufLabelsToPromLabels(lbls)
		}
	})
}

func TestSortZLabelSets(t *testing.T) {
	expectedResult := ZLabelSets{
		{
			Labels: ProtobufLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":    "grpc_client_handled_total",
					"cluster":     "test",
					"grpc_code":   "OK",
					"grpc_method": "Info",
				}),
			),
		},
		{
			Labels: ProtobufLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":    "grpc_client_handled_total",
					"cluster":     "test",
					"grpc_code":   "OK",
					"grpc_method": "LabelNames",
				}),
			),
		},
		{
			Labels: ProtobufLabelsFromPromLabels(
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
			Labels: ProtobufLabelsFromPromLabels(
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
			Labels: ProtobufLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":    "grpc_server_handled_total",
					"cluster":     "test",
					"grpc_code":   "OK",
					"grpc_method": "Info",
				}),
			),
		},
		{
			Labels: ProtobufLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__": "up",
					"instance": "localhost:10908",
				}),
			),
		},
	}

	list := ZLabelSets{
		{
			Labels: ProtobufLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__": "up",
					"instance": "localhost:10908",
				}),
			),
		},
		{
			Labels: ProtobufLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":    "grpc_server_handled_total",
					"cluster":     "test",
					"grpc_code":   "OK",
					"grpc_method": "Info",
				}),
			),
		},
		{
			Labels: ProtobufLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":    "grpc_client_handled_total",
					"cluster":     "test",
					"grpc_code":   "OK",
					"grpc_method": "LabelNames",
				}),
			),
		},
		{
			Labels: ProtobufLabelsFromPromLabels(
				labels.FromMap(map[string]string{
					"__name__":    "grpc_client_handled_total",
					"cluster":     "test",
					"grpc_code":   "OK",
					"grpc_method": "Info",
				}),
			),
		},
		{
			Labels: ProtobufLabelsFromPromLabels(
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
			Labels: ProtobufLabelsFromPromLabels(
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

func TestHashWithPrefix(t *testing.T) {
	lbls := []*Label{
		{Name: "foo", Value: "bar"},
		{Name: "baz", Value: "qux"},
	}
	testutil.Equals(t, HashWithPrefix("a", lbls), HashWithPrefix("a", lbls))
	testutil.Assert(t, HashWithPrefix("a", lbls) != HashWithPrefix("a", []*Label{lbls[0]}))
	testutil.Assert(t, HashWithPrefix("a", lbls) != HashWithPrefix("a", []*Label{lbls[1], lbls[0]}))
	testutil.Assert(t, HashWithPrefix("a", lbls) != HashWithPrefix("b", lbls))
}

var benchmarkLabelsResult uint64

func BenchmarkHasWithPrefix(b *testing.B) {
	for _, tcase := range []struct {
		name string
		lbls []*Label
	}{
		{
			name: "typical labels under 1KB",
			lbls: func() []*Label {
				lbls := make([]*Label, 10)
				for i := 0; i < len(lbls); i++ {
					// *Label ~20B name, 50B value.
					lbls[i] = &Label{Name: fmt.Sprintf("abcdefghijabcdefghijabcdefghij%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i)}
				}
				return lbls
			}(),
		},
		{
			name: "bigger labels over 1KB",
			lbls: func() []*Label {
				lbls := make([]*Label, 10)
				for i := 0; i < len(lbls); i++ {
					//*Label ~50B name, 50B value.
					lbls[i] = &Label{Name: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i)}
				}
				return lbls
			}(),
		},
		{
			name: "extremely large label value 10MB",
			lbls: func() []*Label {
				lbl := &strings.Builder{}
				lbl.Grow(1024 * 1024 * 10) // 10MB.
				word := "abcdefghij"
				for i := 0; i < lbl.Cap()/len(word); i++ {
					_, _ = lbl.WriteString(word)
				}
				return []*Label{{Name: "__name__", Value: lbl.String()}}
			}(),
		},
	} {
		b.Run(tcase.name, func(b *testing.B) {
			var h uint64

			const prefix = "test-"

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h = HashWithPrefix(prefix, tcase.lbls)
			}
			benchmarkLabelsResult = h
		})
	}
}
