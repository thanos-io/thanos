// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package labelpb

import (
	"fmt"
	"strings"
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
	testutil.Equals(t, labels.FromMap(testLsetMap), LabelsToPromLabels(LabelsFromPromLabels(labels.FromMap(testLsetMap))))

	lset := labels.FromMap(testLsetMap)
	for i := range LabelsFromPromLabels(lset) {
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

func TestLabelMarshall_Unmarshall(t *testing.T) {
	l := LabelsFromPromLabels(labels.FromStrings("aaaaaaa", "bbbbb"))[0]
	b, err := (&l).Marshal()
	testutil.Ok(t, err)

	l2 := &Label{}
	testutil.Ok(t, l2.Unmarshal(b))
	testutil.Equals(t, labels.FromStrings("aaaaaaa", "bbbbb"), LabelsToPromLabels([]Label{*l2}))
}

func TestExtendLabels(t *testing.T) {
	testutil.Equals(t, labels.Labels{{Name: "a", Value: "1"}, {Name: "replica", Value: "01"}, {Name: "xb", Value: "2"}},
		ExtendLabels(labels.Labels{{Name: "xb", Value: "2"}, {Name: "a", Value: "1"}}, labels.FromStrings("replica", "01")))

	testutil.Equals(t, labels.Labels{{Name: "replica", Value: "01"}},
		ExtendLabels(labels.Labels{}, labels.FromStrings("replica", "01")))

	testutil.Equals(t, labels.Labels{{Name: "a", Value: "1"}, {Name: "replica", Value: "01"}, {Name: "xb", Value: "2"}},
		ExtendLabels(labels.Labels{{Name: "xb", Value: "2"}, {Name: "replica", Value: "NOT01"}, {Name: "a", Value: "1"}}, labels.FromStrings("replica", "01")))
}

var (
	dest     Label
	destCopy FullCopyLabel
)

func BenchmarkLabelsMarshallUnmarshall(b *testing.B) {
	const (
		fmtLbl = "%07daaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"
		num    = 1000000
	)

	b.Run("copy", func(b *testing.B) {
		b.ReportAllocs()
		lbls := FullCopyLabelSet{Labels: make([]FullCopyLabel, 0, num)}
		for i := 0; i < num; i++ {
			lbls.Labels = append(lbls.Labels, FullCopyLabel{Name: fmt.Sprintf(fmtLbl, i), Value: fmt.Sprintf(fmtLbl, i)})
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			data, err := lbls.Marshal()
			testutil.Ok(b, err)

			destCopy = FullCopyLabel{}
			testutil.Ok(b, (&destCopy).Unmarshal(data))
		}
	})

	b.Run("zerocopy", func(b *testing.B) {
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
}

func TestHashWithPrefix(t *testing.T) {
	lbls := []Label{
		{Name: "foo", Value: "bar"},
		{Name: "baz", Value: "qux"},
	}
	testutil.Equals(t, HashWithPrefix("a", lbls), HashWithPrefix("a", lbls))
	testutil.Assert(t, HashWithPrefix("a", lbls) != HashWithPrefix("a", []Label{lbls[0]}))
	testutil.Assert(t, HashWithPrefix("a", lbls) != HashWithPrefix("a", []Label{lbls[1], lbls[0]}))
	testutil.Assert(t, HashWithPrefix("a", lbls) != HashWithPrefix("b", lbls))
}

var benchmarkLabelsResult uint64

func BenchmarkHasWithPrefix(b *testing.B) {
	for _, tcase := range []struct {
		name string
		lbls []Label
	}{
		{
			name: "typical labels under 1KB",
			lbls: func() []Label {
				lbls := make([]Label, 10)
				for i := 0; i < len(lbls); i++ {
					// Label ~20B name, 50B value.
					lbls[i] = Label{Name: fmt.Sprintf("abcdefghijabcdefghijabcdefghij%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i)}
				}
				return lbls
			}(),
		},
		{
			name: "bigger labels over 1KB",
			lbls: func() []Label {
				lbls := make([]Label, 10)
				for i := 0; i < len(lbls); i++ {
					//Label ~50B name, 50B value.
					lbls[i] = Label{Name: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i)}
				}
				return lbls
			}(),
		},
		{
			name: "extremely large label value 10MB",
			lbls: func() []Label {
				lbl := &strings.Builder{}
				lbl.Grow(1024 * 1024 * 10) // 10MB.
				word := "abcdefghij"
				for i := 0; i < lbl.Cap()/len(word); i++ {
					_, _ = lbl.WriteString(word)
				}
				return []Label{{Name: "__name__", Value: lbl.String()}}
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
