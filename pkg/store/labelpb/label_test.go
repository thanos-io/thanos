// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package labelpb

import (
	"fmt"
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
		ExtendLabels(labels.Labels{{Name: "xb", Value: "2"}, {Name: "a", Value: "1"}}, labels.FromStrings("replica", "01")))

	testutil.Equals(t, labels.Labels{{Name: "replica", Value: "01"}},
		ExtendLabels(labels.Labels{}, labels.FromStrings("replica", "01")))

	testutil.Equals(t, labels.Labels{{Name: "a", Value: "1"}, {Name: "replica", Value: "01"}, {Name: "xb", Value: "2"}},
		ExtendLabels(labels.Labels{{Name: "xb", Value: "2"}, {Name: "replica", Value: "NOT01"}, {Name: "a", Value: "1"}}, labels.FromStrings("replica", "01")))
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
