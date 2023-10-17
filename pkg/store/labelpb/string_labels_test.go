// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package labelpb

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/labels"

	fuzz "github.com/AdaLogics/go-fuzz-headers"

	"github.com/efficientgo/core/testutil"
)

func TestStringLabelsToPromLabels_LabelsToPromLabels(t *testing.T) {
	testutil.Equals(t, labels.FromMap(testLsetMap), StringLabelsToPromLabels(StringLabelsFromPromLabels(labels.FromMap(testLsetMap))))
}

func TestStringLabelsMarshal_Unmarshal(t *testing.T) {
	l := StringLabelsFromPromLabels(labels.FromStrings("aaaaaaa", "bbbbb"))
	b, err := (&l).Marshal()
	testutil.Ok(t, err)

	l2 := &StringLabels{}
	testutil.Ok(t, l2.Unmarshal(b))
	testutil.Equals(t, labels.FromStrings("aaaaaaa", "bbbbb"), StringLabelsToPromLabels(*l2))
}

func BenchmarkStringLabelsMarshalUnmarshal(b *testing.B) {
	const (
		fmtLbl = "%07daaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"
		num    = 1000000
	)

	b.Run("StringLabels", func(b *testing.B) {
		b.ReportAllocs()
		m := make(map[string]string, num)
		for i := 0; i < num; i++ {
			m[fmt.Sprintf(fmtLbl, i)] = fmt.Sprintf(fmtLbl, i)
		}
		lbls := StringLabelsFromPromLabels(labels.FromMap(m))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			data, err := lbls.Marshal()
			testutil.Ok(b, err)
			testutil.Ok(b, (&StringLabels{}).Unmarshal(data))
		}
	})
}

func FuzzUnsafeStringlabelConversions(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5})
	f.Fuzz(func(t *testing.T, b []byte) {
		var m map[string]string
		if err := fuzz.NewConsumer(b).FuzzMap(&m); err != nil {
			return
		}
		lbls := labels.FromMap(m)

		before := StringLabelsFromPromLabels(lbls)
		data, err := proto.Marshal(&before)
		testutil.Ok(t, err)

		after := StringLabels{}
		testutil.Ok(t, proto.Unmarshal(data, &after))

		testutil.Equals(t, StringLabelsToPromLabels(after), lbls)
	})
}
