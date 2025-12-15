// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package convert provides conversion functions between Thanos protobuf labels
// and Prometheus labels.Labels. This package is separate from labelpb to avoid
// an import cycle when Prometheus imports labelpb.
package convert

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

// ExtendSortedLabels extend given labels by extend in labels format.
// The type conversion is done safely, which means we don't modify extend labels underlying array.
//
// In case of existing labels already present in given label set, it will be overwritten by external one.
func ExtendSortedLabels(lset, extend labels.Labels) labels.Labels {
	if extend.IsEmpty() {
		return lset.Copy()
	}
	b := labels.NewBuilder(lset)
	extend.Range(func(l labels.Label) {
		b.Set(l.Name, l.Value)
	})
	return b.Labels()
}

// PromLabelSetsToString converts a slice of Prometheus labels.Labels to a sorted, comma-separated string.
func PromLabelSetsToString(lsets []labels.Labels) string {
	s := []string{}
	for _, ls := range lsets {
		s = append(s, ls.String())
	}
	sort.Strings(s)
	return strings.Join(s, ",")
}

// LabelSetToPromLabels converts a *labelpb.LabelSet (protobuf v2 generated type) to Prometheus labels.Labels.
func LabelSetToPromLabels(ls *labelpb.LabelSet) labels.Labels {
	if ls == nil || len(ls.Labels) == 0 {
		return labels.EmptyLabels()
	}
	b := labels.NewScratchBuilder(len(ls.Labels))
	for _, l := range ls.Labels {
		b.Add(l.Name, l.Value)
	}
	return b.Labels()
}

// PromLabelsToLabelSet converts Prometheus labels.Labels to *labelpb.LabelSet (protobuf v2 generated type).
func PromLabelsToLabelSet(ls labels.Labels) *labelpb.LabelSet {
	if ls.IsEmpty() {
		return nil
	}
	result := &labelpb.LabelSet{
		Labels: make([]*labelpb.Label, 0, ls.Len()),
	}
	ls.Range(func(l labels.Label) {
		result.Labels = append(result.Labels, &labelpb.Label{Name: l.Name, Value: l.Value})
	})
	return result
}

// LabelSetsToPromLabelSets converts a slice of *labelpb.LabelSet to a slice of Prometheus labels.Labels.
func LabelSetsToPromLabelSets(lss []*labelpb.LabelSet) []labels.Labels {
	result := make([]labels.Labels, 0, len(lss))
	for _, ls := range lss {
		result = append(result, LabelSetToPromLabels(ls))
	}
	return result
}

// PromLabelSetsToLabelSets converts a slice of Prometheus labels.Labels to a slice of *labelpb.LabelSet.
func PromLabelSetsToLabelSets(lss []labels.Labels) []*labelpb.LabelSet {
	result := make([]*labelpb.LabelSet, 0, len(lss))
	for _, ls := range lss {
		result = append(result, PromLabelsToLabelSet(ls))
	}
	return result
}

// PromLabelsToLabels converts Prometheus labels.Labels to []*labelpb.Label (protobuf v2 generated type).
func PromLabelsToLabels(ls labels.Labels) []*labelpb.Label {
	if ls.IsEmpty() {
		return nil
	}
	result := make([]*labelpb.Label, 0, ls.Len())
	ls.Range(func(l labels.Label) {
		result = append(result, &labelpb.Label{Name: l.Name, Value: l.Value})
	})
	return result
}

// LabelsToPromLabels converts []*labelpb.Label (protobuf v2 generated type) to Prometheus labels.Labels.
func LabelsToPromLabels(lbls []*labelpb.Label) labels.Labels {
	if len(lbls) == 0 {
		return labels.EmptyLabels()
	}
	b := labels.NewScratchBuilder(len(lbls))
	for _, l := range lbls {
		b.Add(l.Name, l.Value)
	}
	return b.Labels()
}

// CustomLabelset is a type alias for labels.Labels that provides custom protobuf unmarshaling.
type CustomLabelset labels.Labels

var builderPool = &sync.Pool{
	New: func() any {
		b := labels.NewScratchBuilder(8)
		return &b
	},
}

// UnmarshalProtobuf unmarshals a CustomLabelset from protobuf bytes.
func (l *CustomLabelset) UnmarshalProtobuf(src []byte) (err error) {
	b := builderPool.Get().(*labels.ScratchBuilder)
	b.Reset()

	defer builderPool.Put(b)

	var fc easyproto.FieldContext

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return errors.Wrap(err, "unmarshal next field")
		}

		if fc.FieldNum != 1 {
			return fmt.Errorf("expected field 1, got %d", fc.FieldNum)
		}

		dat, ok := fc.MessageData()
		if !ok {
			return fmt.Errorf("expected message data for field %d", fc.FieldNum)
		}

		var n, v string
		var msgFc easyproto.FieldContext
		for len(dat) > 0 {
			dat, err = msgFc.NextField(dat)
			if err != nil {
				return errors.Wrap(err, "unmarshal next field in message")
			}

			switch msgFc.FieldNum {
			case 1:
				n, ok = msgFc.String()
				if !ok {
					return fmt.Errorf("expected string data for field %d", msgFc.FieldNum)
				}
			case 2:
				v, ok = msgFc.String()
				if !ok {
					return fmt.Errorf("expected string data for field %d", msgFc.FieldNum)
				}
			default:
				return fmt.Errorf("unexpected field %d in label message", msgFc.FieldNum)
			}
		}

		b.Add(n, v)

	}

	*l = CustomLabelset(b.Labels())
	return nil
}
