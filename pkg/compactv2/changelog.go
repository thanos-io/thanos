// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compactv2

import (
	"fmt"
	"io"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

type ChangeLogger interface {
	DeleteSeries(del labels.Labels, intervals tombstones.Intervals)
	ModifySeries(old labels.Labels, new labels.Labels)
}

type changeLog struct {
	w io.Writer
}

func NewChangeLog(w io.Writer) *changeLog {
	return &changeLog{
		w: w,
	}
}

func (l *changeLog) DeleteSeries(del labels.Labels, intervals tombstones.Intervals) {
	_, _ = fmt.Fprintf(l.w, "Deleted %v %v\n", del.String(), intervals)
}

func (l *changeLog) ModifySeries(old, new labels.Labels) {
	_, _ = fmt.Fprintf(l.w, "Relabelled %v %v\n", old.String(), new.String())
}
