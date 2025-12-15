package infopb

import (
	"math"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

// NewTSDBInfo creates a new TSDBInfo from Prometheus labels.
func NewTSDBInfo(mint, maxt int64, lbls labels.Labels) *TSDBInfo {
	return &TSDBInfo{
		Labels:  labelpb.PromLabelsToLabelSet(lbls),
		MinTime: mint,
		MaxTime: maxt,
	}
}

type TSDBInfos []*TSDBInfo

func (infos TSDBInfos) MaxT() int64 {
	var maxt int64 = math.MinInt64
	for _, info := range infos {
		if info.MaxTime > maxt {
			maxt = info.MaxTime
		}
	}
	return maxt
}

func (infos TSDBInfos) LabelSets() []labels.Labels {
	lsets := make([]labels.Labels, 0, len(infos))
	for _, info := range infos {
		if info != nil && info.Labels != nil {
			lsets = append(lsets, LabelSetToPromLabels(info.Labels))
		}
	}
	return lsets
}

// LabelSetToPromLabels converts a proto LabelSet to Prometheus labels.Labels.
func LabelSetToPromLabels(ls *labelpb.LabelSet) labels.Labels {
	if ls == nil {
		return labels.EmptyLabels()
	}
	b := labels.NewScratchBuilder(len(ls.Labels))
	for _, l := range ls.Labels {
		if l != nil {
			b.Add(l.Name, l.Value)
		}
	}
	return b.Labels()
}
