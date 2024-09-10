package infopb

import (
	"math"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

func NewTSDBInfo(mint, maxt int64, lbls []*labelpb.Label) *TSDBInfo {
	return &TSDBInfo{
		Labels: &labelpb.LabelSet{
			Labels: lbls,
		},
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
		lsets = append(lsets, labelpb.LabelpbLabelsToPromLabels(info.Labels.Labels))

	}
	return lsets
}
