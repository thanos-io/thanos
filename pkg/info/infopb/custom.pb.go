package infopb

import (
	"math"

	"github.com/prometheus/prometheus/model/labels"
)

type TSDBInfos []TSDBInfo

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
		lsets = append(lsets, info.Labels)

	}
	return lsets
}
