// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package statuspb

import (
	"cmp"
	"maps"
	"slices"

	v1 "github.com/prometheus/prometheus/web/api/v1"
)

func NewTSDBStatisticsResponse(statistics *TSDBStatistics) *TSDBStatisticsResponse {
	return &TSDBStatisticsResponse{
		Result: &TSDBStatisticsResponse_Statistics{
			Statistics: statistics,
		},
	}
}

func NewWarningTSDBStatisticsResponse(warning error) *TSDBStatisticsResponse {
	return &TSDBStatisticsResponse{
		Result: &TSDBStatisticsResponse_Warning{
			Warning: warning.Error(),
		},
	}
}

// Merge merges the provided TSDBStatisticsEntry with the receiver.
func (tse *TSDBStatisticsEntry) Merge(stats *TSDBStatisticsEntry) {
	tse.HeadStatistics.NumSeries += stats.HeadStatistics.NumSeries
	tse.HeadStatistics.NumLabelPairs += stats.HeadStatistics.NumLabelPairs
	tse.HeadStatistics.ChunkCount += stats.HeadStatistics.ChunkCount

	if tse.HeadStatistics.MinTime <= 0 || tse.HeadStatistics.MinTime > stats.HeadStatistics.MinTime {
		tse.HeadStatistics.MinTime = stats.HeadStatistics.MinTime
	}

	if tse.HeadStatistics.MaxTime < stats.HeadStatistics.MaxTime {
		tse.HeadStatistics.MaxTime = stats.HeadStatistics.MaxTime
	}

	tse.SeriesCountByMetricName = mergeStatistics(tse.SeriesCountByMetricName, stats.SeriesCountByMetricName, addValue)
	// The same label values may exist on different instances so it makes more
	// sense to keep the max value rather than adding them all.
	tse.LabelValueCountByLabelName = mergeStatistics(tse.LabelValueCountByLabelName, stats.LabelValueCountByLabelName, maxValue)
	tse.MemoryInBytesByLabelName = mergeStatistics(tse.MemoryInBytesByLabelName, stats.MemoryInBytesByLabelName, addValue)
	tse.SeriesCountByLabelValuePair = mergeStatistics(tse.SeriesCountByLabelValuePair, stats.SeriesCountByLabelValuePair, addValue)
}

func addValue(a, b uint64) uint64 {
	return a + b
}

func maxValue(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func mergeStatistics(a, b []Statistic, mergeFunc func(uint64, uint64) uint64) []Statistic {
	merged := make(map[string]Statistic, len(a))
	for _, stat := range a {
		merged[stat.Name] = stat
	}

	for _, stat := range b {
		v, found := merged[stat.Name]
		if !found {
			merged[stat.Name] = stat
			continue
		}
		v.Value = mergeFunc(v.Value, stat.Value)
		merged[stat.Name] = v
	}

	return slices.SortedFunc(maps.Values(merged), func(a, b Statistic) int {
		if c := cmp.Compare(b.Value, a.Value); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	})
}

// ConvertToPrometheusTSDBStat converts a protobuf Statistic slice to the equivalent Prometheus struct.
func ConvertToPrometheusTSDBStat(stats []Statistic) []v1.TSDBStat {
	ret := make([]v1.TSDBStat, len(stats))
	for i := range stats {
		ret[i] = v1.TSDBStat{
			Name:  stats[i].Name,
			Value: stats[i].Value,
		}
	}

	return ret
}
