// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package statuspb

import (
	"cmp"
	"maps"
	"slices"

	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
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

	return slices.SortedStableFunc(maps.Values(merged), func(a, b Statistic) int {
		// Descending sort.
		return cmp.Compare(b.Value, a.Value)
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

// ToTSDBStats converts a TSDBStatisticsEntry to tsdb.Stats format.
// This is useful for components like Sidecar that receive statistics from Prometheus
// and need to convert them to the internal TSDB stats format.
// The limit parameter controls the maximum number of entries in each index stats slice.
// A limit of 0 or negative means no limit.
func (tse *TSDBStatisticsEntry) ToTSDBStats(limit int) tsdb.Stats {
	stats := tsdb.Stats{
		NumSeries: tse.HeadStatistics.NumSeries,
		MinTime:   tse.HeadStatistics.MinTime,
		MaxTime:   tse.HeadStatistics.MaxTime,
	}

	if len(tse.SeriesCountByMetricName) > 0 ||
		len(tse.LabelValueCountByLabelName) > 0 ||
		len(tse.SeriesCountByLabelValuePair) > 0 ||
		tse.HeadStatistics.NumLabelPairs > 0 {

		stats.IndexPostingStats = &index.PostingsStats{
			NumLabelPairs:           int(tse.HeadStatistics.NumLabelPairs),
			CardinalityMetricsStats: convertToIndexStats(tse.SeriesCountByMetricName, limit),
			LabelValueStats:         convertToIndexStats(tse.LabelValueCountByLabelName, limit),
			LabelValuePairsStats:    convertToIndexStats(tse.SeriesCountByLabelValuePair, limit),
		}
	}

	return stats
}

// convertToIndexStats converts a slice of Statistic to index.Stat slice.
// The limit parameter controls the maximum number of entries returned.
// A limit of 0 or negative means no limit.
func convertToIndexStats(stats []Statistic, limit int) []index.Stat {
	if len(stats) == 0 {
		return nil
	}
	result := make([]index.Stat, len(stats))
	for i, s := range stats {
		result[i] = index.Stat{
			Name:  s.Name,
			Count: s.Value,
		}
	}
	if limit > 0 && limit < len(result) {
		result = result[:limit]
	}
	return result
}
