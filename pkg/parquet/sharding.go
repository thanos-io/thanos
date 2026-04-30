// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package parquet

import (
	"fmt"
	"math"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

// Shard represents a group of series that will be written to the same Parquet file.
type Shard struct {
	// Index is the shard number.
	Index int
	// Series is the list of series in this shard with their labels.
	Series []SeriesWithLabels
	// LabelNames is the set of unique label names in this shard.
	LabelNames []string
}

// SeriesWithLabels represents a series with its labels and original index.
type SeriesWithLabels struct {
	Labels      labels.Labels
	Hash        uint64
	OriginalIdx int // Index in the original allSeries slice
}

// ShardSeries divides series into shards based on cardinality and label column count.
// Sharding prevents schema explosion (too many columns) and keeps file sizes manageable.
// Returns a slice of shards, where each shard will be written to separate Parquet files.
func ShardSeries(
	allSeries []labels.Labels,
	targetSeriesPerShard int,
	sortLabels []string,
) ([]Shard, error) {
	if len(allSeries) == 0 {
		return nil, fmt.Errorf("no series to shard")
	}

	// Sort series by the specified labels to improve compression and query performance
	sortedSeries := make([]labels.Labels, len(allSeries))
	copy(sortedSeries, allSeries)
	compareBySortedLabels := func(a, b labels.Labels) int {
		for _, lb := range sortLabels {
			aVal := a.Get(lb)
			bVal := b.Get(lb)
			if aVal != bVal {
				if aVal < bVal {
					return -1
				}
				return 1
			}
		}
		return labels.Compare(a, b)
	}

	// Sort series using the comparison function
	for i := 0; i < len(sortedSeries)-1; i++ {
		for j := i + 1; j < len(sortedSeries); j++ {
			if compareBySortedLabels(sortedSeries[i], sortedSeries[j]) > 0 {
				sortedSeries[i], sortedSeries[j] = sortedSeries[j], sortedSeries[i]
			}
		}
	}

	// Create shards
	shards := []Shard{{Index: 0}}
	currentShard := &shards[0]
	currentLabelNames := make(map[string]struct{})
	currentUniqueCount := 0
	var currentHash uint64

	for idx, lbls := range sortedSeries {
		hash := lbls.Hash()

		// Check if this is a new unique series
		isNewSeries := (hash != currentHash)
		if isNewSeries {
			// Collect label names for this series
			seriesLabelNames := make(map[string]struct{})
			lbls.Range(func(l labels.Label) {
				seriesLabelNames[l.Name] = struct{}{}
			})

			// Calculate potential new column count
			potentialLabelCount := len(currentLabelNames)
			for name := range seriesLabelNames {
				if _, exists := currentLabelNames[name]; !exists {
					potentialLabelCount++
				}
			}

			// Check if we need to create a new shard:
			// 1. Too many unique series (exceeds targetSeriesPerShard)
			// 2. Too many columns (approaching MaxInt16 limit for Parquet)
			needNewShard := currentUniqueCount >= targetSeriesPerShard ||
				potentialLabelCount+ChunkColumnsPerDay+2 >= math.MaxInt16

			if needNewShard && currentUniqueCount > 0 {
				// Start a new shard
				newShardIndex := len(shards)
				shards = append(shards, Shard{Index: newShardIndex})
				currentShard = &shards[newShardIndex]
				currentLabelNames = make(map[string]struct{})
				currentUniqueCount = 0

				// Add this series' labels to the new shard
				lbls.Range(func(l labels.Label) {
					currentLabelNames[strings.Clone(l.Name)] = struct{}{}
				})
			} else {
				// Add new labels to current shard
				lbls.Range(func(l labels.Label) {
					if _, exists := currentLabelNames[l.Name]; !exists {
						currentLabelNames[strings.Clone(l.Name)] = struct{}{}
					}
				})
			}

			currentUniqueCount++
			currentHash = hash
		}

		// Add series to current shard
		currentShard.Series = append(currentShard.Series, SeriesWithLabels{
			Labels:      lbls,
			Hash:        hash,
			OriginalIdx: idx,
		})
	}

	// Extract sorted label names for each shard
	for i := range shards {
		shard := &shards[i]
		labelNamesSet := make(map[string]struct{})
		for _, series := range shard.Series {
			series.Labels.Range(func(l labels.Label) {
				labelNamesSet[l.Name] = struct{}{}
			})
		}

		shard.LabelNames = make([]string, 0, len(labelNamesSet))
		for name := range labelNamesSet {
			shard.LabelNames = append(shard.LabelNames, name)
		}
		// Sort label names for consistent schema ordering
		for i := 0; i < len(shard.LabelNames)-1; i++ {
			for j := i + 1; j < len(shard.LabelNames); j++ {
				if shard.LabelNames[i] > shard.LabelNames[j] {
					shard.LabelNames[i], shard.LabelNames[j] = shard.LabelNames[j], shard.LabelNames[i]
				}
			}
		}
	}

	return shards, nil
}
