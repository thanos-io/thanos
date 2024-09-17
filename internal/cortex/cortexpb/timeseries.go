// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package cortexpb

import (
	"flag"
)

var (
	expectedTimeseries       = 100
	expectedLabels           = 20
	expectedSamplesPerSeries = 10
)

// PreallocConfig configures how structures will be preallocated to optimise
// proto unmarshalling.
type PreallocConfig struct{}

// RegisterFlags registers configuration settings.
func (PreallocConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&expectedTimeseries, "ingester-client.expected-timeseries", expectedTimeseries, "Expected number of timeseries per request, used for preallocations.")
	f.IntVar(&expectedLabels, "ingester-client.expected-labels", expectedLabels, "Expected number of labels per timeseries, used for preallocations.")
	f.IntVar(&expectedSamplesPerSeries, "ingester-client.expected-samples-per-series", expectedSamplesPerSeries, "Expected number of samples per timeseries, used for preallocations.")
}
