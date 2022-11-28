// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

// This file was taken from Prometheus (https://github.com/prometheus/prometheus).
// The original license header is included below:
//
// Copyright 2014 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encoding

import "github.com/prometheus/client_golang/prometheus"

// Usually, a separate file for instrumentation is frowned upon. Metrics should
// be close to where they are used. However, the metrics below are set all over
// the place, so we go for a separate instrumentation file in this case.
var (
	Ops = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "chunk_ops_total",
			Help:      "The total number of chunk operations by their type.",
		},
		[]string{OpTypeLabel},
	)
)

const (
	namespace = "prometheus"
	subsystem = "local_storage"

	// OpTypeLabel is the label name for chunk operation types.
	OpTypeLabel = "type"

	// Transcode is the label value for transcode chunk ops.
	Transcode = "transcode"
)

func init() {
	prometheus.MustRegister(Ops)
}
