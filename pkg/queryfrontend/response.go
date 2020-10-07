// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import "github.com/prometheus/prometheus/pkg/labels"

// ThanosLabelsResponse represents a label_names or label_values response.
type ThanosLabelsResponse struct {
	Status    string   `json:"status"`
	Data      []string `json:"data,omitempty"`
	ErrorType string   `json:"errorType,omitempty"`
	Error     string   `json:"error,omitempty"`
}

func (r ThanosLabelsResponse) Reset()         {}
func (r ThanosLabelsResponse) String() string { return "" }
func (r ThanosLabelsResponse) ProtoMessage()  {}

// ThanosSeriesResponse represents a series response.
type ThanosSeriesResponse struct {
	Status    string          `json:"status"`
	Data      []labels.Labels `json:"data,omitempty"`
	ErrorType string          `json:"errorType,omitempty"`
	Error     string          `json:"error,omitempty"`
}

func (r ThanosSeriesResponse) Reset()         {}
func (r ThanosSeriesResponse) String() string { return "" }
func (r ThanosSeriesResponse) ProtoMessage()  {}
