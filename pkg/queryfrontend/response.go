package queryfrontend

import "github.com/prometheus/prometheus/pkg/labels"

type ThanosLabelsResponse struct {
	Status    string   `json:"status"`
	Data      []string `json:"data,omitempty"`
	ErrorType string   `json:"errorType,omitempty"`
	Error     string   `json:"error,omitempty"`
}

func (r ThanosLabelsResponse) Reset()         {}
func (r ThanosLabelsResponse) String() string { return "" }
func (r ThanosLabelsResponse) ProtoMessage()  {}

type ThanosSeriesResponse struct {
	Status    string          `json:"status"`
	Data      []labels.Labels `json:"data,omitempty"`
	ErrorType string          `json:"errorType,omitempty"`
	Error     string          `json:"error,omitempty"`
}

func (r ThanosSeriesResponse) Reset()         {}
func (r ThanosSeriesResponse) String() string { return "" }
func (r ThanosSeriesResponse) ProtoMessage()  {}
