// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"encoding/json"
	"unsafe"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

// ThanosResponseExtractor helps to extract specific info from Query Response.
type ThanosResponseExtractor struct{}

// Extract extracts response for specific a range from a response.
// This interface is not used for labels and series responses.
func (ThanosResponseExtractor) Extract(_, _ int64, resp queryrange.Response) queryrange.Response {
	return resp
}

// ResponseWithoutHeaders returns the response without HTTP headers.
func (ThanosResponseExtractor) ResponseWithoutHeaders(resp queryrange.Response) queryrange.Response {
	switch tr := resp.(type) {
	case *ThanosLabelsResponse:
		return &ThanosLabelsResponse{Status: queryrange.StatusSuccess, Data: tr.Data}
	case *ThanosSeriesResponse:
		return &ThanosSeriesResponse{Status: queryrange.StatusSuccess, Data: tr.Data}
	}
	return resp
}

func (ThanosResponseExtractor) ResponseWithoutStats(resp queryrange.Response) queryrange.Response {
	switch tr := resp.(type) {
	case *ThanosLabelsResponse:
		return &ThanosLabelsResponse{Status: queryrange.StatusSuccess, Data: tr.Data}
	case *ThanosSeriesResponse:
		return &ThanosSeriesResponse{Status: queryrange.StatusSuccess, Data: tr.Data}
	}
	return resp
}

// headersToQueryRangeHeaders convert slice of ResponseHeader to Cortex queryrange.PrometheusResponseHeader in an
// unsafe manner. It reuses the same memory.
func headersToQueryRangeHeaders(headers []*ResponseHeader) []*queryrange.PrometheusResponseHeader {
	return *(*[]*queryrange.PrometheusResponseHeader)(unsafe.Pointer(&headers))
}

// GetHeaders returns the HTTP headers in the response.
func (m *ThanosLabelsResponse) GetHeaders() []*queryrange.PrometheusResponseHeader {
	return headersToQueryRangeHeaders(m.Headers)
}

// GetHeaders returns the HTTP headers in the response.
func (m *ThanosSeriesResponse) GetHeaders() []*queryrange.PrometheusResponseHeader {
	return headersToQueryRangeHeaders(m.Headers)
}

// GetStats returns response stats. Unimplemented for ThanosLabelsResponse.
func (m *ThanosLabelsResponse) GetStats() *queryrange.PrometheusResponseStats {
	return nil
}

// GetStats returns response stats. Unimplemented for ThanosSeriesResponse.
func (m *ThanosSeriesResponse) GetStats() *queryrange.PrometheusResponseStats {
	return nil
}

// MarshalJSON implements json.Marshaler for ThanosLabelsResponse.
// Headers are excluded from JSON serialization to match the original gogoproto behavior.
func (m *ThanosLabelsResponse) MarshalJSON() ([]byte, error) {
	type alias struct {
		Status    string   `json:"status"`
		Data      []string `json:"data"`
		ErrorType string   `json:"errorType,omitempty"`
		Error     string   `json:"error,omitempty"`
	}
	return json.Marshal(&alias{
		Status:    m.Status,
		Data:      m.Data,
		ErrorType: m.ErrorType,
		Error:     m.Error,
	})
}

// UnmarshalJSON implements json.Unmarshaler for ThanosLabelsResponse.
// Headers are excluded from JSON serialization to match the original gogoproto behavior.
func (m *ThanosLabelsResponse) UnmarshalJSON(data []byte) error {
	type alias struct {
		Status    string   `json:"status"`
		Data      []string `json:"data"`
		ErrorType string   `json:"errorType,omitempty"`
		Error     string   `json:"error,omitempty"`
	}
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	m.Status = a.Status
	m.Data = a.Data
	m.ErrorType = a.ErrorType
	m.Error = a.Error
	return nil
}

// MarshalJSON implements json.Marshaler for ThanosSeriesResponse.
// Headers are excluded from JSON serialization to match the original gogoproto behavior.
func (m *ThanosSeriesResponse) MarshalJSON() ([]byte, error) {
	type alias struct {
		Status    string              `json:"status"`
		Data      []*labelpb.LabelSet `json:"data"`
		ErrorType string              `json:"errorType,omitempty"`
		Error     string              `json:"error,omitempty"`
	}
	return json.Marshal(&alias{
		Status:    m.Status,
		Data:      m.Data,
		ErrorType: m.ErrorType,
		Error:     m.Error,
	})
}

// UnmarshalJSON implements json.Unmarshaler for ThanosSeriesResponse.
// Headers are excluded from JSON serialization to match the original gogoproto behavior.
func (m *ThanosSeriesResponse) UnmarshalJSON(data []byte) error {
	type alias struct {
		Status    string              `json:"status"`
		Data      []*labelpb.LabelSet `json:"data"`
		ErrorType string              `json:"errorType,omitempty"`
		Error     string              `json:"error,omitempty"`
	}
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	m.Status = a.Status
	m.Data = a.Data
	m.ErrorType = a.ErrorType
	m.Error = a.Error
	return nil
}
