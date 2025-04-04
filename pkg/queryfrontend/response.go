// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"unsafe"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
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
