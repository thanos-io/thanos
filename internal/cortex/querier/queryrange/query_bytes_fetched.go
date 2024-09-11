// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"net/http"
	"strconv"
)

// QueryBytesFetchedHeaderName is the http header name of number of bytes fetched by a query from m3readcoord.
// This name is compatible with M3 and rule manager code
const QueryBytesFetchedHeaderName = "M3-Fetched-Bytes-Estimate"
const QuerySeriesFetchedHeaderName = "M3-Fetched-Series-Estimate"

func sumQueryBytesFetched(responses ...Response) uint64 {
	var result uint64
	result = 0
	for _, resp := range responses {
		for _, hdr := range resp.GetHeaders() {
			if hdr.GetName() == QueryBytesFetchedHeaderName {
				for _, v := range hdr.GetValues() {
					n, err := strconv.ParseUint(v, 10, 64)
					if err != nil {
						continue
					}
					result += n
				}
				break
			}
		}
	}
	return result
}

func QueryBytesFetchedPrometheusResponseHeaders(responses ...Response) []*PrometheusResponseHeader {
	n := sumQueryBytesFetched(responses...)
	if n == 0 {
		return nil
	}
	return []*PrometheusResponseHeader{{
		Name:   QueryBytesFetchedHeaderName,
		Values: []string{strconv.FormatUint(n, 10)},
	}}
}

func QueryBytesFetchedHttpHeaderValue(response Response) []string {
	var result []string
	for _, hdr := range response.GetHeaders() {
		if hdr.GetName() == QueryBytesFetchedHeaderName {
			result = hdr.GetValues()
			break
		}
	}
	return result
}

func getHeaderValue(hdr http.Header, key string) uint64 {
	if val, ok := hdr[key]; ok {
		if len(val) != 1 {
			return 0
		}
		n, err := strconv.ParseUint(val[0], 10, 64)
		if err != nil {
			return 0
		}
		return n
	}
	return 0
}

func GetQueryBytesFetchedFromHeader(hdr http.Header) uint64 {
	return getHeaderValue(hdr, QueryBytesFetchedHeaderName)
}

func GetQuerySeriesFetchedFromHeader(hdr http.Header) uint64 {
	return getHeaderValue(hdr, QuerySeriesFetchedHeaderName)
}
