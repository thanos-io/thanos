// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"strconv"
)

// QueryBytesFetchedHeaderName is the http header name of number of bytes fetched by a query from m3readcoord.
// This name is compatible with M3 and rule manager code
const QueryBytesFetchedHeaderName = "M3-Fetched-Bytes-Estimate"

func sumQueryBytesFetched(responses ...Response) uint64 {
	var result uint64
	result = 0
	for _, resp := range responses {
		for _, hdr := range resp.GetHeaders() {
			if hdr.GetName() == QueryBytesFetchedHeaderName {
				for _, v := range hdr.Values {
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
	return []*PrometheusResponseHeader{{
		Name:   QueryBytesFetchedHeaderName,
		Values: []string{strconv.FormatUint(sumQueryBytesFetched(responses...), 10)},
	}}
}

func QueryBytesFetchedHttpHeaderValue(response Response) []string {
	result := []string{}
	for _, hdr := range response.GetHeaders() {
		if hdr.GetName() == QueryBytesFetchedHeaderName {
			result = hdr.GetValues()
			break
		}
	}
	return result
}
