// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"strconv"
)

const QueryBytesFetchedHeaderName = "M3-Fetched-Bytes-Estimate"

func sumQueryBytesFetched(responses ...Response) int {
	result := 0
	for _, resp := range responses {
		for _, hdr := range resp.GetHeaders() {
			if hdr.GetName() == QueryBytesFetchedHeaderName {
				for _, v := range hdr.Values {
					n, err := strconv.Atoi(v)
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
		Values: []string{strconv.Itoa(sumQueryBytesFetched(responses...))},
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
