// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryBytesFetchedPrometheusResponseHeaders(t *testing.T) {
	resp1 := PrometheusResponse{Headers: []*PrometheusResponseHeader{&PrometheusResponseHeader{Name: "M3-Fetched-Bytes-Estimate", Values: []string{"100"}}}}
	resp2 := PrometheusResponse{Headers: []*PrometheusResponseHeader{&PrometheusResponseHeader{Name: "M3-Fetched-Bytes-Estimate", Values: []string{"1000"}}}}
	resp3 := PrometheusResponse{}
	hdrs := QueryBytesFetchedPrometheusResponseHeaders(&resp1, &resp2, &resp3)
	expected := []*PrometheusResponseHeader{{Name: QueryBytesFetchedHeaderName,
		Values: []string{"1100"}}}
	require.Equal(t, hdrs, expected)
}
