// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package querier

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/prometheus/scrape"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestMetadataHandler_Success(t *testing.T) {
	d := &MockDistributor{}
	d.On("MetricsMetadata", mock.Anything).Return(
		[]scrape.MetricMetadata{
			{Metric: "alertmanager_dispatcher_aggregation_groups", Help: "Number of active aggregation groups", Type: "gauge", Unit: ""},
		},
		nil)

	handler := MetadataHandler(d)

	request, err := http.NewRequest("GET", "/metadata", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Result().StatusCode)
	responseBody, err := ioutil.ReadAll(recorder.Result().Body)
	require.NoError(t, err)

	expectedJSON := `
	{
		"status": "success",
		"data": {
			"alertmanager_dispatcher_aggregation_groups": [
				{
					"help": "Number of active aggregation groups",
					"type": "gauge",
					"unit": ""
				}
			]
		}
	}
	`

	require.JSONEq(t, expectedJSON, string(responseBody))
}

func TestMetadataHandler_Error(t *testing.T) {
	d := &MockDistributor{}
	d.On("MetricsMetadata", mock.Anything).Return([]scrape.MetricMetadata{}, fmt.Errorf("no user id"))

	handler := MetadataHandler(d)

	request, err := http.NewRequest("GET", "/metadata", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)
	responseBody, err := ioutil.ReadAll(recorder.Result().Body)
	require.NoError(t, err)

	expectedJSON := `
	{
		"status": "error",
		"error": "no user id"
	}
	`

	require.JSONEq(t, expectedJSON, string(responseBody))
}
