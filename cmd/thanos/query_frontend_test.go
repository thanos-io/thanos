// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"net/http"
	"testing"

	"github.com/efficientgo/core/testutil"
)

func Test_extractOrgId(t *testing.T) {
	var testData = []struct {
		configuredHeaders []string
		requestHeaders    map[string]string
		expectOrgId       string
	}{
		{
			configuredHeaders: []string{"x-grafana-user", "x-server-id"},
			requestHeaders:    map[string]string{"x-grafana-user": "imagrafanauser", "x-server-id": "iamserverid"},
			expectOrgId:       "imagrafanauser",
		},
		{
			configuredHeaders: []string{"x-server-id", "x-grafana-user"},
			requestHeaders:    map[string]string{"x-grafana-user": "imagrafanauser", "x-server-id": "iamserverid"},
			expectOrgId:       "iamserverid",
		},
		{
			configuredHeaders: []string{},
			requestHeaders:    map[string]string{"another-header": "another"},
			expectOrgId:       "anonymous",
		},
	}
	for _, data := range testData {
		config := queryFrontendConfig{
			orgIdHeaders: data.configuredHeaders,
		}
		req, _ := http.NewRequest("", "", nil)
		for k, v := range data.requestHeaders {
			req.Header.Set(k, v)
		}
		id := extractOrgId(&config, req)
		testutil.Equals(t, data.expectOrgId, id)
	}
}
