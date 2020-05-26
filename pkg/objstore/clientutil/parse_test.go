// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package clientutil

import (
	"net/http"
	"testing"
	"time"

	alioss "github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestParseLastModified(t *testing.T) {
	tests := map[string]struct {
		headerValue string
		expectedVal time.Time
		expectedErr string
	}{
		"no header": {
			expectedErr: "Last-Modified header not found",
		},
		"invalid header value": {
			headerValue: "invalid",
			expectedErr: `parse Last-Modified: parsing time "invalid" as "2006-01-02T15:04:05Z07:00": cannot parse "invalid" as "2006"`,
		},
		"valid header value": {
			headerValue: "2015-11-06T10:07:11.000Z",
			expectedVal: time.Date(2015, time.November, 6, 10, 7, 11, 0, time.UTC),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			meta := http.Header{}
			if testData.headerValue != "" {
				meta.Add(alioss.HTTPHeaderLastModified, testData.headerValue)
			}

			actual, err := ParseLastModified(meta)

			if testData.expectedErr != "" {
				testutil.NotOk(t, err)
				testutil.Equals(t, testData.expectedErr, err.Error())
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, testData.expectedVal, actual)
			}
		})
	}
}

func TestParseContentLength(t *testing.T) {
	tests := map[string]struct {
		headerValue string
		expectedVal int64
		expectedErr string
	}{
		"no header": {
			expectedErr: "Content-Length header not found",
		},
		"invalid header value": {
			headerValue: "invalid",
			expectedErr: `convert Content-Length: strconv.ParseInt: parsing "invalid": invalid syntax`,
		},
		"valid header value": {
			headerValue: "12345",
			expectedVal: 12345,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			meta := http.Header{}
			if testData.headerValue != "" {
				meta.Add(alioss.HTTPHeaderContentLength, testData.headerValue)
			}

			actual, err := ParseContentLength(meta)

			if testData.expectedErr != "" {
				testutil.NotOk(t, err)
				testutil.Equals(t, testData.expectedErr, err.Error())
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, testData.expectedVal, actual)
			}
		})
	}
}
