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
	location, _ := time.LoadLocation("GMT")
	tests := map[string]struct {
		headerValue string
		expectedVal time.Time
		expectedErr string
		format      string
	}{
		"no header": {
			expectedErr: "Last-Modified header not found",
		},
		"empty format string to default RFC3339 format": {
			headerValue: "2015-11-06T10:07:11.000Z",
			expectedVal: time.Date(2015, time.November, 6, 10, 7, 11, 0, time.UTC),
			format:      "",
		},
		"valid RFC3339 header value": {
			headerValue: "2015-11-06T10:07:11.000Z",
			expectedVal: time.Date(2015, time.November, 6, 10, 7, 11, 0, time.UTC),
			format:      time.RFC3339,
		},
		"invalid RFC3339 header value": {
			headerValue: "invalid",
			expectedErr: `parse Last-Modified: parsing time "invalid" as "2006-01-02T15:04:05Z07:00": cannot parse "invalid" as "2006"`,
			format:      time.RFC3339,
		},
		"valid RFC1123 header value": {
			headerValue: "Fri, 24 Feb 2012 06:07:48 GMT",
			expectedVal: time.Date(2012, time.February, 24, 6, 7, 48, 0, location),
			format:      time.RFC1123,
		},
		"invalid RFC1123 header value": {
			headerValue: "invalid",
			expectedErr: `parse Last-Modified: parsing time "invalid" as "Mon, 02 Jan 2006 15:04:05 MST": cannot parse "invalid" as "Mon"`,
			format:      time.RFC1123,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			meta := http.Header{}
			if testData.headerValue != "" {
				meta.Add(alioss.HTTPHeaderLastModified, testData.headerValue)
			}

			actual, err := ParseLastModified(meta, testData.format)

			if testData.expectedErr != "" {
				testutil.NotOk(t, err)
				testutil.Equals(t, testData.expectedErr, err.Error())
			} else {
				testutil.Ok(t, err)
				testutil.Assert(t, testData.expectedVal.Equal(actual))
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
