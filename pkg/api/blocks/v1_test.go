// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	baseAPI "github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestMain(m *testing.M) {
	testutil.TolerantVerifyLeakMain(m)
}

type endpointTestCase struct {
	endpoint baseAPI.ApiFunc
	params   map[string]string
	query    url.Values
	method   string
	response interface{}
	errType  baseAPI.ErrorType
}
type responeCompareFunction func(interface{}, interface{}) bool

func testEndpoint(t *testing.T, test endpointTestCase, name string, responseCompareFunc responeCompareFunction) bool {
	return t.Run(name, func(t *testing.T) {
		// Build a context with the correct request params.
		ctx := context.Background()
		for p, v := range test.params {
			ctx = route.WithParam(ctx, p, v)
		}

		reqURL := "http://example.com"
		params := test.query.Encode()

		var body io.Reader
		if test.method == http.MethodPost {
			body = strings.NewReader(params)
		} else if test.method == "" {
			test.method = "ANY"
			reqURL += "?" + params
		}

		req, err := http.NewRequest(test.method, reqURL, body)
		if err != nil {
			t.Fatal(err)
		}

		if body != nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}

		resp, _, apiErr, releaseResources := test.endpoint(req.WithContext(ctx))
		defer releaseResources()
		if apiErr != nil {
			if test.errType == baseAPI.ErrorNone {
				t.Fatalf("Unexpected error: %s", apiErr)
			}
			if test.errType != apiErr.Typ {
				t.Fatalf("Expected error of type %q but got type %q", test.errType, apiErr.Typ)
			}
			return
		}
		if test.errType != baseAPI.ErrorNone {
			t.Fatalf("Expected error of type %q but got none", test.errType)
		}

		if !responseCompareFunc(resp, test.response) {
			t.Fatalf("Response does not match, expected:\n%+v\ngot:\n%+v", test.response, resp)
		}
	})
}

func TestMarkBlockEndpoint(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// create block
	b1, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "b", Value: "1"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "val1"}}, 124, metadata.NoneFunc)
	testutil.Ok(t, err)

	// upload block
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	logger := log.NewNopLogger()
	testutil.Ok(t, block.Upload(ctx, logger, bkt, path.Join(tmpDir, b1.String()), metadata.NoneFunc))

	now := time.Now()
	api := &BlocksAPI{
		baseAPI: &baseAPI.BaseAPI{
			Now: func() time.Time { return now },
		},
		logger: logger,
		globalBlocksInfo: &BlocksInfo{
			Blocks: []metadata.Meta{},
			Label:  "foo",
		},
		loadedBlocksInfo: &BlocksInfo{
			Blocks: []metadata.Meta{},
			Label:  "foo",
		},
		disableCORS: true,
		bkt:         bkt,
	}

	var tests = []endpointTestCase{
		// Empty ID
		{
			endpoint: api.markBlock,
			query: url.Values{
				"id": []string{""},
			},
			errType: baseAPI.ErrorBadData,
		},
		// Empty action
		{
			endpoint: api.markBlock,
			query: url.Values{
				"id":     []string{ulid.MustNew(1, nil).String()},
				"action": []string{""},
			},
			errType: baseAPI.ErrorBadData,
		},
		// invalid ULID
		{
			endpoint: api.markBlock,
			query: url.Values{
				"id":     []string{"invalid_id"},
				"action": []string{"DELETION"},
			},
			errType: baseAPI.ErrorBadData,
		},
		// invalid action
		{
			endpoint: api.markBlock,
			query: url.Values{
				"id":     []string{ulid.MustNew(2, nil).String()},
				"action": []string{"INVALID_ACTION"},
			},
			errType: baseAPI.ErrorBadData,
		},
		{
			endpoint: api.markBlock,
			query: url.Values{
				"id":     []string{b1.String()},
				"action": []string{"DELETION"},
			},
			response: nil,
		},
	}

	for i, test := range tests {
		if ok := testEndpoint(t, test, fmt.Sprintf("#%d %s", i, test.query.Encode()), reflect.DeepEqual); !ok {
			return
		}
	}

	file := path.Join(tmpDir, b1.String())
	_, err = os.Stat(file)
	testutil.Ok(t, err)
}
