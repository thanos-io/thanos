package s3

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestUserAgent(t *testing.T) {

	const serverResponse = `<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`

	var receivedUserAgent string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedUserAgent = r.Header.Get("User-Agent")
		// Header required by minio-go, otherwise it fails with "Last-Modified time format is invalid"
		w.Header().Add("Last-Modified", "Sun, 15 Apr 2018 20:26:05 GMT")
		fmt.Fprintln(w, serverResponse)
	}))

	s3URL, _ := url.ParseRequestURI(api.URL)

	defer api.Close()

	s3Config := Config{
		Bucket:   "testing",
		Endpoint: s3URL.Host,
		Insecure: true,
	}

	bucket, err := NewBucket(&s3Config, nil, "test-component")
	testutil.Ok(t, err)

	bucketExists, err := bucket.Exists(context.Background(), "test_obj")
	testutil.Ok(t, err)
	testutil.Assert(t, bucketExists, "Couldn't get test bucket")
	testutil.Assert(t, strings.Contains(receivedUserAgent, "thanos-test-component"), "Didn't receive proper user agent string from client")
}
