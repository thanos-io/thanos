package s3

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestUserAgent(t *testing.T) {

	serverResponse := `<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`

	var receivedUserAgent = ""
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedUserAgent = r.Header.Get("User-Agent")
		w.Header().Add("Last-Modified", "Sun, 15 Apr 2018 20:26:05 GMT")
		fmt.Fprintln(w, serverResponse)
	}))

	s3URL, _ := url.ParseRequestURI(api.URL)

	defer api.Close()

	var s3Config Config

	s3Config.Bucket = "testing"
	s3Config.Endpoint = s3URL.Host
	s3Config.Insecure = true

	bucket, err := NewBucket(&s3Config, nil)

	ctx, cancel := context.WithCancel(context.Background())
	bucketExists, err := bucket.Exists(ctx, "test_obj")

	if err != nil {
		t.Fatal(err)
		cancel()
	}
	if !bucketExists {
		t.Error("Test bucket doesn't exist")
	}
	if !strings.Contains(receivedUserAgent, "Thanos") {
		t.Error("Didn't receive proper user agent string from client")
	}

}
