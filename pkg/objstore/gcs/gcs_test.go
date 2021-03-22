// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package gcs

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/go-kit/kit/log"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestBucket_Get_ShouldReturnErrorIfServerTruncateResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
		w.Header().Set("Content-Length", "100")

		// Write less bytes than the content length.
		_, err := w.Write([]byte("12345"))
		testutil.Ok(t, err)
	}))
	defer srv.Close()

	os.Setenv("STORAGE_EMULATOR_HOST", srv.Listener.Addr().String())

	cfg := Config{
		Bucket:         "test-bucket",
		ServiceAccount: "",
	}

	bkt, err := NewBucketWithConfig(context.Background(), log.NewNopLogger(), cfg, "test")
	testutil.Ok(t, err)

	reader, err := bkt.Get(context.Background(), "test")
	testutil.Ok(t, err)

	// We expect an error when reading back.
	_, err = ioutil.ReadAll(reader)
	testutil.Equals(t, io.ErrUnexpectedEOF, err)
}
