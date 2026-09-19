// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package ui

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/common/route"

	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
)

func TestSanitizePrefix(t *testing.T) {
	type args struct {
		prefix string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			"InvalidEscaping",
			args{
				prefix: "/%%",
			},
			"",
			true,
		},
		{
			"URL",
			args{
				prefix: "http://www.example.com/some%20path/two?foo=bar?foo1=bar1#id",
			},
			"/some path/two",
			false,
		},
		{
			"DelimiterNotAllowed",
			args{
				prefix: "http://www.example.com/host%3A%2F%2Fpath/",
			},
			"/host:/path",
			false,
		},
		{
			"EmptyPrefix",
			args{
				prefix: "",
			},
			"",
			false,
		},
		{
			"Root",
			args{
				prefix: "/",
			},
			"",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SanitizePrefix(tt.args.prefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("SanitizePrefix() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SanitizePrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestServeStaticAsset ensures that assets requested through the "/static/*filepath"
// route are resolved from the embedded file system. Embedded paths always use forward
// slashes, so building the lookup path must not depend on the OS path separator.
func TestServeStaticAsset(t *testing.T) {
	const staticRoot = "static/react/static"

	// Asset file names contain content hashes, so discover a real one instead of
	// hard-coding it.
	var asset string
	testutil.Ok(t, fs.WalkDir(reactUI, staticRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if asset == "" && !d.IsDir() {
			asset = p
		}
		return nil
	}))
	if asset == "" {
		t.Fatalf("no embedded assets found under %q", staticRoot)
	}

	want, err := fs.ReadFile(reactUI, asset)
	testutil.Ok(t, err)

	r := route.New()
	registerReactApp(r, extpromhttp.NewNopInstrumentationMiddleware(), &BaseUI{logger: log.NewNopLogger()})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/static/"+strings.TrimPrefix(asset, staticRoot+"/"), nil)
	r.ServeHTTP(rec, req)

	testutil.Equals(t, http.StatusOK, rec.Code)
	testutil.Equals(t, want, rec.Body.Bytes())
}
