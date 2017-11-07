package main

import (
	"context"
	"net/url"
	"testing"

	"github.com/improbable-eng/promlts/pkg/testutil"
)

func TestSidecar_queryExternalLabels(t *testing.T) {
	p, err := testutil.NewPrometheus(":12347")
	testutil.Ok(t, err)

	err = p.SetConfig(`
global:
  external_labels:
    region: eu-west
    az: 1
`)
	testutil.Ok(t, err)

	u, err := url.Parse("http://localhost:12347/")
	testutil.Ok(t, err)

	testutil.Ok(t, p.Start())
	defer p.Stop()

	ext, err := queryExternalLabels(context.Background(), u)
	testutil.Ok(t, err)

	testutil.Equals(t, 2, len(ext))
	testutil.Equals(t, "eu-west", ext.Get("region"))
	testutil.Equals(t, "1", ext.Get("az"))
}
