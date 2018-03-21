package main

import (
	"context"
	"net/url"
	"testing"

	"fmt"

	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestSidecar_queryExternalLabels(t *testing.T) {
	p, err := testutil.NewPrometheus()
	testutil.Ok(t, err)

	err = p.SetConfig(`
global:
  external_labels:
    region: eu-west
    az: 1
`)
	testutil.Ok(t, err)

	testutil.Ok(t, p.Start())
	defer p.Stop()

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	ext, err := queryExternalLabels(context.Background(), u)
	testutil.Ok(t, err)

	testutil.Equals(t, 2, len(ext))
	testutil.Equals(t, "eu-west", ext.Get("region"))
	testutil.Equals(t, "1", ext.Get("az"))
}

func TestSidecar_queryIsCompactionEnabled_False(t *testing.T) {
	p, err := testutil.NewPrometheus()
	testutil.Ok(t, err)

	p.SetCompaction(false)

	testutil.Ok(t, p.Start())
	defer p.Stop()

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	isCompEnabled, noFlagEndpoint, err := queryIsCompactionEnabled(context.Background(), u)
	testutil.Ok(t, err)

	testutil.Assert(t, !isCompEnabled, "compaction is disabled.")
	testutil.Assert(t, !noFlagEndpoint, "flag endpoint exists.")
}

func TestSidecar_queryIsCompactionEnabled_True(t *testing.T) {
	p, err := testutil.NewPrometheus()
	testutil.Ok(t, err)

	p.SetCompaction(true)

	testutil.Ok(t, p.Start())
	defer p.Stop()

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	isCompEnabled, noFlagEndpoint, err := queryIsCompactionEnabled(context.Background(), u)
	testutil.Ok(t, err)

	testutil.Assert(t, isCompEnabled, "compaction is enabled.")
	testutil.Assert(t, !noFlagEndpoint, "flag endpoint exists.")
}
