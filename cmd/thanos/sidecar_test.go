package main

import (
	"context"
	"net/url"
	"testing"

	"fmt"

	"github.com/go-kit/kit/log"
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
	defer func() { testutil.Ok(t, p.Stop()) }()

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	ext, err := queryExternalLabels(context.Background(), log.NewNopLogger(), u)
	testutil.Ok(t, err)

	testutil.Equals(t, 2, len(ext))
	testutil.Equals(t, "eu-west", ext.Get("region"))
	testutil.Equals(t, "1", ext.Get("az"))
}
