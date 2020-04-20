// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestStore_ParseRelabelConfig(t *testing.T) {
	_, err := parseRelabelConfig([]byte(`
    - action: drop
      regex: "A"
      source_labels:
      - cluster
    `))
	testutil.Ok(t, err)

	_, err = parseRelabelConfig([]byte(`
    - action: labelmap
      regex: "A"
    `))
	testutil.NotOk(t, err)
	testutil.Equals(t, "unsupported relabel action: labelmap", err.Error())
}
