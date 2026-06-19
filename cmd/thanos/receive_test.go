// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/relabel"
)

func Test_parseRelabelConfig(t *testing.T) {
	t.Run("empty content returns no config", func(t *testing.T) {
		global, perTenant, err := parseRelabelConfig(nil)
		testutil.Ok(t, err)
		testutil.Assert(t, global == nil, "expected nil global config")
		testutil.Assert(t, perTenant == nil, "expected nil per-tenant config")

		global, perTenant, err = parseRelabelConfig([]byte(""))
		testutil.Ok(t, err)
		testutil.Assert(t, global == nil, "expected nil global config")
		testutil.Assert(t, perTenant == nil, "expected nil per-tenant config")
	})

	t.Run("global list format", func(t *testing.T) {
		content := []byte(`
- source_labels: [__name__]
  action: drop
  regex: "global_drop_.*"
`)
		global, perTenant, err := parseRelabelConfig(content)
		testutil.Ok(t, err)
		testutil.Assert(t, perTenant == nil, "expected nil per-tenant config for global format")
		testutil.Equals(t, 1, len(global))
		testutil.Equals(t, relabel.Drop, global[0].Action)
		testutil.Equals(t, model.LabelNames{"__name__"}, global[0].SourceLabels)
	})

	t.Run("per-tenant map format", func(t *testing.T) {
		content := []byte(`
tenant-a:
  - source_labels: [__name__]
    action: drop
    regex: "tenant_a_drop_.*"
tenant-b:
  - source_labels: [__name__]
    action: keep
    regex: "tenant_b_keep_.*"
`)
		global, perTenant, err := parseRelabelConfig(content)
		testutil.Ok(t, err)
		testutil.Assert(t, global == nil, "expected nil global config for per-tenant format")
		testutil.Equals(t, 2, len(perTenant))
		testutil.Equals(t, 1, len(perTenant["tenant-a"]))
		testutil.Equals(t, relabel.Drop, perTenant["tenant-a"][0].Action)
		testutil.Equals(t, relabel.Keep, perTenant["tenant-b"][0].Action)
	})

	t.Run("invalid global config fails validation", func(t *testing.T) {
		// hashmod with no modulus is invalid.
		content := []byte(`
- action: hashmod
  source_labels: [__name__]
  target_label: shard
`)
		_, _, err := parseRelabelConfig(content)
		testutil.NotOk(t, err)
	})

	t.Run("invalid per-tenant config fails validation", func(t *testing.T) {
		content := []byte(`
tenant-a:
  - action: hashmod
    source_labels: [__name__]
    target_label: shard
`)
		_, _, err := parseRelabelConfig(content)
		testutil.NotOk(t, err)
	})

	t.Run("unknown action is rejected", func(t *testing.T) {
		content := []byte(`
- action: bogus
  source_labels: [__name__]
`)
		_, _, err := parseRelabelConfig(content)
		testutil.NotOk(t, err)
	})

	t.Run("malformed yaml is rejected", func(t *testing.T) {
		content := []byte(`::: not valid yaml :::`)
		_, _, err := parseRelabelConfig(content)
		testutil.NotOk(t, err)
	})
}
