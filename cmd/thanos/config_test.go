// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/efficientgo/core/testutil"
	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/prometheus/prometheus/model/relabel"

	"github.com/thanos-io/thanos/pkg/block"
)

func newTestRelabelCfg(t *testing.T, args ...string) *relabelCfg {
	t.Helper()

	app := kingpin.New("test", "")
	cfg := &relabelCfg{extflag.RegisterPathOrContent(app, "relabel-config", "relabel configuration.")}
	_, err := app.Parse(args)
	testutil.Ok(t, err)
	return cfg
}

func TestRelabelCfg_RelabelConfig(t *testing.T) {
	t.Run("parses content", func(t *testing.T) {
		cfg := newTestRelabelCfg(t, "--relabel-config=- action: drop\n  source_labels: [__name__]\n  regex: a")
		relabelConfig, err := cfg.RelabelConfig(block.SelectorSupportedRelabelActions)
		testutil.Ok(t, err)
		testutil.Equals(t, 1, len(relabelConfig))
		testutil.Equals(t, relabel.Drop, relabelConfig[0].Action)
	})

	t.Run("fails on content error", func(t *testing.T) {
		cfg := newTestRelabelCfg(t, "--relabel-config-file="+filepath.Join(t.TempDir(), "missing.yaml"))
		_, err := cfg.RelabelConfig(nil)
		testutil.NotOk(t, err)
	})
}

func TestRelabelCfg_RelabelConfigWithTenants(t *testing.T) {
	t.Run("parses per-tenant file", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "relabel.yaml")
		testutil.Ok(t, os.WriteFile(file, []byte(`
default:
  - action: drop
    source_labels: [__name__]
    regex: default_drop
tenant-a:
  - action: keep
    source_labels: [__name__]
    regex: tenant_a_keep
`), 0o600))

		cfg := newTestRelabelCfg(t, "--relabel-config-file="+file)
		defaultCfgs, perTenant, err := cfg.RelabelConfigWithTenants(nil)
		testutil.Ok(t, err)
		testutil.Equals(t, 1, len(defaultCfgs))
		testutil.Equals(t, relabel.Drop, defaultCfgs[0].Action)
		testutil.Equals(t, 1, len(perTenant))
		testutil.Equals(t, relabel.Keep, perTenant["tenant-a"][0].Action)
	})

	t.Run("fails on content error", func(t *testing.T) {
		cfg := newTestRelabelCfg(t, "--relabel-config-file="+filepath.Join(t.TempDir(), "missing.yaml"))
		_, _, err := cfg.RelabelConfigWithTenants(nil)
		testutil.NotOk(t, err)
	})
}
