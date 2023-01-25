// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestPrometheus_Rules_e2e(t *testing.T) {
	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	curr, err := os.Getwd()
	testutil.Ok(t, err)
	root := filepath.Join(curr, "../../")

	p.SetConfig(fmt.Sprintf(`
global:
  external_labels:
    region: eu-west

rule_files:
  - %s/examples/alerts/alerts.yaml
  - %s/examples/alerts/rules.yaml
`, root, root))
	testutil.Ok(t, p.Start())

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	promRules := NewPrometheus(u, promclient.NewDefaultClient(), func() labels.Labels {
		return labels.FromStrings("replica", "test1")
	})
	testRulesAgainstExamples(t, filepath.Join(root, "examples/alerts"), promRules)
}
