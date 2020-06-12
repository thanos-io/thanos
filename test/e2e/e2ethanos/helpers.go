// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ethanos

import (
	"os/exec"
	"testing"

	"github.com/cortexproject/cortex/integration/e2e"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func CleanScenario(t *testing.T, s *e2e.Scenario) func() {
	return func() {
		// Otherwise close will fail.
		out, err := exec.Command("chmod", "-R", "777", s.SharedDir()).CombinedOutput()
		t.Log("command out", string(out))
		testutil.Ok(t, err)
		s.Close()
	}
}
