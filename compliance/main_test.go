// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compliance

import (
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	flag.Parse()

	var skips []string
	skips = append(skips, senderSkips...)
	if existing := flag.Lookup("test.skip").Value.String(); existing != "" {
		skips = append([]string{existing}, skips...)
	}
	if len(skips) > 0 {
		_ = flag.Set("test.skip", strings.Join(skips, "|"))
	}

	os.Exit(m.Run())
}

func buildThanos(t *testing.T) string {
	t.Helper()

	bin := filepath.Join(t.TempDir(), "thanos")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	cmd := exec.CommandContext(t.Context(), "go", "build", "-tags=netgo,slicelabels", "-o", bin, "./cmd/thanos")
	cmd.Dir = ".."
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "failed to build thanos binary: %s", string(out))
	return bin
}
