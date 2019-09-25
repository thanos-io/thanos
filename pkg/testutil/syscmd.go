// +build !linux

package testutil

import (
	"os/exec"
)

// SetSysProcAttr changes cmd.SysProcAttr as needed to run and cleanup test commands.
func SetSysProcAttr(cmd *exec.Cmd) {}
