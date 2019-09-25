package testutil

import (
	"os/exec"
	"syscall"
)

// SetSysProcAttr changes cmd.SysProcAttr as needed to run and cleanup test commands.
func SetSysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		// For linux only, kill this if the go test process dies before the cleanup.
		Pdeathsig: syscall.SIGKILL,
	}
}
