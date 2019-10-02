package testutil

import "syscall"

func SysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		// For linux only, kill this if the go test process dies before the cleanup.
		Pdeathsig: syscall.SIGKILL,
	}
}
