// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2eutil

import "syscall"

func SysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		// For linux only, kill this if the go test process dies before the cleanup.
		Pdeathsig: syscall.SIGKILL,
	}
}
