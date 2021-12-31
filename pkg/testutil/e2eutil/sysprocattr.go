// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

//go:build !linux
// +build !linux

package e2eutil

import "syscall"

func SysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{}
}
