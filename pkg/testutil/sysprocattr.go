// +build !linux

package testutil

import "syscall"

func SysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{}
}
