// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2eutil

import "net"

// FreePort returns port that is free now.
func FreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	return l.Addr().(*net.TCPAddr).Port, l.Close()
}
