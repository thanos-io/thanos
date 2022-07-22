// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package util

import "unsafe"

func YoloBuf(s string) []byte {
	return *((*[]byte)(unsafe.Pointer(&s)))
}
