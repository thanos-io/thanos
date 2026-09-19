// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"os"
	"testing"

	"github.com/go-kit/log"

	"github.com/efficientgo/core/testutil"
)

func TestHashSmoke(t *testing.T) {
	dir := t.TempDir()
	f, err := os.CreateTemp(dir, "hash")
	testutil.Ok(t, err)

	_, err = f.Write([]byte("test"))
	testutil.Ok(t, err)

	exp := ObjectHash{Func: SHA256Func, Value: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"}
	h, err := CalculateHash(f.Name(), SHA256Func, log.NewNopLogger())
	testutil.Ok(t, err)
	testutil.Equals(t, exp, h)

	_, err = CalculateHash(f.Name(), NoneFunc, log.NewNopLogger())
	testutil.NotOk(t, err)
}
