// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestHashSmoke(t *testing.T) {
	dir, err := ioutil.TempDir("", "testhash")
	testutil.Ok(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	f, err := ioutil.TempFile(dir, "hash")
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
