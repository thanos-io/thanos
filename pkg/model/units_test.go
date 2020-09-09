// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"testing"

	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestBytes_Marshaling(t *testing.T) {
	value := Bytes(1048576)

	encoded, err := yaml.Marshal(&value)
	testutil.Ok(t, err)
	testutil.Equals(t, "1MiB\n", string(encoded))

	var decoded Bytes
	err = yaml.Unmarshal(encoded, &decoded)
	testutil.Equals(t, value, decoded)
	testutil.Ok(t, err)
}
