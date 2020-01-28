// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func Test_checkRules(t *testing.T) {

	validFiles := []string{
		"./testdata/rules-files/valid.yaml",
	}

	invalidFiles := [][]string{
		[]string{"./testdata/rules-files/non-existing-file.yaml"},
		[]string{"./testdata/rules-files/invalid-yaml-format.yaml"},
		[]string{"./testdata/rules-files/invalid-rules-data.yaml"},
		[]string{"./testdata/rules-files/invalid-unknown-field.yaml"},
	}

	logger := log.NewNopLogger()

	testutil.Ok(t, checkRulesFiles(logger, &validFiles))

	for _, fn := range invalidFiles {
		testutil.NotOk(t, checkRulesFiles(logger, &fn))
	}
}
