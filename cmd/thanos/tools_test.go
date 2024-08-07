// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"os"
	"testing"

	"github.com/go-kit/log"

	"github.com/efficientgo/core/testutil"
)

func Test_CheckRules(t *testing.T) {
	validFiles := []string{
		"./testdata/rules-files/valid.yaml",
	}

	invalidFiles := [][]string{
		{"./testdata/rules-files/non-existing-file.yaml"},
		{"./testdata/rules-files/invalid-yaml-format.yaml"},
		{"./testdata/rules-files/invalid-rules-data.yaml"},
		{"./testdata/rules-files/invalid-unknown-field.yaml"},
	}

	logger := log.NewNopLogger()
	testutil.Ok(t, checkRulesFiles(logger, &validFiles))

	for _, fn := range invalidFiles {
		testutil.NotOk(t, checkRulesFiles(logger, &fn), "expected err for file %s", fn)
	}
}

func Test_CheckRules_Glob(t *testing.T) {
	// regex path
	files := &[]string{"./testdata/rules-files/valid*.yaml"}
	logger := log.NewNopLogger()
	testutil.Ok(t, checkRulesFiles(logger, files))

	// direct path
	files = &[]string{"./testdata/rules-files/valid.yaml"}
	testutil.Ok(t, checkRulesFiles(logger, files))

	// invalid path
	files = &[]string{"./testdata/rules-files/*.yamlaaa"}
	testutil.NotOk(t, checkRulesFiles(logger, files), "expected err for file %s", files)

	// Unreadble path
	files = &[]string{"./testdata/rules-files/unreadable_valid.yaml"}
	filename := (*files)[0]
	testutil.Ok(t, os.Chmod(filename, 0000), "failed to change file permissions of %s to 0000", filename)
	testutil.NotOk(t, checkRulesFiles(logger, files), "expected err for file %s", files)
	testutil.Ok(t, os.Chmod(filename, 0777), "failed to change file permissions of %s to 0777", filename)
}
