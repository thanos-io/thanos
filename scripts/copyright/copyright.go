// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type copyright []byte

var (
	// thanos compatible for Go and Proto files.
	thanos copyright = []byte(`// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

`)
	cortex copyright = []byte(`// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

`)
)

func applyLicenseToProtoAndGo() error {
	return filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Filter out stuff that does not need copyright.
		if info.IsDir() {
			switch path {
			case "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".pb.go") {
			return nil
		}
		if (filepath.Ext(path) != ".proto" && filepath.Ext(path) != ".go") ||
			// We copied this file, and we want to maintain its license (MIT).
			path == "pkg/testutil/testutil.go" ||
			// Generated file.
			path == "pkg/ui/bindata.go" {
			return nil
		}

		b, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return err
		}

		var cr copyright
		if strings.HasPrefix(path, "internal/cortex") {
			// We vendored in Cortex files, and we want to maintain its license
			cr = cortex
		} else {
			cr = thanos
		}
		if err := writeLicence(cr, path, b); err != nil {
			return err
		}
		return nil
	})
}

func writeLicence(cr copyright, path string, b []byte) error {
	if !strings.HasPrefix(string(b), string(cr)) {
		log.Println("file", path, "is missing Copyright header. Adding.")

		var bb bytes.Buffer
		_, _ = bb.Write(cr)
		_, _ = bb.Write(b)
		if err := os.WriteFile(path, bb.Bytes(), 0600); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if err := applyLicenseToProtoAndGo(); err != nil {
		log.Fatal(err)
	}
}
