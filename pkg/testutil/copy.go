package testutil

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
)

func Copy(t testing.TB, src, dst string) {
	Ok(t, copyRecursive(src, dst))
}

func copyRecursive(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dst, relPath), os.ModePerm)
		}

		if !info.Mode().IsRegular() {
			return errors.Errorf("%s is not a regular file", path)
		}

		source, err := os.Open(path)
		if err != nil {
			return err
		}
		defer source.Close()

		destination, err := os.Create(filepath.Join(dst, relPath))
		if err != nil {
			return err
		}
		defer destination.Close()
		_, err = io.Copy(destination, source)
		return err
	})
}
