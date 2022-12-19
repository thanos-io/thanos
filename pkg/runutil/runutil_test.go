// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package runutil

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/pkg/errors"
)

type testCloser struct {
	err error
}

func (c testCloser) Close() error {
	return c.err
}

func TestCloseWithErrCapture(t *testing.T) {
	for _, tcase := range []struct {
		err    error
		closer io.Closer

		expectedErrStr string
	}{
		{
			err:            nil,
			closer:         testCloser{err: nil},
			expectedErrStr: "",
		},
		{
			err:            errors.New("test"),
			closer:         testCloser{err: nil},
			expectedErrStr: "test",
		},
		{
			err:            nil,
			closer:         testCloser{err: errors.New("test")},
			expectedErrStr: "close: test",
		},
		{
			err:            errors.New("test"),
			closer:         testCloser{err: errors.New("test")},
			expectedErrStr: "2 errors: test; close: test",
		},
	} {
		if ok := t.Run("", func(t *testing.T) {
			ret := tcase.err
			CloseWithErrCapture(&ret, tcase.closer, "close")

			if tcase.expectedErrStr == "" {
				if ret != nil {
					t.Error("Expected error to be nil")
					t.Fail()
				}
			} else {
				if ret == nil {
					t.Error("Expected error to be not nil")
					t.Fail()
				}

				if tcase.expectedErrStr != ret.Error() {
					t.Errorf("%s != %s", tcase.expectedErrStr, ret.Error())
					t.Fail()
				}
			}

		}); !ok {
			return
		}
	}
}

type loggerCapturer struct {
	// WasCalled is true if the Log() function has been called.
	WasCalled bool
}

func (lc *loggerCapturer) Log(keyvals ...interface{}) error {
	lc.WasCalled = true
	return nil
}

type emulatedCloser struct {
	io.Reader

	calls int
}

func (e *emulatedCloser) Close() error {
	e.calls++
	if e.calls == 1 {
		return nil
	}
	if e.calls == 2 {
		return errors.Wrap(os.ErrClosed, "can even be a wrapped one")
	}
	return errors.New("something very bad happened")
}

// newEmulatedCloser returns a ReadCloser with a Close method
// that at first returns success but then returns that
// it has been closed already. After that, it returns that
// something very bad had happened.
func newEmulatedCloser(r io.Reader) io.ReadCloser {
	return &emulatedCloser{Reader: r}
}

func TestCloseMoreThanOnce(t *testing.T) {
	lc := &loggerCapturer{}
	r := newEmulatedCloser(strings.NewReader("somestring"))

	CloseWithLogOnErr(lc, r, "should not be called")
	CloseWithLogOnErr(lc, r, "should not be called")
	testutil.Equals(t, false, lc.WasCalled)

	CloseWithLogOnErr(lc, r, "should be called")
	testutil.Equals(t, true, lc.WasCalled)
}

func TestDeleteAll(t *testing.T) {
	dir := t.TempDir()

	f, err := os.Create(filepath.Join(dir, "file1"))
	testutil.Ok(t, err)
	testutil.Ok(t, f.Close())

	testutil.Ok(t, os.MkdirAll(filepath.Join(dir, "a"), os.ModePerm))
	testutil.Ok(t, os.MkdirAll(filepath.Join(dir, "b"), os.ModePerm))
	testutil.Ok(t, os.MkdirAll(filepath.Join(dir, "c", "innerc"), os.ModePerm))
	f, err = os.Create(filepath.Join(dir, "a", "file2"))
	testutil.Ok(t, err)
	testutil.Ok(t, f.Close())
	f, err = os.Create(filepath.Join(dir, "c", "file3"))
	testutil.Ok(t, err)
	testutil.Ok(t, f.Close())

	testutil.Ok(t, DeleteAll(dir, "file1", "a", filepath.Join("c", "innerc")))

	// Deleted.
	_, err = os.Stat(filepath.Join(dir, "file1"))
	testutil.Assert(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(dir, "b/"))
	testutil.Assert(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(dir, "file3"))
	testutil.Assert(t, os.IsNotExist(err))

	// Exists.
	_, err = os.Stat(filepath.Join(dir, "a", "file2"))
	testutil.Ok(t, err)
	_, err = os.Stat(filepath.Join(dir, "a/"))
	testutil.Ok(t, err)
	_, err = os.Stat(filepath.Join(dir, "c", "innerc"))
	testutil.Ok(t, err)
}

func TestDeleteAll_ShouldReturnNoErrorIfDirectoryDoesNotExists(t *testing.T) {
	dir := t.TempDir()
	testutil.Ok(t, os.RemoveAll(dir))

	// Calling DeleteAll() on a non-existent directory should return no error.
	testutil.Ok(t, DeleteAll(dir))
}
