package runutil

import (
	"io"
	"testing"

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
