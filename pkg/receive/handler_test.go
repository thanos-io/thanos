package receive

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
)

func TestCountCause(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		f    func(error) bool
		out  int
	}{
		{
			name: "nil",
			f:    isConflict,
			out:  0,
		},
		{
			name: "nil multierror",
			err:  terrors.MultiError([]error{}),
			f:    isConflict,
			out:  0,
		},
		{
			name: "matching nil",
			f:    func(err error) bool { return err == nil },
			out:  1,
		},
		{
			name: "matching simple",
			err:  conflictErr,
			f:    isConflict,
			out:  1,
		},
		{
			name: "non-matching multierror",
			err: terrors.MultiError([]error{
				errors.New("foo"),
				errors.New("bar"),
			}),
			f:   isConflict,
			out: 0,
		},
		{
			name: "nested non-matching multierror",
			err: errors.Wrap(terrors.MultiError([]error{
				errors.New("foo"),
				errors.New("bar"),
			}), "baz"),
			f:   isConflict,
			out: 0,
		},
		{
			name: "deep nested non-matching multierror",
			err: errors.Wrap(terrors.MultiError([]error{
				errors.New("foo"),
				terrors.MultiError([]error{
					errors.New("bar"),
					errors.New("qux"),
				}),
			}), "baz"),
			f:   isConflict,
			out: 0,
		},
		{
			name: "matching multierror",
			err: terrors.MultiError([]error{
				storage.ErrOutOfOrderSample,
				errors.New("foo"),
				errors.New("bar"),
			}),
			f:   isConflict,
			out: 1,
		},
		{
			name: "matching multierror many",
			err: terrors.MultiError([]error{
				storage.ErrOutOfOrderSample,
				conflictErr,
				errors.New(strconv.Itoa(http.StatusConflict)),
				errors.New("foo"),
				errors.New("bar"),
			}),
			f:   isConflict,
			out: 3,
		},
		{
			name: "nested matching multierror",
			err: errors.Wrap(terrors.MultiError([]error{
				storage.ErrOutOfOrderSample,
				errors.New("foo"),
				errors.New("bar"),
			}), "baz"),
			f:   isConflict,
			out: 0,
		},
		{
			name: "deep nested matching multierror",
			err: errors.Wrap(terrors.MultiError([]error{
				terrors.MultiError([]error{
					errors.New("qux"),
					errors.New(strconv.Itoa(http.StatusConflict)),
				}),
				errors.New("foo"),
				errors.New("bar"),
			}), "baz"),
			f:   isConflict,
			out: 0,
		},
	} {
		if n := countCause(tc.err, tc.f); n != tc.out {
			t.Errorf("test case %s: expected %d, got %d", tc.name, tc.out, n)
		}
	}
}
