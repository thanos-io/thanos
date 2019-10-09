package receive

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
)

func TestHasCause(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		f    func(error) bool
		out  bool
	}{
		{
			name: "nil",
			f:    isConflict,
			out:  false,
		},
		{
			name: "nil multierror",
			err:  terrors.MultiError([]error{}),
			f:    isConflict,
			out:  false,
		},
		{
			name: "non-matching multierror",
			err: terrors.MultiError([]error{
				errors.New("foo"),
				errors.New("bar"),
			}),
			f:   isConflict,
			out: false,
		},
		{
			name: "nested non-matching multierror",
			err: errors.Wrap(terrors.MultiError([]error{
				errors.New("foo"),
				errors.New("bar"),
			}), "baz"),
			f:   isConflict,
			out: false,
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
			out: false,
		},
		{
			name: "matching multierror",
			err: terrors.MultiError([]error{
				storage.ErrOutOfOrderSample,
				errors.New("foo"),
				errors.New("bar"),
			}),
			f:   isConflict,
			out: true,
		},
		{
			name: "nested matching multierror",
			err: errors.Wrap(terrors.MultiError([]error{
				storage.ErrOutOfOrderSample,
				errors.New("foo"),
				errors.New("bar"),
			}), "baz"),
			f:   isConflict,
			out: true,
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
			out: true,
		},
	} {
		if hasCause(tc.err, tc.f) != tc.out {
			t.Errorf("test case %s: expected %t, got %t", tc.name, tc.out, !tc.out)
		}
	}
}
