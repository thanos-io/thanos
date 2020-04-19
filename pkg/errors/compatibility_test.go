package errors

import (
	goerr "errors"
	"fmt"
	"testing"

	// TODO(bwplotka): When in need to export this package, make sure to copy testuils.
	"github.com/thanos-io/thanos/pkg/testutil"
)

// --- Go113+ ---
// https://golang.org/pkg/errors/#pkg-index

func TestGoNewAndCause(t *testing.T) {
	testNewAndCause(t, func(format string, args ...interface{}) error {
		return goerr.New(fmt.Sprintf(format, args...))
	})
}

func TestGoErrorfAndCause(t *testing.T) {
	testNewAndCause(t, fmt.Errorf)
}

func TestGoWrapAndCause(t *testing.T) {
	// This checks if wrapping
	testWrapAndCause(t, func(err error, format string, args ...interface{}) error {
		// Wrapping nil has different semantics.
		if err == nil {
			err = NilErrorWrappedErr
		}

		return fmt.Errorf(format+": %w", append(append([]interface{}{}, args...), err)...)
	})
}

type someErr struct {
	message string
}

func (e *someErr) Error() string { return e.message }

func TestGoAs(t *testing.T) {
	{
		err := New("err1")
		want := &someErr{}
		testutil.Equals(t, false, goerr.As(err, &want))
		testutil.Equals(t, &someErr{}, want)
	}
	{
		err := New("err2")
		want := &errFrame{}
		testutil.Equals(t, true, goerr.As(err, &want))
		testutil.Equals(t, err, want)
	}
	{
		err := Wrap(New("err3"), "wrap")
		want := &someErr{}
		testutil.Equals(t, false, goerr.As(err, &want))
		testutil.Equals(t, &someErr{}, want)
	}
	{
		err := Wrap(New("err4"), "wrap")
		want := &errFrame{}
		testutil.Equals(t, true, goerr.As(err, &want))
		testutil.Equals(t, err, want)
	}
	{
		err := Wrap(Wrap(Wrap(&someErr{message: "someErr1"}, "wrap"), "wrap2"), "wrap3")
		want := &someErr{}
		testutil.Equals(t, true, goerr.As(err, &want))
		testutil.Equals(t, &someErr{message: "someErr1"}, want)
	}
	{
		err := Wrap(Wrap(Wrap(&someErr{message: "someErr1"}, "wrap"), "wrap2"), "wrap3")
		want := &errFrame{}
		testutil.Equals(t, true, goerr.As(err, &want))
		testutil.Equals(t, err, want)
	}
}

func TestGoIs(t *testing.T) {
	{
		err := New("err1")
		want := &someErr{}
		testutil.Equals(t, false, goerr.Is(err, want))
	}
	{
		err := New("err2")
		want := New("not err2")
		testutil.Equals(t, false, goerr.Is(err, want))
	}
	{
		err := New("err2")
		want := New("err2")
		testutil.Equals(t, false, goerr.Is(err, want))
	}
	{
		err := New("err2")
		want := err
		testutil.Equals(t, true, goerr.Is(err, want))
	}
	{
		err := Wrap(New("err3"), "wrap")
		want := &someErr{}
		testutil.Equals(t, false, goerr.Is(err, want))
	}
	{
		err := Wrap(New("err4"), "wrap")
		want := New("err4")
		testutil.Equals(t, false, goerr.Is(err, want))
	}
	{
		inner := New("err4")
		err := Wrap(inner, "wrap")
		want := inner
		testutil.Equals(t, true, goerr.Is(err, want))
	}
	{
		err := Wrap(Wrap(Wrap(&someErr{message: "someErr1"}, "wrap"), "wrap2"), "wrap3")
		want := &someErr{message: "someErr1"}
		testutil.Equals(t, false, goerr.Is(err, want))
	}
	{
		inner := &someErr{message: "someErr1"}
		err := Wrap(Wrap(Wrap(inner, "wrap"), "wrap2"), "wrap3")
		want := inner
		testutil.Equals(t, true, goerr.Is(err, want))
	}
	{
		middle := Wrap(&someErr{message: "someErr1"}, "wrap")
		err := Wrap(Wrap(middle, "wrap2"), "wrap3")
		want := middle
		testutil.Equals(t, true, goerr.Is(err, want))
	}
}

// Critical functions from pkg/error

func TestPkgGoCause(t *testing.T) {

}
