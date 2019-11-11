package util

import (
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestRungroupRecover(t *testing.T) {
	var r run.Group
	logger := log.NewNopLogger()

	r.Add(RecoverGoroutine(logger, func() error {
		panic("test")
	}), func(err error) {})

	err := r.Run()

	testutil.Equals(t, err.Error(), "goroutine encountered test")
}
