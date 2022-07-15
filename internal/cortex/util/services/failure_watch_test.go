package services

import (
	"context"
	"errors"
	"testing"

	e2 "github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestNilServiceFailureWatcher(t *testing.T) {
	var w *FailureWatcher

	// prove it doesn't fail, but returns nil channel.
	require.Nil(t, w.Chan())
}

func TestServiceFailureWatcher(t *testing.T) {
	w := NewFailureWatcher()

	err := errors.New("this error doesn't end with dot")

	failing := NewBasicService(nil, nil, func(_ error) error {
		return err
	})

	w.WatchService(failing)

	require.NoError(t, failing.StartAsync(context.Background()))

	e := <-w.Chan()
	require.NotNil(t, e)
	require.Equal(t, err, e2.Cause(e))
}
