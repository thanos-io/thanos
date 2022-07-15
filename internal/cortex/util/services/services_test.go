package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestIdleService(t *testing.T) {
	t.Parallel()

	started := false
	stopped := false

	s := NewIdleService(func(ctx context.Context) error {
		started = true
		return nil
	}, func(_ error) error {
		stopped = true
		return nil
	})
	defer s.StopAsync()

	require.False(t, started)
	require.Equal(t, New, s.State())
	require.NoError(t, s.StartAsync(context.Background()))
	require.Error(t, s.StartAsync(context.Background())) // cannot start twice
	require.NoError(t, s.AwaitRunning(context.Background()))
	require.True(t, started)
	require.False(t, stopped)
	require.Equal(t, Running, s.State())
	s.StopAsync()
	require.NoError(t, s.AwaitTerminated(context.Background()))
	require.True(t, stopped)
}

func TestTimerService(t *testing.T) {
	t.Parallel()

	var iterations atomic.Uint64

	s := NewTimerService(100*time.Millisecond, nil, func(ctx context.Context) error {
		iterations.Inc()
		return nil
	}, nil)
	defer s.StopAsync()

	require.Equal(t, New, s.State())
	require.NoError(t, s.StartAsync(context.Background()))
	require.Error(t, s.StartAsync(context.Background()))
	require.NoError(t, s.AwaitRunning(context.Background()))
	require.Equal(t, Running, s.State())

	time.Sleep(1 * time.Second)

	val := iterations.Load()
	require.NotZero(t, val) // we should observe some iterations now

	s.StopAsync()
	require.NoError(t, s.AwaitTerminated(context.Background()))
	require.Equal(t, Terminated, s.State())
}

func TestHelperFunctionsNoError(t *testing.T) {
	t.Parallel()

	s := NewIdleService(nil, nil)
	require.NoError(t, StartAndAwaitRunning(context.Background(), s))
	require.NoError(t, StopAndAwaitTerminated(context.Background(), s))
}

func TestHelperFunctionsStartError(t *testing.T) {
	t.Parallel()

	e := errors.New("some error")
	s := NewIdleService(func(serviceContext context.Context) error { return e }, nil)

	require.Equal(t, e, StartAndAwaitRunning(context.Background(), s))
	require.Equal(t, e, StopAndAwaitTerminated(context.Background(), s))
}

func TestHelperFunctionsStartTooSlow(t *testing.T) {
	t.Parallel()

	s := NewIdleService(func(serviceContext context.Context) error {
		// ignores context
		time.Sleep(1 * time.Second)
		return nil
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		// cancel context early
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	require.Equal(t, context.Canceled, StartAndAwaitRunning(ctx, s))
	require.NoError(t, StopAndAwaitTerminated(context.Background(), s))
}

func TestHelperFunctionsStopError(t *testing.T) {
	t.Parallel()

	e := errors.New("some error")
	s := NewIdleService(nil, func(_ error) error { return e })

	require.NoError(t, StartAndAwaitRunning(context.Background(), s))
	require.Equal(t, e, StopAndAwaitTerminated(context.Background(), s))
}

func TestHelperFunctionsStopTooSlow(t *testing.T) {
	t.Parallel()

	s := NewIdleService(nil, func(_ error) error { time.Sleep(1 * time.Second); return nil })

	require.NoError(t, StartAndAwaitRunning(context.Background(), s))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	require.Equal(t, context.DeadlineExceeded, StopAndAwaitTerminated(ctx, s))
	require.NoError(t, StopAndAwaitTerminated(context.Background(), s))
}
