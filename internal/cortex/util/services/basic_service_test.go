package services

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var _ Service = &BasicService{} // just make sure that BasicService implements Service

type serv struct {
	Service

	conf servConf
}

type servConf struct {
	startSleep            time.Duration
	startReturnContextErr bool
	startRetVal           error

	runSleep            time.Duration
	runReturnContextErr bool
	runRetVal           error

	stopRetVal error
}

func newServ(conf servConf) *serv {
	s := &serv{
		conf: conf,
	}
	s.Service = NewBasicService(s.startUp, s.run, s.shutDown)
	return s
}

func (s *serv) startUp(ctx context.Context) error {
	select {
	case <-time.After(s.conf.startSleep):
	case <-ctx.Done():
		if s.conf.startReturnContextErr {
			return ctx.Err()
		}
	}
	return s.conf.startRetVal
}

func (s *serv) run(ctx context.Context) error {
	select {
	case <-time.After(s.conf.runSleep):
	case <-ctx.Done():
		if s.conf.runReturnContextErr {
			return ctx.Err()
		}
	}
	return s.conf.runRetVal
}

func (s *serv) shutDown(_ error) error {
	return s.conf.stopRetVal
}

type testCase struct {
	startReturnContextErr bool
	startRetVal           error

	runReturnContextErr bool
	runRetVal           error

	stopRetVal error

	cancelAfterStartAsync bool
	stopAfterStartAsync   bool

	cancelAfterAwaitRunning bool
	stopAfterAwaitRunning   bool

	// Expected values
	awaitRunningError    []error
	awaitTerminatedError error
	failureCase          error
	listenerLog          []string
}

func TestStopInNew(t *testing.T) {
	t.Parallel()

	s := newServ(servConf{})

	require.Equal(t, New, s.State())
	s.StopAsync()
	require.Error(t, s.AwaitRunning(context.Background()))
	require.NoError(t, s.AwaitTerminated(context.Background()))
	require.Equal(t, Terminated, s.State())
}

func TestAllFunctionality(t *testing.T) {
	errStartFailed := errors.New("start failed")
	errRunFailed := errors.New("runFn failed")
	errStopFailed := errors.New("stop failed")

	testCases := map[string]testCase{
		"normal flow": {
			listenerLog: []string{"starting", "running", "stopping: Running", "terminated: Stopping"},
		},

		"start returns error": {
			startRetVal:          errStartFailed,
			awaitRunningError:    []error{invalidServiceStateWithFailureError(Failed, Running, errStartFailed)},
			awaitTerminatedError: invalidServiceStateWithFailureError(Failed, Terminated, errStartFailed), // Failed in start
			failureCase:          errStartFailed,
			listenerLog:          []string{"starting", "failed: Starting: start failed"},
		},

		"start is canceled via context and returns cancelation error": {
			cancelAfterStartAsync: true,
			startReturnContextErr: true,
			awaitRunningError:     []error{invalidServiceStateWithFailureError(Failed, Running, context.Canceled)},
			awaitTerminatedError:  invalidServiceStateWithFailureError(Failed, Terminated, context.Canceled),
			failureCase:           context.Canceled,
			listenerLog:           []string{"starting", "failed: Starting: context canceled"},
		},

		"start is canceled via context, doesn't return error. Run shouldn't runFn, since context is canceled now.": {
			cancelAfterStartAsync: true,
			startReturnContextErr: false,
			// we can observe Stopping or Terminated
			awaitRunningError:    []error{invalidServiceStateError(Stopping, Running), invalidServiceStateError(Terminated, Running)},
			awaitTerminatedError: nil, // but still terminates correctly, since Start or RunningFn didn't return error
			failureCase:          nil, // start didn't return error, service stopped without calling run
			listenerLog:          []string{"starting", "stopping: Starting", "terminated: Stopping"},
		},

		"start is canceled via StopAsync, but start doesn't return error": {
			startReturnContextErr: false, // don't return error on cancellation, just stop early
			stopAfterStartAsync:   true,
			// we can observe Stopping or Terminated
			awaitRunningError:    []error{invalidServiceStateError(Stopping, Running), invalidServiceStateError(Terminated, Running)},
			awaitTerminatedError: nil, // stopped while starting, but no error. Should be in Terminated state.
			failureCase:          nil, // start didn't return error, service stopped without calling run
			listenerLog:          []string{"starting", "stopping: Starting", "terminated: Stopping"},
		},

		"runFn returns error": {
			runRetVal:            errRunFailed,
			awaitTerminatedError: invalidServiceStateWithFailureError(Failed, Terminated, errRunFailed), // service will get into Failed state, since run failed
			failureCase:          errRunFailed,
			listenerLog:          []string{"starting", "running", "stopping: Running", "failed: Stopping: runFn failed"},
		},

		"runFn returns error from context cancelation": {
			runReturnContextErr:     true,
			cancelAfterAwaitRunning: true,
			awaitTerminatedError:    invalidServiceStateWithFailureError(Failed, Terminated, context.Canceled), // service will get into Failed state, since run failed
			failureCase:             context.Canceled,
			listenerLog:             []string{"starting", "running", "stopping: Running", "failed: Stopping: context canceled"},
		},

		"runFn and stop both return error, only one is reported": {
			runRetVal:            errRunFailed,
			stopRetVal:           errStopFailed,
			awaitTerminatedError: invalidServiceStateWithFailureError(Failed, Terminated, errRunFailed), // service will get into Failed state, since run failed
			failureCase:          errRunFailed,                                                          // run fails first, its error is returned
			listenerLog:          []string{"starting", "running", "stopping: Running", "failed: Stopping: runFn failed"},
		},

		"stop returns error": {
			runRetVal:            nil,
			stopRetVal:           errStopFailed,
			awaitTerminatedError: invalidServiceStateWithFailureError(Failed, Terminated, errStopFailed), // service will get into Failed state, since stop fails
			failureCase:          errStopFailed,
			listenerLog:          []string{"starting", "running", "stopping: Running", "failed: Stopping: stop failed"},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// don't run tests in parallel, somehow that doesn't report failures correctly :-(
			// t.Parallel()
			runTestCase(t, tc)
		})
	}
}

func runTestCase(t *testing.T, tc testCase) {
	s := newServ(servConf{
		startSleep:            time.Second,
		startRetVal:           tc.startRetVal,
		startReturnContextErr: tc.startReturnContextErr,
		runSleep:              time.Second,
		runRetVal:             tc.runRetVal,
		runReturnContextErr:   tc.runReturnContextErr,
		stopRetVal:            tc.stopRetVal,
	})

	sl := newServiceListener()
	require.NoError(t, sl.StartAsync(context.Background()))
	require.NoError(t, sl.AwaitRunning(context.Background()))

	s.AddListener(sl)

	require.Equal(t, New, s.State())

	ctx, servCancel := context.WithCancel(context.Background())
	defer servCancel() // make sure to call cancel at least once

	require.NoError(t, s.StartAsync(ctx), "StartAsync")
	require.Error(t, s.StartAsync(ctx), "second StartAsync") // must always return error
	if tc.cancelAfterStartAsync {
		servCancel()
	}
	if tc.stopAfterStartAsync {
		s.StopAsync()
	}

	awaitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	awaitRunningErr := s.AwaitRunning(awaitCtx)

	if len(tc.awaitRunningError) == 0 {
		require.NoError(t, awaitRunningErr, "AwaitRunning")
	} else {
		require.Contains(t, tc.awaitRunningError, awaitRunningErr, "AwaitRunning")
	}

	if tc.cancelAfterAwaitRunning {
		servCancel()
	}
	if tc.stopAfterAwaitRunning {
		s.StopAsync()
	}

	awaitCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.Equal(t, tc.awaitTerminatedError, s.AwaitTerminated(awaitCtx), "AwaitTerminated")
	require.Equal(t, tc.failureCase, s.FailureCase(), "FailureCase")

	// get log, and compare against expected
	// we can only get log once listener is finished, otherwise we risk race conditions

	sl.StopAsync()
	require.NoError(t, sl.AwaitTerminated(context.Background()))

	require.Equal(t, tc.listenerLog, sl.log)
}

// serviceListener is implemented as a service!
type serviceListener struct {
	Service

	log []string
	ch  chan string
}

func newServiceListener() *serviceListener {
	sl := &serviceListener{
		ch: make(chan string),
	}
	sl.Service = NewBasicService(nil, sl.collect, nil)
	return sl
}

func (sl *serviceListener) collect(ctx context.Context) error {
	for l := range sl.ch {
		sl.log = append(sl.log, l)
	}
	return nil
}

func (sl *serviceListener) Failed(from State, failure error) {
	sl.ch <- fmt.Sprintf("failed: %v: %v", from, failure)
	close(sl.ch)
}

func (sl *serviceListener) Running() {
	sl.ch <- "running"
}

func (sl *serviceListener) Starting() {
	sl.ch <- "starting"
}

func (sl *serviceListener) Stopping(from State) {
	sl.ch <- fmt.Sprintf("stopping: %v", from)
}

func (sl *serviceListener) Terminated(from State) {
	sl.ch <- fmt.Sprintf("terminated: %v", from)
	close(sl.ch)
}

func TestFailureCaseFromRunningIsPassedToStopping(t *testing.T) {
	err := errors.New("test error")

	s := NewBasicService(nil, func(_ context.Context) error {
		return err
	}, func(failureCase error) error {
		if failureCase != err {
			t.Log("invalid failureCase")
			t.Fail()
		}
		return nil
	})

	require.NoError(t, s.StartAsync(context.Background()))
	require.Error(t, s.AwaitTerminated(context.Background())) // service fails, so we expect Failed state

	fc := s.FailureCase()
	require.Equal(t, err, fc)
}

func TestServiceName(t *testing.T) {
	s := NewIdleService(nil, nil).WithName("test name")
	require.Equal(t, "test name", DescribeService(s))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, s.StartAsync(ctx))

	// once service has started, BasicService will not allow changing the name
	s.WithName("new")
	require.Equal(t, "test name", DescribeService(s))
}
