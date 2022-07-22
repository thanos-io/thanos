// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package scheduler

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go/config"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/thanos-io/thanos/internal/cortex/frontend/v2/frontendv2pb"
	"github.com/thanos-io/thanos/internal/cortex/scheduler/schedulerpb"
	"github.com/thanos-io/thanos/internal/cortex/util/flagext"
	"github.com/thanos-io/thanos/internal/cortex/util/httpgrpcutil"
	"github.com/thanos-io/thanos/internal/cortex/util/services"
	"github.com/thanos-io/thanos/internal/cortex/util/test"
)

const testMaxOutstandingPerTenant = 5

func setupScheduler(t *testing.T, reg prometheus.Registerer) (*Scheduler, schedulerpb.SchedulerForFrontendClient, schedulerpb.SchedulerForQuerierClient) {
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.MaxOutstandingPerTenant = testMaxOutstandingPerTenant

	s, err := NewScheduler(cfg, &limits{queriers: 2}, log.NewNopLogger(), reg)
	require.NoError(t, err)

	server := grpc.NewServer()
	schedulerpb.RegisterSchedulerForFrontendServer(server, s)
	schedulerpb.RegisterSchedulerForQuerierServer(server, s)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), s)
	})

	l, err := net.Listen("tcp", "")
	require.NoError(t, err)

	go func() {
		_ = server.Serve(l)
	}()

	t.Cleanup(func() {
		_ = l.Close()
	})

	c, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	return s, schedulerpb.NewSchedulerForFrontendClient(c), schedulerpb.NewSchedulerForQuerierClient(c)
}

func TestSchedulerBasicEnqueue(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	{
		querierLoop, err := querierClient.QuerierLoop(context.Background())
		require.NoError(t, err)
		require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

		msg2, err := querierLoop.Recv()
		require.NoError(t, err)
		require.Equal(t, uint64(1), msg2.QueryID)
		require.Equal(t, "frontend-12345", msg2.FrontendAddress)
		require.Equal(t, "GET", msg2.HttpRequest.Method)
		require.Equal(t, "/hello", msg2.HttpRequest.Url)
		require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{}))
	}

	verifyNoPendingRequestsLeft(t, scheduler)
}

func TestSchedulerEnqueueWithCancel(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.CANCEL,
		QueryID: 1,
	})

	querierLoop := initQuerierLoop(t, querierClient, "querier-1")

	verifyQuerierDoesntReceiveRequest(t, querierLoop, 500*time.Millisecond)
	verifyNoPendingRequestsLeft(t, scheduler)
}

func initQuerierLoop(t *testing.T, querierClient schedulerpb.SchedulerForQuerierClient, querier string) schedulerpb.SchedulerForQuerier_QuerierLoopClient {
	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: querier}))

	return querierLoop
}

func TestSchedulerEnqueueByMultipleFrontendsWithCancel(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	frontendLoop1 := initFrontendLoop(t, frontendClient, "frontend-1")
	frontendLoop2 := initFrontendLoop(t, frontendClient, "frontend-2")

	frontendToScheduler(t, frontendLoop1, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello1"},
	})

	frontendToScheduler(t, frontendLoop2, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello2"},
	})

	// Cancel first query by first frontend.
	frontendToScheduler(t, frontendLoop1, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.CANCEL,
		QueryID: 1,
	})

	querierLoop := initQuerierLoop(t, querierClient, "querier-1")

	// Let's verify that we can receive query 1 from frontend-2.
	msg, err := querierLoop.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(1), msg.QueryID)
	require.Equal(t, "frontend-2", msg.FrontendAddress)
	// Must notify scheduler back about finished processing, or it will not send more requests (nor remove "current" request from pending ones).
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{}))

	// But nothing else.
	verifyQuerierDoesntReceiveRequest(t, querierLoop, 500*time.Millisecond)
	verifyNoPendingRequestsLeft(t, scheduler)
}

func TestSchedulerEnqueueWithFrontendDisconnect(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	// Wait until the frontend has connected to the scheduler.
	test.Poll(t, time.Second, float64(1), func() interface{} {
		return promtest.ToFloat64(scheduler.connectedFrontendClients)
	})

	// Disconnect frontend.
	require.NoError(t, frontendLoop.CloseSend())

	// Wait until the frontend has disconnected.
	test.Poll(t, time.Second, float64(0), func() interface{} {
		return promtest.ToFloat64(scheduler.connectedFrontendClients)
	})

	querierLoop := initQuerierLoop(t, querierClient, "querier-1")

	verifyQuerierDoesntReceiveRequest(t, querierLoop, 500*time.Millisecond)
	verifyNoPendingRequestsLeft(t, scheduler)
}

func TestCancelRequestInProgress(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	_, err = querierLoop.Recv()
	require.NoError(t, err)

	// At this point, scheduler assumes that querier is processing the request (until it receives empty QuerierToScheduler message back).
	// Simulate frontend disconnect.
	require.NoError(t, frontendLoop.CloseSend())

	// Add a little sleep to make sure that scheduler notices frontend disconnect.
	time.Sleep(500 * time.Millisecond)

	// Report back end of request processing. This should return error, since the QuerierLoop call has finished on scheduler.
	// Note: testing on querierLoop.Context() cancellation didn't work :(
	err = querierLoop.Send(&schedulerpb.QuerierToScheduler{})
	require.Error(t, err)

	verifyNoPendingRequestsLeft(t, scheduler)
}

func TestTracingContext(t *testing.T) {
	scheduler, frontendClient, _ := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")

	closer, err := config.Configuration{}.InitGlobalTracer("test")
	require.NoError(t, err)
	defer closer.Close()

	req := &schedulerpb.FrontendToScheduler{
		Type:            schedulerpb.ENQUEUE,
		QueryID:         1,
		UserID:          "test",
		HttpRequest:     &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
		FrontendAddress: "frontend-12345",
	}

	sp, _ := opentracing.StartSpanFromContext(context.Background(), "client")
	opentracing.GlobalTracer().Inject(sp.Context(), opentracing.HTTPHeaders, (*httpgrpcutil.HttpgrpcHeadersCarrier)(req.HttpRequest))

	frontendToScheduler(t, frontendLoop, req)

	scheduler.pendingRequestsMu.Lock()
	defer scheduler.pendingRequestsMu.Unlock()
	require.Equal(t, 1, len(scheduler.pendingRequests))

	for _, r := range scheduler.pendingRequests {
		require.NotNil(t, r.parentSpanContext)
	}
}

func TestSchedulerShutdown_FrontendLoop(t *testing.T) {
	scheduler, frontendClient, _ := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")

	// Stop the scheduler. This will disable receiving new requests from frontends.
	scheduler.StopAsync()

	// We can still send request to scheduler, but we get shutdown error back.
	require.NoError(t, frontendLoop.Send(&schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	}))

	msg, err := frontendLoop.Recv()
	require.NoError(t, err)
	require.True(t, msg.Status == schedulerpb.SHUTTING_DOWN)
}

func TestSchedulerShutdown_QuerierLoop(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	// Scheduler now has 1 query. Let's connect querier and fetch it.

	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	// Dequeue first query.
	_, err = querierLoop.Recv()
	require.NoError(t, err)

	scheduler.StopAsync()

	// Unblock scheduler loop, to find next request.
	err = querierLoop.Send(&schedulerpb.QuerierToScheduler{})
	require.NoError(t, err)

	// This should now return with error, since scheduler is going down.
	_, err = querierLoop.Recv()
	require.Error(t, err)
}

func TestSchedulerMaxOutstandingRequests(t *testing.T) {
	_, frontendClient, _ := setupScheduler(t, nil)

	for i := 0; i < testMaxOutstandingPerTenant; i++ {
		// coming from different frontends
		fl := initFrontendLoop(t, frontendClient, fmt.Sprintf("frontend-%d", i))
		require.NoError(t, fl.Send(&schedulerpb.FrontendToScheduler{
			Type:        schedulerpb.ENQUEUE,
			QueryID:     uint64(i),
			UserID:      "test", // for same user.
			HttpRequest: &httpgrpc.HTTPRequest{},
		}))

		msg, err := fl.Recv()
		require.NoError(t, err)
		require.True(t, msg.Status == schedulerpb.OK)
	}

	// One more query from the same user will trigger an error.
	fl := initFrontendLoop(t, frontendClient, "extra-frontend")
	require.NoError(t, fl.Send(&schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     0,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	}))

	msg, err := fl.Recv()
	require.NoError(t, err)
	require.True(t, msg.Status == schedulerpb.TOO_MANY_REQUESTS_PER_TENANT)
}

func TestSchedulerForwardsErrorToFrontend(t *testing.T) {
	_, frontendClient, querierClient := setupScheduler(t, nil)

	fm := &frontendMock{resp: map[uint64]*httpgrpc.HTTPResponse{}}
	frontendAddress := ""

	// Setup frontend grpc server
	{
		frontendGrpcServer := grpc.NewServer()
		frontendv2pb.RegisterFrontendForQuerierServer(frontendGrpcServer, fm)

		l, err := net.Listen("tcp", "")
		require.NoError(t, err)

		frontendAddress = l.Addr().String()

		go func() {
			_ = frontendGrpcServer.Serve(l)
		}()

		t.Cleanup(func() {
			_ = l.Close()
		})
	}

	// After preparations, start frontend and querier.
	frontendLoop := initFrontendLoop(t, frontendClient, frontendAddress)
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     100,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	// Scheduler now has 1 query. We now connect querier, fetch the request, and then close the connection.
	// This will make scheduler to report error back to frontend.

	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	// Dequeue first query.
	_, err = querierLoop.Recv()
	require.NoError(t, err)

	// Querier now disconnects, without sending empty message back.
	require.NoError(t, querierLoop.CloseSend())

	// Verify that frontend was notified about request.
	test.Poll(t, 2*time.Second, true, func() interface{} {
		resp := fm.getRequest(100)
		if resp == nil {
			return false
		}

		require.Equal(t, int32(http.StatusInternalServerError), resp.Code)
		return true
	})
}

func TestSchedulerMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	scheduler, frontendClient, _ := setupScheduler(t, reg)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "another",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{user="another"} 1
		cortex_query_scheduler_queue_length{user="test"} 1
	`), "cortex_query_scheduler_queue_length"))

	scheduler.cleanupMetricsForInactiveUser("test")

	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{user="another"} 1
	`), "cortex_query_scheduler_queue_length"))
}

func initFrontendLoop(t *testing.T, client schedulerpb.SchedulerForFrontendClient, frontendAddr string) schedulerpb.SchedulerForFrontend_FrontendLoopClient {
	loop, err := client.FrontendLoop(context.Background())
	require.NoError(t, err)

	require.NoError(t, loop.Send(&schedulerpb.FrontendToScheduler{
		Type:            schedulerpb.INIT,
		FrontendAddress: frontendAddr,
	}))

	// Scheduler acks INIT by sending OK back.
	resp, err := loop.Recv()
	require.NoError(t, err)
	require.True(t, resp.Status == schedulerpb.OK)

	return loop
}

func frontendToScheduler(t *testing.T, frontendLoop schedulerpb.SchedulerForFrontend_FrontendLoopClient, req *schedulerpb.FrontendToScheduler) {
	require.NoError(t, frontendLoop.Send(req))
	msg, err := frontendLoop.Recv()
	require.NoError(t, err)
	require.True(t, msg.Status == schedulerpb.OK)
}

// If this verification succeeds, there will be leaked goroutine left behind. It will be cleaned once grpc server is shut down.
func verifyQuerierDoesntReceiveRequest(t *testing.T, querierLoop schedulerpb.SchedulerForQuerier_QuerierLoopClient, timeout time.Duration) {
	ch := make(chan interface{}, 1)

	go func() {
		m, e := querierLoop.Recv()
		if e != nil {
			ch <- e
		} else {
			ch <- m
		}
	}()

	select {
	case val := <-ch:
		require.Failf(t, "expected timeout", "got %v", val)
	case <-time.After(timeout):
		return
	}
}

func verifyNoPendingRequestsLeft(t *testing.T, scheduler *Scheduler) {
	test.Poll(t, 1*time.Second, 0, func() interface{} {
		scheduler.pendingRequestsMu.Lock()
		defer scheduler.pendingRequestsMu.Unlock()
		return len(scheduler.pendingRequests)
	})
}

type limits struct {
	queriers int
}

func (l limits) MaxQueriersPerUser(_ string) int {
	return l.queriers
}

type frontendMock struct {
	mu   sync.Mutex
	resp map[uint64]*httpgrpc.HTTPResponse
}

func (f *frontendMock) QueryResult(_ context.Context, request *frontendv2pb.QueryResultRequest) (*frontendv2pb.QueryResultResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.resp[request.QueryID] = request.HttpResponse
	return &frontendv2pb.QueryResultResponse{}, nil
}

func (f *frontendMock) getRequest(queryID uint64) *httpgrpc.HTTPResponse {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.resp[queryID]
}
