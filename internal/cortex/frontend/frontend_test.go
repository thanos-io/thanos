// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package frontend

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	strconv "strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/internal/cortex/frontend/transport"
	"github.com/thanos-io/thanos/internal/cortex/frontend/v1/frontendv1pb"
	querier_worker "github.com/thanos-io/thanos/internal/cortex/querier/worker"
	"github.com/thanos-io/thanos/internal/cortex/util/concurrency"
	"github.com/thanos-io/thanos/internal/cortex/util/flagext"
	"github.com/thanos-io/thanos/internal/cortex/util/services"
)

const (
	query        = "/api/v1/query_range?end=1536716898&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536673680&step=120"
	responseBody = `{"status":"success","data":{"resultType":"Matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}]}}`
)

func TestFrontend_RequestHostHeaderWhenDownstreamURLIsConfigured(t *testing.T) {
	// Create an HTTP server listening locally. This server mocks the downstream
	// Prometheus API-compatible server.
	downstreamListen, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	observedHost := make(chan string, 2)
	downstreamServer := http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			observedHost <- r.Host

			_, err := w.Write([]byte(responseBody))
			require.NoError(t, err)
		}),
	}

	defer downstreamServer.Shutdown(context.Background()) //nolint:errcheck
	go downstreamServer.Serve(downstreamListen)           //nolint:errcheck

	// Configure the query-frontend with the mocked downstream server.
	config := defaultFrontendConfig()
	config.DownstreamURL = fmt.Sprintf("http://%s", downstreamListen.Addr())

	// Configure the test to send a request to the query-frontend and assert on the
	// Host HTTP header received by the downstream server.
	test := func(addr string) {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/%s", addr, query), nil)
		require.NoError(t, err)

		ctx := context.Background()
		req = req.WithContext(ctx)
		err = user.InjectOrgIDIntoHTTPRequest(user.InjectOrgID(ctx, "1"), req)
		require.NoError(t, err)

		client := http.Client{
			Transport: &nethttp.Transport{},
		}
		resp, err := client.Do(req)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)

		defer resp.Body.Close()
		_, err = ioutil.ReadAll(resp.Body)
		require.NoError(t, err)

		// We expect the Host received by the downstream is the downstream host itself
		// and not the query-frontend host.
		downstreamReqHost := <-observedHost
		assert.Equal(t, downstreamListen.Addr().String(), downstreamReqHost)
		assert.NotEqual(t, downstreamReqHost, addr)
	}

	testFrontend(t, config, nil, test, false, nil)
	testFrontend(t, config, nil, test, true, nil)
}

func TestFrontend_LogsSlowQueries(t *testing.T) {
	// Create an HTTP server listening locally. This server mocks the downstream
	// Prometheus API-compatible server.
	downstreamListen, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	downstreamServer := http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write([]byte(responseBody))
			require.NoError(t, err)
		}),
	}

	defer downstreamServer.Shutdown(context.Background()) //nolint:errcheck
	go downstreamServer.Serve(downstreamListen)           //nolint:errcheck

	tests := map[string]struct {
		longerThan time.Duration
		shouldLog  bool
	}{
		"longer than set to > 0": {
			longerThan: 1 * time.Microsecond,
			shouldLog:  true,
		},
		"longer than set to < 0": {
			longerThan: -1 * time.Microsecond,
			shouldLog:  true,
		},
		"logging disabled": {
			longerThan: 0,
			shouldLog:  false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Configure the query-frontend with the mocked downstream server.
			config := defaultFrontendConfig()
			config.Handler.LogQueriesLongerThan = testData.longerThan
			config.DownstreamURL = fmt.Sprintf("http://%s", downstreamListen.Addr())

			var buf concurrency.SyncBuffer

			test := func(addr string) {
				// To assert form values are logged as well.
				data := url.Values{}
				data.Set("test", "form")
				data.Set("issue", "3111")

				req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/?foo=bar", addr), strings.NewReader(data.Encode()))
				req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
				req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

				ctx := context.Background()
				req = req.WithContext(ctx)
				assert.NoError(t, err)
				err = user.InjectOrgIDIntoHTTPRequest(user.InjectOrgID(ctx, "1"), req)
				assert.NoError(t, err)

				client := http.Client{
					Transport: &nethttp.Transport{},
				}

				resp, err := client.Do(req)
				assert.NoError(t, err)
				b, err := ioutil.ReadAll(resp.Body)
				resp.Body.Close()

				assert.NoError(t, err)
				assert.Equal(t, 200, resp.StatusCode, string(b))

				logs := buf.String()
				assert.NotContains(t, logs, "unable to parse form for request")

				if testData.shouldLog {
					assert.Contains(t, logs, "msg=\"slow query detected\"")
					assert.Contains(t, logs, "param_issue=3111")
					assert.Contains(t, logs, "param_test=form")
					assert.Contains(t, logs, "param_foo=bar")
				} else {
					assert.NotContains(t, logs, "msg=\"slow query detected\"")
				}
			}

			testFrontend(t, config, nil, test, false, log.NewLogfmtLogger(&buf))
		})
	}
}

func TestFrontend_ReturnsRequestBodyTooLargeError(t *testing.T) {
	// Create an HTTP server listening locally. This server mocks the downstream
	// Prometheus API-compatible server.
	downstreamListen, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	downstreamServer := http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write([]byte(responseBody))
			require.NoError(t, err)
		}),
	}

	defer downstreamServer.Shutdown(context.Background()) //nolint:errcheck
	go downstreamServer.Serve(downstreamListen)           //nolint:errcheck

	// Configure the query-frontend with the mocked downstream server.
	config := defaultFrontendConfig()
	config.DownstreamURL = fmt.Sprintf("http://%s", downstreamListen.Addr())
	config.Handler.MaxBodySize = 1

	test := func(addr string) {
		data := url.Values{}
		data.Set("test", "max body size")

		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/?foo=bar", addr), strings.NewReader(data.Encode()))
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

		ctx := context.Background()
		req = req.WithContext(ctx)
		assert.NoError(t, err)
		err = user.InjectOrgIDIntoHTTPRequest(user.InjectOrgID(ctx, "1"), req)
		assert.NoError(t, err)

		client := http.Client{
			Transport: &nethttp.Transport{},
		}

		resp, err := client.Do(req)
		assert.NoError(t, err)
		b, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		assert.NoError(t, err)

		assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode, string(b))
	}

	testFrontend(t, config, nil, test, false, nil)
}

func testFrontend(t *testing.T, config CombinedFrontendConfig, handler http.Handler, test func(addr string), matchMaxConcurrency bool, l log.Logger) {
	logger := log.NewNopLogger()
	if l != nil {
		logger = l
	}

	var workerConfig querier_worker.Config
	flagext.DefaultValues(&workerConfig)
	workerConfig.Parallelism = 1
	workerConfig.MatchMaxConcurrency = matchMaxConcurrency
	workerConfig.MaxConcurrentRequests = 1

	// localhost:0 prevents firewall warnings on Mac OS X.
	grpcListen, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	workerConfig.FrontendAddress = grpcListen.Addr().String()

	httpListen, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	rt, v1, v2, err := InitFrontend(config, limits{}, 0, logger, nil)
	require.NoError(t, err)
	require.NotNil(t, rt)
	// v1 will be nil if DownstreamURL is defined.
	require.Nil(t, v2)
	if v1 != nil {
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), v1))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), v1))
		})
	}

	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(otgrpc.OpenTracingStreamServerInterceptor(opentracing.GlobalTracer())),
	)
	defer grpcServer.GracefulStop()

	if v1 != nil {
		frontendv1pb.RegisterFrontendServer(grpcServer, v1)
	}

	r := mux.NewRouter()
	r.PathPrefix("/").Handler(middleware.Merge(
		middleware.AuthenticateUser,
		middleware.Tracer{},
	).Wrap(transport.NewHandler(config.Handler, rt, logger, nil)))

	httpServer := http.Server{
		Handler: r,
	}
	defer httpServer.Shutdown(context.Background()) //nolint:errcheck

	go httpServer.Serve(httpListen) //nolint:errcheck
	go grpcServer.Serve(grpcListen) //nolint:errcheck

	var worker services.Service
	worker, err = querier_worker.NewQuerierWorker(workerConfig, httpgrpc_server.NewServer(handler), logger, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), worker))

	test(httpListen.Addr().String())

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), worker))
}

func defaultFrontendConfig() CombinedFrontendConfig {
	config := CombinedFrontendConfig{}
	flagext.DefaultValues(&config)
	flagext.DefaultValues(&config.Handler)
	flagext.DefaultValues(&config.FrontendV1)
	flagext.DefaultValues(&config.FrontendV2)
	return config
}

type limits struct {
	queriers int
}

func (l limits) MaxQueriersPerUser(_ string) int {
	return l.queriers
}
