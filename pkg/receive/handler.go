package receive

import (
	"context"
	"fmt"
	"io/ioutil"
	stdlog "log"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store/prompb"
	conntrack "github.com/mwitkow/go-conntrack"
	"github.com/oklog/run"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/route"
	promtsdb "github.com/prometheus/prometheus/storage/tsdb"
)

var (
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "thanos_http_request_duration_seconds",
			Help:    "Histogram of latencies for HTTP requests.",
			Buckets: []float64{.1, .2, .4, 1, 3, 8, 20, 60, 120},
		},
		[]string{"handler"},
	)
	responseSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "thanos_http_response_size_bytes",
			Help:    "Histogram of response size for HTTP requests.",
			Buckets: prometheus.ExponentialBuckets(100, 10, 8),
		},
		[]string{"handler"},
	)
)

// Options for the web Handler.
type Options struct {
	Receiver      *Writer
	ListenAddress string
	Registry      prometheus.Registerer
	ReadyStorage  *promtsdb.ReadyStorage
}

// Handler serves a Prometheus remote write receiving HTTP endpoint.
type Handler struct {
	readyStorage *promtsdb.ReadyStorage
	logger       log.Logger
	receiver     *Writer
	router       *route.Router
	options      *Options
	quitCh       chan struct{}

	ready uint32 // ready is uint32 rather than boolean to be able to use atomic functions.
}

func instrumentHandler(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
	return promhttp.InstrumentHandlerDuration(
		requestDuration.MustCurryWith(prometheus.Labels{"handler": handlerName}),
		promhttp.InstrumentHandlerResponseSize(
			responseSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
			handler,
		),
	)
}

func NewHandler(logger log.Logger, o *Options) *Handler {
	router := route.New().WithInstrumentation(instrumentHandler)
	if logger == nil {
		logger = log.NewNopLogger()
	}

	h := &Handler{
		logger:       logger,
		router:       router,
		readyStorage: o.ReadyStorage,
		receiver:     o.Receiver,
		options:      o,
		quitCh:       make(chan struct{}),
	}

	readyf := h.testReady
	router.Post("/api/v1/receive", readyf(h.receive))

	return h
}

// Ready sets Handler to be ready.
func (h *Handler) Ready() {
	atomic.StoreUint32(&h.ready, 1)
}

// Verifies whether the server is ready or not.
func (h *Handler) isReady() bool {
	ready := atomic.LoadUint32(&h.ready)
	return ready > 0
}

// Checks if server is ready, calls f if it is, returns 503 if it is not.
func (h *Handler) testReady(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.isReady() {
			f(w, r)
			return
		}

		w.WriteHeader(http.StatusServiceUnavailable)
		_, err := fmt.Fprintf(w, "Service Unavailable")
		if err != nil {
			h.logger.Log("msg", "failed to write to response body", "err", err)
		}
	}
}

// Quit returns the receive-only quit channel.
func (h *Handler) Quit() <-chan struct{} {
	return h.quitCh
}

// Checks if server is ready, calls f if it is, returns 503 if it is not.
func (h *Handler) testReadyHandler(f http.Handler) http.HandlerFunc {
	return h.testReady(f.ServeHTTP)
}

// Run serves the HTTP endpoints.
func (h *Handler) Run(ctx context.Context) error {
	level.Info(h.logger).Log("msg", "Start listening for connections", "address", h.options.ListenAddress)

	listener, err := net.Listen("tcp", h.options.ListenAddress)
	if err != nil {
		return err
	}

	// Monitor incoming connections with conntrack.
	listener = conntrack.NewListener(listener,
		conntrack.TrackWithName("http"),
		conntrack.TrackWithTracing())

	operationName := nethttp.OperationNameFunc(func(r *http.Request) string {
		return fmt.Sprintf("%s %s", r.Method, r.URL.Path)
	})
	mux := http.NewServeMux()
	mux.Handle("/", h.router)

	errlog := stdlog.New(log.NewStdlibAdapter(level.Error(h.logger)), "", 0)

	httpSrv := &http.Server{
		Handler:  nethttp.Middleware(opentracing.GlobalTracer(), mux, operationName),
		ErrorLog: errlog,
	}

	var g run.Group
	g.Add(func() error {
		return httpSrv.Serve(listener)
	}, func(error) {
		runutil.CloseWithLogOnErr(h.logger, listener, "receive HTTP listener")
	})

	return g.Run()
}

func (h *Handler) receive(w http.ResponseWriter, req *http.Request) {
	compressed, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		level.Error(h.logger).Log("msg", "snappy decode error", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var wreq prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &wreq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.receiver.Receive(&wreq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
