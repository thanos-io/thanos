package receive

import (
	"bytes"
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
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
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
	Endpoint      string
	TenantHeader  string
}

// Handler serves a Prometheus remote write receiving HTTP endpoint.
type Handler struct {
	readyStorage *promtsdb.ReadyStorage
	logger       log.Logger
	receiver     *Writer
	router       *route.Router
	hashring     Hashring
	options      *Options
	listener     net.Listener

	// These fields are uint32 rather than boolean to be able to use atomic functions.
	storageReady  uint32
	hashringReady uint32
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
	}

	readyf := h.testReady
	router.Post("/api/v1/receive", readyf(h.receive))

	return h
}

// StorageReady marks the storage as ready.
func (h *Handler) StorageReady() {
	atomic.StoreUint32(&h.storageReady, 1)
}

// Hashring sets the hashring for the handler and marks the hashring as ready.
// If the hashring is nil, then the hashring is marked as not ready.
func (h *Handler) Hashring(hashring Hashring) {
	if hashring == nil {
		atomic.StoreUint32(&h.hashringReady, 0)
		h.hashring = nil
		return
	}
	h.hashring = hashring
	atomic.StoreUint32(&h.hashringReady, 1)
}

// Verifies whether the server is ready or not.
func (h *Handler) isReady() bool {
	sr := atomic.LoadUint32(&h.storageReady)
	hr := atomic.LoadUint32(&h.hashringReady)
	return sr > 0 && hr > 0
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

// Close stops the Handler.
func (h *Handler) Close() {
	runutil.CloseWithLogOnErr(h.logger, h.listener, "receive HTTP listener")
}

// Run serves the HTTP endpoints.
func (h *Handler) Run() error {
	level.Info(h.logger).Log("msg", "Start listening for connections", "address", h.options.ListenAddress)

	var err error
	h.listener, err = net.Listen("tcp", h.options.ListenAddress)
	if err != nil {
		return err
	}

	// Monitor incoming connections with conntrack.
	h.listener = conntrack.NewListener(h.listener,
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

	return httpSrv.Serve(h.listener)
}

func (h *Handler) receive(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
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

	tenant := r.Header.Get(h.options.TenantHeader)
	local, err := h.forward(r.Context(), tenant, &wreq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// There may be no WriteRequest destined for the local node.
	if local != nil {
		if err := h.receiver.Receive(local); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
}

// forward accepts a write request, batches its time series by
// corresponding endpoint, and forwards them in parallel. It returns a write
// request containing only the time series that correspond to
// local handler. For a given write request, at most one outgoing
// write request will be made to every other node in the hashring.
// The function only returns when all requests have finished,
// or the context is canceled.
func (h *Handler) forward(ctx context.Context, tenant string, wreq *prompb.WriteRequest) (*prompb.WriteRequest, error) {
	wreqs := make(map[string]*prompb.WriteRequest)
	// Batch all of the time series in the write request
	// into several smaller write requests that are
	// grouped by target endpoint. This ensures that
	// for any incoming write request to a node,
	// at most one outgoing write request will be made
	// to every other node in the hashring, rather than
	// one request per time series.
	for i := range wreq.Timeseries {
		endpoint, err := h.hashring.Get(tenant, &wreq.Timeseries[i])
		if err != nil {
			return nil, err
		}
		if _, ok := wreqs[endpoint]; !ok {
			wreqs[endpoint] = &prompb.WriteRequest{}
		}
		wr := wreqs[endpoint]
		wr.Timeseries = append(wr.Timeseries, wreq.Timeseries[i])
	}

	ec := make(chan error)
	defer close(ec)
	// We don't wan't to use a sync.WaitGroup here because that
	// introduces an unnecessary second synchronization mechanism,
	// the first being the error chan. Plus, it saves us a goroutine
	// as in order to collect errors while doing wg.Wait, we would
	// need a separate error collection goroutine.
	var n int
	var local *prompb.WriteRequest
	for endpoint := range wreqs {
		// If the endpoint for the write request is the
		// local node, then don't make a request.
		// Save it for later so it can be returned.
		if endpoint == h.options.Endpoint {
			local = wreqs[endpoint]
			continue
		}
		n++
		go func(endpoint string) {
			buf, err := proto.Marshal(wreqs[endpoint])
			if err != nil {
				level.Error(h.logger).Log("msg", "proto marshal error", "err", err, "endpoint", endpoint)
				ec <- err
				return
			}
			req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(snappy.Encode(nil, buf)))
			if err != nil {
				level.Error(h.logger).Log("msg", "create request error", "err", err, "endpoint", endpoint)
				ec <- err
				return
			}
			req.Header.Add(h.options.TenantHeader, tenant)
			// Actually make the request against the endpoint
			// we determined should handle these time series.
			if _, err := http.DefaultClient.Do(req.WithContext(ctx)); err != nil {
				level.Error(h.logger).Log("msg", "forward request error", "err", err, "endpoint", endpoint)
				ec <- err
				return
			}
			ec <- nil
		}(endpoint)
	}

	// Collect any errors from forwarding the time series.
	// Rather than doing a wg.Wait here, we decrement a counter
	// for every error received on the chan. This simplifies
	// error collection and avoids data races with a separate
	// error collection goroutine.
	var errs error
	for ; n > 0; n-- {
		if err := <-ec; err != nil {
			if errs == nil {
				errs = err
				continue
			}
			errs = errors.Wrap(errs, err.Error())
		}
	}

	return local, errs
}
