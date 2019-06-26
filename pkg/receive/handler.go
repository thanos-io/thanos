package receive

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	stdlog "log"
	"net"
	"net/http"
	"strconv"
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
	terrors "github.com/prometheus/tsdb/errors"
)

// Options for the web Handler.
type Options struct {
	Receiver          *Writer
	ListenAddress     string
	Registry          prometheus.Registerer
	ReadyStorage      *promtsdb.ReadyStorage
	Endpoint          string
	TenantHeader      string
	ReplicaHeader     string
	ReplicationFactor uint64
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

	// Metrics
	requestDuration      *prometheus.HistogramVec
	requestsTotal        *prometheus.CounterVec
	responseSize         *prometheus.HistogramVec
	forwardRequestsTotal *prometheus.CounterVec

	// These fields are uint32 rather than boolean to be able to use atomic functions.
	storageReady  uint32
	hashringReady uint32
}

func NewHandler(logger log.Logger, o *Options) *Handler {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	h := &Handler{
		logger:       logger,
		readyStorage: o.ReadyStorage,
		receiver:     o.Receiver,
		options:      o,
		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "thanos_http_request_duration_seconds",
				Help:    "Histogram of latencies for HTTP requests.",
				Buckets: []float64{.1, .2, .4, 1, 3, 8, 20, 60, 120},
			},
			[]string{"handler"},
		),
		requestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "thanos_http_requests_total",
				Help: "Tracks the number of HTTP requests.",
			}, []string{"code", "handler", "method"},
		),
		responseSize: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "thanos_http_response_size_bytes",
				Help:    "Histogram of response size for HTTP requests.",
				Buckets: prometheus.ExponentialBuckets(100, 10, 8),
			},
			[]string{"handler"},
		),
		forwardRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "thanos_receive_forward_requests_total",
				Help: "The number of forward requests.",
			}, []string{"result"},
		),
	}

	router := route.New().WithInstrumentation(h.instrumentHandler)
	h.router = router

	readyf := h.testReady
	router.Post("/api/v1/receive", readyf(h.receive))

	if o.Registry != nil {
		o.Registry.MustRegister(
			h.requestDuration,
			h.requestsTotal,
			h.responseSize,
			h.forwardRequestsTotal,
		)
	}

	return h
}

func (h *Handler) instrumentHandler(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
	return promhttp.InstrumentHandlerDuration(
		h.requestDuration.MustCurryWith(prometheus.Labels{"handler": handlerName}),
		promhttp.InstrumentHandlerResponseSize(
			h.responseSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
			promhttp.InstrumentHandlerCounter(
				h.requestsTotal.MustCurryWith(prometheus.Labels{"handler": handlerName}),
				handler,
			),
		),
	)
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
	if h.listener != nil {
		runutil.CloseWithLogOnErr(h.logger, h.listener, "receive HTTP listener")
	}
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

// replica encapsulates the replica number of a request and if the request is
// already replicated.
type replica struct {
	n          uint64
	replicated bool
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

	var rep replica
	replicaRaw := r.Header.Get(h.options.ReplicaHeader)
	// If the header is emtpy, we assume the request is not yet replicated.
	if replicaRaw != "" {
		if rep.n, err = strconv.ParseUint(replicaRaw, 10, 64); err != nil {
			http.Error(w, "could not parse replica header", http.StatusBadRequest)
			return
		}
		rep.replicated = true
	}
	// The replica value in the header is zero-indexed, thus we need >=.
	if rep.n >= h.options.ReplicationFactor {
		http.Error(w, "replica count exceeds replication factor", http.StatusBadRequest)
		return
	}

	tenant := r.Header.Get(h.options.TenantHeader)

	// Forward any time series as necessary. All time series
	// destined for the local node will be written to the receiver.
	// Time series will be replicated as necessary.
	if err := h.forward(r.Context(), tenant, rep, &wreq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// forward accepts a write request, batches its time series by
// corresponding endpoint, and forwards them in parallel to the
// correct endpoint. Requests destined for the local node are written
// the the local receiver. For a given write request, at most one outgoing
// write request will be made to every other node in the hashring,
// unless the request needs to be replicated.
// The function only returns when all requests have finished
// or the context is canceled.
func (h *Handler) forward(ctx context.Context, tenant string, r replica, wreq *prompb.WriteRequest) error {
	wreqs := make(map[string]*prompb.WriteRequest)
	replicas := make(map[string]replica)
	// Batch all of the time series in the write request
	// into several smaller write requests that are
	// grouped by target endpoint. This ensures that
	// for any incoming write request to a node,
	// at most one outgoing write request will be made
	// to every other node in the hashring, rather than
	// one request per time series.
	for i := range wreq.Timeseries {
		endpoint, err := h.hashring.GetN(tenant, &wreq.Timeseries[i], r.n)
		if err != nil {
			return err
		}
		if _, ok := wreqs[endpoint]; !ok {
			wreqs[endpoint] = &prompb.WriteRequest{}
			replicas[endpoint] = r
		}
		wr := wreqs[endpoint]
		wr.Timeseries = append(wr.Timeseries, wreq.Timeseries[i])
	}

	return h.parallelizeRequests(ctx, tenant, replicas, wreqs)
}

// parallelizeRequests parallelizes a given set of write requests.
// The function only returns when all requests have finished
// or the context is canceled.
func (h *Handler) parallelizeRequests(ctx context.Context, tenant string, replicas map[string]replica, wreqs map[string]*prompb.WriteRequest) error {
	ec := make(chan error)
	defer close(ec)
	// We don't wan't to use a sync.WaitGroup here because that
	// introduces an unnecessary second synchronization mechanism,
	// the first being the error chan. Plus, it saves us a goroutine
	// as in order to collect errors while doing wg.Wait, we would
	// need a separate error collection goroutine.
	var n int
	for endpoint := range wreqs {
		n++
		// If the request is not yet replicated, let's replicate it.
		// If the replication factor isn't greater than 1, let's
		// just forward the requests.
		if !replicas[endpoint].replicated && h.options.ReplicationFactor > 1 {
			go func(endpoint string) {
				ec <- h.replicate(ctx, tenant, wreqs[endpoint])
			}(endpoint)
			continue
		}
		// If the endpoint for the write request is the
		// local node, then don't make a request but store locally.
		// By handing replication to the local node in the same
		// function as replication to other nodes, we can treat
		// a failure to write locally as just another error that
		// can be ignored if the replication factor is met.
		if endpoint == h.options.Endpoint {
			go func(endpoint string) {
				ec <- h.receiver.Receive(wreqs[endpoint])
			}(endpoint)
			continue
		}
		// Make a request to the specified endpoint.
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
			req.Header.Add(h.options.ReplicaHeader, strconv.FormatUint(replicas[endpoint].n, 10))

			// Increment the counters as necessary now that
			// the requests will go out.
			defer func() {
				if err != nil {
					h.forwardRequestsTotal.WithLabelValues("error").Inc()
					return
				}
				h.forwardRequestsTotal.WithLabelValues("success").Inc()
			}()

			// Actually make the request against the endpoint
			// we determined should handle these time series.
			var res *http.Response
			res, err = http.DefaultClient.Do(req.WithContext(ctx))
			if err != nil {
				level.Error(h.logger).Log("msg", "forward request error", "err", err, "endpoint", endpoint)
				ec <- err
				return
			}
			if res.StatusCode != http.StatusOK {
				ec <- errors.New(res.Status)
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
	var errs terrors.MultiError
	for ; n > 0; n-- {
		if err := <-ec; err != nil {
			errs.Add(err)
		}
	}

	return errs.Err()
}

// replicate replicates a write request to (replication-factor) nodes
// selected by the tenant and time series.
// The function only returns when all replication requests have finished
// or the context is canceled.
func (h *Handler) replicate(ctx context.Context, tenant string, wreq *prompb.WriteRequest) error {
	wreqs := make(map[string]*prompb.WriteRequest)
	replicas := make(map[string]replica)
	var i uint64
	for i = 0; i < h.options.ReplicationFactor; i++ {
		endpoint, err := h.hashring.GetN(tenant, &wreq.Timeseries[0], i)
		if err != nil {
			return err
		}
		wreqs[endpoint] = wreq
		replicas[endpoint] = replica{i, true}
	}

	err := h.parallelizeRequests(ctx, tenant, replicas, wreqs)
	if errs, ok := err.(terrors.MultiError); ok {
		if uint64(len(errs)) >= (h.options.ReplicationFactor+1)/2 {
			return errors.New("did not meet replication threshhold")
		}
	}
	return errors.Wrap(err, "could not replicate write request")
}
