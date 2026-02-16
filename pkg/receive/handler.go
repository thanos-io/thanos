// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	stdlog "log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/jpillora/backoff"
	"github.com/klauspost/compress/s2"
	"github.com/mwitkow/go-conntrack"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/api"
	statusapi "github.com/thanos-io/thanos/pkg/api/status"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/receive/writecapnp"

	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/server/http/middleware"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"github.com/thanos-io/thanos/pkg/tracing"
)

const (
	// DefaultStatsLimit is the default value used for limiting tenant stats.
	DefaultStatsLimit = 10
	// DefaultReplicaHeader is the default header used to designate the replica count of a write request.
	DefaultReplicaHeader = "THANOS-REPLICA"
	// AllTenantsQueryParam is the query parameter for getting TSDB stats for all tenants.
	AllTenantsQueryParam = "all_tenants"
	// LimitStatsQueryParam is the query parameter for limiting the amount of returned TSDB stats.
	LimitStatsQueryParam = "limit"
	// Labels for metrics.
	labelSuccess = "success"
	labelError   = "error"
)

type ReplicationProtocol string

const (
	ProtobufReplication  ReplicationProtocol = "protobuf"
	CapNProtoReplication ReplicationProtocol = "capnproto"
)

var (
	// errConflict is returned whenever an operation fails due to any conflict-type error.
	errConflict = errors.New("conflict")

	errBadReplica  = errors.New("request replica exceeds receiver replication factor")
	errNotReady    = errors.New("target not ready")
	errUnavailable = errors.New("target not available")
	errInternal    = errors.New("internal error")
)

type WriteableStoreAsyncClient interface {
	storepb.WriteableStoreClient
	RemoteWriteAsync(context.Context, *storepb.WriteRequest, endpointReplica, []int, chan writeResponse, func(error))
}

// Options for the web Handler.
type Options struct {
	Writer                  *Writer
	ListenAddress           string
	Registry                *prometheus.Registry
	TenantHeader            string
	TenantField             string
	DefaultTenantID         string
	ReplicaHeader           string
	Endpoint                string
	ReplicationFactor       uint64
	SplitTenantLabelName    string
	ReceiverMode            ReceiverMode
	Tracer                  opentracing.Tracer
	TLSConfig               *tls.Config
	DialOpts                []grpc.DialOption
	ForwardTimeout          time.Duration
	MaxBackoff              time.Duration
	MaxArtificialDelay      time.Duration
	RelabelConfigs          []*relabel.Config
	TSDBStats               TSDBStats
	Limiter                 *Limiter
	AsyncForwardWorkerCount uint
	ReplicationProtocol     ReplicationProtocol
	OtlpEnableTargetInfo    bool
	OtlpResourceAttributes  []string
}

// Handler serves a Prometheus remote write receiving HTTP endpoint.
type Handler struct {
	logger               log.Logger
	writer               *Writer
	router               *route.Router
	options              *Options
	splitTenantLabelName string
	httpSrv              *http.Server

	mtx          sync.RWMutex
	hashring     Hashring
	peers        peersContainer
	receiverMode ReceiverMode

	forwardRequests   *prometheus.CounterVec
	replications      *prometheus.CounterVec
	replicationFactor prometheus.Gauge

	writeSamplesTotal    *prometheus.HistogramVec
	writeTimeseriesTotal *prometheus.HistogramVec

	pendingWriteRequests        prometheus.Gauge
	pendingWriteRequestsCounter atomic.Int32

	Limiter *Limiter
}

func NewHandler(logger log.Logger, o *Options) *Handler {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	var registerer prometheus.Registerer = nil
	if o.Registry != nil {
		registerer = o.Registry
	}

	workers := o.AsyncForwardWorkerCount
	if workers == 0 {
		workers = 1
	}
	level.Info(logger).Log("msg", "Starting receive handler with async forward workers", "workers", workers)

	h := &Handler{
		logger:               logger,
		writer:               o.Writer,
		router:               route.New(),
		options:              o,
		splitTenantLabelName: o.SplitTenantLabelName,
		peers: newPeerGroup(
			logger,
			backoff.Backoff{
				Factor: 2,
				Min:    100 * time.Millisecond,
				Max:    o.MaxBackoff,
				Jitter: true,
			},
			promauto.With(registerer).NewHistogram(
				prometheus.HistogramOpts{
					Name:    "thanos_receive_forward_delay_seconds",
					Help:    "The delay between the time the request was received and the time it was forwarded to a worker. ",
					Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
				},
			),
			workers,
			o.MaxArtificialDelay,
			o.ReplicationProtocol,
			o.DialOpts...),
		receiverMode: o.ReceiverMode,
		Limiter:      o.Limiter,
		forwardRequests: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "thanos_receive_forward_requests_total",
				Help: "The number of forward requests.",
			}, []string{"result"},
		),
		replications: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "thanos_receive_replications_total",
				Help: "The number of replication operations done by the receiver. The success of replication is fulfilled when a quorum is met.",
			}, []string{"result"},
		),
		replicationFactor: promauto.With(registerer).NewGauge(
			prometheus.GaugeOpts{
				Name: "thanos_receive_replication_factor",
				Help: "The number of times to replicate incoming write requests.",
			},
		),
		writeTimeseriesTotal: promauto.With(registerer).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "thanos",
				Subsystem: "receive",
				Name:      "write_timeseries",
				Help:      "The number of timeseries received in the incoming write requests.",
				Buckets:   []float64{10, 50, 100, 500, 1000, 5000, 10000},
			}, []string{"code", "tenant"},
		),
		writeSamplesTotal: promauto.With(registerer).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "thanos",
				Subsystem: "receive",
				Name:      "write_samples",
				Help:      "The number of sampled received in the incoming write requests.",
				Buckets:   []float64{10, 50, 100, 500, 1000, 5000, 10000},
			}, []string{"code", "tenant"},
		),
		pendingWriteRequests: promauto.With(registerer).NewGauge(
			prometheus.GaugeOpts{
				Name: "thanos_receive_pending_write_requests",
				Help: "The number of pending write requests.",
			},
		),
	}

	h.forwardRequests.WithLabelValues(labelSuccess)
	h.forwardRequests.WithLabelValues(labelError)
	h.replications.WithLabelValues(labelSuccess)
	h.replications.WithLabelValues(labelError)

	if o.ReplicationFactor > 1 {
		h.replicationFactor.Set(float64(o.ReplicationFactor))
	} else {
		h.replicationFactor.Set(1)
	}

	ins := extpromhttp.NewNopInstrumentationMiddleware()
	if o.Registry != nil {
		var buckets = []float64{0.001, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.25, 0.5, 0.75, 1, 2, 3, 4, 5}

		const bucketIncrement = 2.0
		for curMax := 5.0 + bucketIncrement; curMax < o.ForwardTimeout.Seconds(); curMax += bucketIncrement {
			buckets = append(buckets, curMax)
		}
		if buckets[len(buckets)-1] < o.ForwardTimeout.Seconds() {
			buckets = append(buckets, o.ForwardTimeout.Seconds())
		}

		ins = extpromhttp.NewTenantInstrumentationMiddleware(
			o.TenantHeader,
			o.DefaultTenantID,
			o.Registry,
			buckets,
		)
	}

	readyf := h.testReady
	instrf := func(name string, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
		next = ins.NewHandler(name, http.HandlerFunc(next))

		if o.Tracer != nil {
			next = tracing.HTTPMiddleware(o.Tracer, name, logger, http.HandlerFunc(next))
		}
		return next
	}

	h.router.Post(
		"/api/v1/receive",
		instrf(
			"receive",
			readyf(
				middleware.RequestID(
					http.HandlerFunc(h.receiveHTTP),
				),
			),
		),
	)

	h.router.Post(
		"/api/v1/otlp",
		instrf(
			"otlp",
			readyf(
				middleware.RequestID(
					http.HandlerFunc(h.receiveOTLPHTTP),
				),
			),
		),
	)

	statusAPI := statusapi.New(statusapi.Options{
		GetStats: h.getStats,
		Registry: h.options.Registry,
	})
	statusAPI.Register(h.router, o.Tracer, logger, ins, logging.NewHTTPServerMiddleware(logger))

	errlog := stdlog.New(log.NewStdlibAdapter(level.Error(h.logger)), "", 0)

	h.httpSrv = &http.Server{
		Handler:   h.router,
		ErrorLog:  errlog,
		TLSConfig: h.options.TLSConfig,
	}

	return h
}

// Hashring sets the hashring for the handler and marks the hashring as ready.
// The hashring must be set to a non-nil value in order for the
// handler to be ready and usable.
// If the hashring is nil, then the handler is marked as not ready.
func (h *Handler) Hashring(hashring Hashring) {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	if h.hashring != nil {
		previousNodes := h.hashring.Nodes()
		newNodes := hashring.Nodes()

		disappearedNodes := getSortedStringSliceDiff(previousNodes, newNodes)
		for _, node := range disappearedNodes {
			if err := h.peers.close(node); err != nil {
				level.Error(h.logger).Log("msg", "closing gRPC connection failed, we might have leaked a file descriptor", "addr", node, "err", err.Error())
			}
		}

		h.hashring.Close()
	}

	h.hashring = hashring
	h.peers.reset()
}

// getSortedStringSliceDiff returns items which are in slice1 but not in slice2.
// The returned slice also only contains unique items i.e. it is a set.
func getSortedStringSliceDiff(slice1, slice2 []Endpoint) []Endpoint {
	slice1Items := make(map[Endpoint]struct{}, len(slice1))
	slice2Items := make(map[Endpoint]struct{}, len(slice2))

	for _, s1 := range slice1 {
		slice1Items[s1] = struct{}{}
	}
	for _, s2 := range slice2 {
		slice2Items[s2] = struct{}{}
	}

	var difference = make([]Endpoint, 0)
	for s1 := range slice1Items {
		_, s2Contains := slice2Items[s1]
		if s2Contains {
			continue
		}
		difference = append(difference, s1)
	}
	slices.SortFunc(difference, func(a, b Endpoint) int {
		return strings.Compare(a.String(), b.String())
	})

	return difference
}

// Verifies whether the server is ready or not.
func (h *Handler) isReady() bool {
	h.mtx.RLock()
	hr := h.hashring != nil
	sr := h.writer != nil
	h.mtx.RUnlock()
	return sr && hr
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

func getStatsLimitParameter(r *http.Request) (int, error) {
	statsLimitStr := r.URL.Query().Get(LimitStatsQueryParam)
	if statsLimitStr == "" {
		return DefaultStatsLimit, nil
	}
	statsLimit, err := strconv.ParseInt(statsLimitStr, 10, 0)
	if err != nil {
		return 0, fmt.Errorf("unable to parse '%s' parameter: %w", LimitStatsQueryParam, err)
	}
	if statsLimit > math.MaxInt {
		return 0, fmt.Errorf("'%s' parameter is larger than %d", LimitStatsQueryParam, math.MaxInt)
	}
	return int(statsLimit), nil
}

func (h *Handler) getStats(r *http.Request, statsByLabelName string) ([]api.TenantStats, *api.ApiError) {
	if !h.isReady() {
		return nil, &api.ApiError{Typ: api.ErrorInternal, Err: fmt.Errorf("service unavailable")}
	}

	tenantID := r.Header.Get(h.options.TenantHeader)
	getAllTenantStats := r.FormValue(AllTenantsQueryParam) == "true"
	if getAllTenantStats && tenantID != "" {
		err := fmt.Errorf("using both the %s parameter and the %s header is not supported", AllTenantsQueryParam, h.options.TenantHeader)
		return nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}

	statsLimit, err := getStatsLimitParameter(r)
	if err != nil {
		return nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}

	if getAllTenantStats {
		return h.options.TSDBStats.TenantStats(statsLimit, statsByLabelName), nil
	}

	if tenantID == "" {
		tenantID = h.options.DefaultTenantID
	}

	return h.options.TSDBStats.TenantStats(statsLimit, statsByLabelName, tenantID), nil
}

// Close stops the Handler.
func (h *Handler) Close() {
	_ = h.peers.Close()
	runutil.CloseWithLogOnErr(h.logger, h.httpSrv, "receive HTTP server")
}

// Run serves the HTTP endpoints.
func (h *Handler) Run() error {
	level.Info(h.logger).Log("msg", "Start listening for connections", "address", h.options.ListenAddress)

	listener, err := net.Listen("tcp", h.options.ListenAddress)
	if err != nil {
		return err
	}

	// Monitor incoming connections with conntrack.
	listener = conntrack.NewListener(listener,
		conntrack.TrackWithName("http"),
		conntrack.TrackWithTracing())

	if h.options.TLSConfig != nil {
		level.Info(h.logger).Log("msg", "Serving HTTPS", "address", h.options.ListenAddress)
		// Cert & Key are already being passed in via TLSConfig.
		return h.httpSrv.ServeTLS(listener, "", "")
	}

	level.Info(h.logger).Log("msg", "Serving plain HTTP", "address", h.options.ListenAddress)
	return h.httpSrv.Serve(listener)
}

// replica encapsulates the replica number of a request and if the request is
// already replicated.
type replica struct {
	n          uint64
	replicated bool
}

// endpointReplica is a pair of a receive endpoint and a write request replica.
type endpointReplica struct {
	endpoint Endpoint
	replica  uint64
}

type trackedSeries struct {
	seriesIDs  []int
	timeSeries []prompb.TimeSeries
}

type writeResponse struct {
	seriesIDs []int
	err       error
	er        endpointReplica
}

func newWriteResponse(seriesIDs []int, err error, er endpointReplica) writeResponse {
	return writeResponse{
		seriesIDs: seriesIDs,
		err:       err,
		er:        er,
	}
}

func (h *Handler) receiveHTTP(w http.ResponseWriter, r *http.Request) {
	var err error
	span, ctx := tracing.StartSpan(r.Context(), "receive_http")
	span.SetTag("receiver.mode", string(h.receiverMode))
	defer span.Finish()

	tenantHTTP, err := tenancy.GetTenantFromHTTP(r, h.options.TenantHeader, h.options.DefaultTenantID, h.options.TenantField)
	if err != nil {
		level.Error(h.logger).Log("msg", "error getting tenant from HTTP", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	tLogger := log.With(h.logger, "tenant", tenantHTTP)
	span.SetTag("tenant", tenantHTTP)

	writeGate := h.Limiter.WriteGate()
	tracing.DoInSpan(r.Context(), "receive_write_gate_ismyturn", func(ctx context.Context) {
		err = writeGate.Start(r.Context())
	})
	defer writeGate.Done()
	if err != nil {
		level.Error(tLogger).Log("err", err, "msg", "internal server error")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	under, err := h.Limiter.HeadSeriesLimiter().isUnderLimit(tenantHTTP)
	if err != nil {
		level.Error(tLogger).Log("msg", "error while limiting", "err", err.Error())
	}

	// Fail request fully if tenant has exceeded set limit.
	if !under {
		http.Error(w, "tenant is above active series limit", http.StatusTooManyRequests)
		return
	}

	requestLimiter := h.Limiter.RequestLimiter()
	// io.ReadAll dynamically adjust the byte slice for read data, starting from 512B.
	// Since this is receive hot path, grow upfront saving allocations and CPU time.
	compressed := bytes.Buffer{}
	if r.ContentLength >= 0 {
		if !requestLimiter.AllowSizeBytes(tenantHTTP, r.ContentLength) {
			http.Error(w, "write request too large", http.StatusRequestEntityTooLarge)
			return
		}
		compressed.Grow(int(r.ContentLength))
	} else {
		compressed.Grow(512)
	}
	_, err = io.Copy(&compressed, r.Body)
	if err != nil {
		http.Error(w, errors.Wrap(err, "read compressed request body").Error(), http.StatusInternalServerError)
		return
	}
	reqBuf, err := s2.Decode(nil, compressed.Bytes())
	if err != nil {
		level.Error(tLogger).Log("msg", "snappy decode error", "err", err)
		http.Error(w, errors.Wrap(err, "snappy decode error").Error(), http.StatusBadRequest)
		return
	}

	if !requestLimiter.AllowSizeBytes(tenantHTTP, int64(len(reqBuf))) {
		http.Error(w, "write request too large", http.StatusRequestEntityTooLarge)
		return
	}

	// NOTE: Due to zero copy ZLabels, Labels used from WriteRequests keeps memory
	// from the whole request. Ensure that we always copy those when we want to
	// store them for longer time.
	var wreq prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &wreq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rep := uint64(0)
	// If the header is empty, we assume the request is not yet replicated.
	if replicaRaw := r.Header.Get(h.options.ReplicaHeader); replicaRaw != "" {
		if rep, err = strconv.ParseUint(replicaRaw, 10, 64); err != nil {
			http.Error(w, "could not parse replica header", http.StatusBadRequest)
			return
		}
	}

	// Exit early if the request contained no data. We don't support metadata yet. We also cannot fail here, because
	// this would mean lack of forward compatibility for remote write proto.
	if len(wreq.Timeseries) == 0 {
		// TODO(yeya24): Handle remote write metadata.
		if len(wreq.Metadata) > 0 {
			// TODO(bwplotka): Do we need this error message?
			level.Debug(tLogger).Log("msg", "only metadata from client; metadata ingestion not supported; skipping")
			return
		}
		level.Debug(tLogger).Log("msg", "empty remote write request; client bug or newer remote write protocol used?; skipping")
		return
	}

	if !requestLimiter.AllowSeries(tenantHTTP, int64(len(wreq.Timeseries))) {
		http.Error(w, "too many timeseries", http.StatusRequestEntityTooLarge)
		return
	}

	totalSamples := 0
	for _, timeseries := range wreq.Timeseries {
		totalSamples += len(timeseries.Samples)
	}
	if !requestLimiter.AllowSamples(tenantHTTP, int64(totalSamples)) {
		http.Error(w, "too many samples", http.StatusRequestEntityTooLarge)
		return
	}

	// Apply relabeling configs.
	h.relabel(&wreq)
	if len(wreq.Timeseries) == 0 {
		level.Debug(tLogger).Log("msg", "remote write request dropped due to relabeling.")
		return
	}

	responseStatusCode := http.StatusOK
	tenantStats, err := h.handleRequest(ctx, rep, tenantHTTP, &wreq)
	if err != nil {
		level.Debug(tLogger).Log("msg", "failed to handle request", "err", err.Error())
		switch errors.Cause(err) {
		case errNotReady:
			responseStatusCode = http.StatusServiceUnavailable
		case errUnavailable:
			responseStatusCode = http.StatusServiceUnavailable
		case errConflict:
			responseStatusCode = http.StatusConflict
		case errBadReplica:
			responseStatusCode = http.StatusBadRequest
		default:
			level.Error(tLogger).Log("err", err, "msg", "internal server error")
			responseStatusCode = http.StatusInternalServerError
		}
		http.Error(w, err.Error(), responseStatusCode)
	}

	for tenant, stats := range tenantStats {
		h.writeTimeseriesTotal.WithLabelValues(strconv.Itoa(responseStatusCode), tenant).Observe(float64(stats.timeseries))
		h.writeSamplesTotal.WithLabelValues(strconv.Itoa(responseStatusCode), tenant).Observe(float64(stats.totalSamples))
	}
}

type requestStats struct {
	timeseries   int
	totalSamples int
}

type tenantRequestStats map[string]requestStats

func (h *Handler) handleRequest(ctx context.Context, rep uint64, tenantHTTP string, wreq *prompb.WriteRequest) (tenantRequestStats, error) {
	tLogger := log.With(h.logger, "tenantHTTP", tenantHTTP)

	// This replica value is used to detect cycles in cyclic topologies.
	// A non-zero value indicates that the request has already been replicated by a previous receive instance.
	// For almost all users, this is only used in fully connected topologies of IngestorRouter instances.
	// For acyclic topologies that use RouterOnly and IngestorOnly instances, this causes issues when replicating data.
	// See discussion in: https://github.com/thanos-io/thanos/issues/4359.
	if h.receiverMode == RouterOnly || h.receiverMode == IngestorOnly {
		rep = 0
	}

	// The replica value in the header is one-indexed, thus we need >.
	if rep > h.options.ReplicationFactor {
		level.Error(tLogger).Log("err", errBadReplica, "msg", "write request rejected",
			"request_replica", rep, "replication_factor", h.options.ReplicationFactor)
		return tenantRequestStats{}, errBadReplica
	}

	r := replica{n: rep, replicated: rep != 0}

	// On the wire, format is 1-indexed and in-code is 0-indexed, so we decrement the value if it was already replicated.
	if r.replicated {
		r.n--
	}

	// Forward any time series as necessary. All time series
	// destined for the local node will be written to the receiver.
	// Time series will be replicated as necessary.
	return h.forward(ctx, tenantHTTP, r, wreq)
}

// forward accepts a write request, batches its time series by
// corresponding endpoint, and forwards them in parallel to the
// correct endpoint. Requests destined for the local node are written
// the local receiver. For a given write request, at most one outgoing
// write request will be made to every other node in the hashring,
// unless the request needs to be replicated.
// The function only returns when all requests have finished
// or the context is canceled.
func (h *Handler) forward(ctx context.Context, tenantHTTP string, r replica, wreq *prompb.WriteRequest) (tenantRequestStats, error) {
	span, ctx := tracing.StartSpan(ctx, "receive_fanout_forward")
	defer span.Finish()

	var replicas []uint64
	if r.replicated {
		replicas = []uint64{r.n}
	} else {
		for rn := uint64(0); rn < h.options.ReplicationFactor; rn++ {
			replicas = append(replicas, rn)
		}
	}

	params := remoteWriteParams{
		tenant:            tenantHTTP,
		writeRequest:      wreq,
		replicas:          replicas,
		alreadyReplicated: r.replicated,
	}

	return h.fanoutForward(ctx, params)
}

type remoteWriteParams struct {
	tenant            string
	writeRequest      *prompb.WriteRequest
	replicas          []uint64
	alreadyReplicated bool
}

func (h *Handler) gatherWriteStats(rf int, writes ...map[endpointReplica]map[string]trackedSeries) tenantRequestStats {
	var stats = make(tenantRequestStats)

	for _, write := range writes {
		for er := range write {
			for tenant, series := range write[er] {
				samples := 0

				for _, ts := range series.timeSeries {
					samples += len(ts.Samples)
				}

				if st, ok := stats[tenant]; ok {
					st.timeseries += len(series.timeSeries)
					st.totalSamples += samples

					stats[tenant] = st
				} else {
					stats[tenant] = requestStats{
						timeseries:   len(series.timeSeries),
						totalSamples: samples,
					}
				}
			}
		}
	}

	// adjust counters by the replication factor
	for tenant, st := range stats {
		st.timeseries /= rf
		st.totalSamples /= rf
		stats[tenant] = st
	}

	return stats
}

func (h *Handler) fanoutForward(ctx context.Context, params remoteWriteParams) (tenantRequestStats, error) {
	ctx, cancel := context.WithTimeout(tracing.CopyTraceContext(context.Background(), ctx), h.options.ForwardTimeout)

	var writeErrors writeErrors
	var stats = make(tenantRequestStats)

	// If all series reached the success threshold, we don't cancel the context
	// so that in-flight forward requests can optimistically complete until timeout,
	// improving the chance of full replication. On failure we cancel immediately.
	optimisticallyWaitForSuccesses := false
	defer func() {
		if !optimisticallyWaitForSuccesses {
			cancel()
		}
	}()

	logTags := []any{"tenant", params.tenant}
	if id, ok := middleware.RequestIDFromContext(ctx); ok {
		logTags = append(logTags, "request-id", id)
	}
	requestLogger := log.With(h.logger, logTags...)

	localWrites, remoteWrites, err := h.distributeTimeseriesToReplicas(params.tenant, params.replicas, params.writeRequest.Timeseries)
	if err != nil {
		level.Error(requestLogger).Log("msg", "failed to distribute timeseries to replicas", "err", err)
		return stats, err
	}

	stats = h.gatherWriteStats(len(params.replicas), localWrites, remoteWrites)

	// Prepare a buffered channel to receive the responses from the local and remote writes. Remote writes will all go
	// asynchronously and with this capacity we will never block on writing to the channel.
	var maxBufferedResponses int
	for er := range localWrites {
		maxBufferedResponses += len(localWrites[er])
	}
	for er := range remoteWrites {
		maxBufferedResponses += len(remoteWrites[er])
	}

	responses := make(chan writeResponse, maxBufferedResponses)
	wg := sync.WaitGroup{}

	h.sendWrites(ctx, &wg, params, localWrites, remoteWrites, responses)

	go func() {
		wg.Wait()
		close(responses)
	}()

	// At the end, make sure to exhaust the channel, letting remaining unnecessary requests finish asynchronously.
	// This is needed if context is canceled or if we reached success or fail quorum faster.
	defer func() {
		go func() {
			for resp := range responses {
				if resp.err != nil {
					level.Debug(requestLogger).Log("msg", "request failed, but not needed to achieve quorum", "err", resp.err)
				}
			}
		}()
	}()

	successThreshold := h.writeQuorum()
	if params.alreadyReplicated {
		successThreshold = 1
	}
	// failureThreshold is the number of failures after which a series can no
	// longer reach the success threshold. For RF=3 and successThreshold=2 this is 2.
	failureThreshold := len(params.replicas) - successThreshold + 1
	successes := make([]int, len(params.writeRequest.Timeseries))
	failures := make([]int, len(params.writeRequest.Timeseries))
	seriesErrs := newReplicationErrors(successThreshold, len(params.writeRequest.Timeseries))
	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case resp, hasMore := <-responses:
			if !hasMore {
				for i, seriesErr := range seriesErrs {
					if failures[i] >= failureThreshold {
						writeErrors.Add(seriesErr)
					}
				}
				return stats, writeErrors.ErrOrNil()
			}

			if resp.err != nil {
				for _, seriesID := range resp.seriesIDs {
					seriesErrs[seriesID].Add(resp.err)
					failures[seriesID]++
				}
			} else {
				for _, seriesID := range resp.seriesIDs {
					successes[seriesID]++
				}
			}

			if canReturnEarly(successes, failures, successThreshold, failureThreshold) {
				var hadErrors bool
				for i, seriesErr := range seriesErrs {
					if failures[i] >= failureThreshold {
						writeErrors.Add(seriesErr)
						hadErrors = true
					}
				}
				optimisticallyWaitForSuccesses = !hadErrors
				return stats, writeErrors.ErrOrNil()
			}
		}
	}
}

// distributeTimeseriesToReplicas distributes the given timeseries from the tenant to different endpoints in a manner
// that achieves the replication factor indicated by replicas.
// The first return value are the series that should be written to the local node. The second return value are the
// series that should be written to remote nodes.
func (h *Handler) distributeTimeseriesToReplicas(
	tenantHTTP string,
	replicas []uint64,
	timeseries []prompb.TimeSeries,
) (map[endpointReplica]map[string]trackedSeries, map[endpointReplica]map[string]trackedSeries, error) {
	h.mtx.RLock()
	defer h.mtx.RUnlock()
	remoteWrites := make(map[endpointReplica]map[string]trackedSeries)
	localWrites := make(map[endpointReplica]map[string]trackedSeries)
	for tsIndex, ts := range timeseries {
		var tenant = tenantHTTP

		if h.splitTenantLabelName != "" {
			lbls := labelpb.ZLabelsToPromLabels(ts.Labels)

			tenantLabel := lbls.Get(h.splitTenantLabelName)
			if tenantLabel != "" {
				tenant = tenantLabel

				newLabels := labels.NewBuilder(lbls)
				newLabels.Del(h.splitTenantLabelName)

				ts.Labels = labelpb.ZLabelsFromPromLabels(
					newLabels.Labels(),
				)
			}
		}

		for _, rn := range replicas {
			endpoint, err := h.hashring.GetN(tenant, &ts, rn)
			if err != nil {
				return nil, nil, err
			}
			endpointReplica := endpointReplica{endpoint: endpoint, replica: rn}
			var writeDestination = remoteWrites
			if endpoint.HasAddress(h.options.Endpoint) {
				writeDestination = localWrites
			}
			writeableSeries, ok := writeDestination[endpointReplica]
			if !ok {
				writeDestination[endpointReplica] = map[string]trackedSeries{
					tenant: {
						seriesIDs:  make([]int, 0),
						timeSeries: make([]prompb.TimeSeries, 0),
					},
				}
			}
			tenantSeries := writeableSeries[tenant]

			tenantSeries.timeSeries = append(tenantSeries.timeSeries, ts)
			tenantSeries.seriesIDs = append(tenantSeries.seriesIDs, tsIndex)

			writeDestination[endpointReplica][tenant] = tenantSeries
		}
	}
	return localWrites, remoteWrites, nil
}

// sendWrites sends the local and remote writes to execute concurrently, controlling them through the provided sync.WaitGroup.
// The responses from the writes are sent to the responses channel.
func (h *Handler) sendWrites(
	ctx context.Context,
	wg *sync.WaitGroup,
	params remoteWriteParams,
	localWrites map[endpointReplica]map[string]trackedSeries,
	remoteWrites map[endpointReplica]map[string]trackedSeries,
	responses chan writeResponse,
) {
	// Do the writes to the local node first. This should be easy and fast.
	for writeDestination := range localWrites {
		func(writeDestination endpointReplica) {
			for tenant, trackedSeries := range localWrites[writeDestination] {
				h.sendLocalWrite(ctx, writeDestination, tenant, trackedSeries, responses)
			}
		}(writeDestination)
	}

	// Do the writes to remote nodes. Run them all in parallel.
	for writeDestination := range remoteWrites {
		for tenant, trackedSeries := range remoteWrites[writeDestination] {
			wg.Add(1)

			h.sendRemoteWrite(ctx, tenant, writeDestination, trackedSeries, params.alreadyReplicated, responses, wg)
		}
	}
}

// sendLocalWrite sends a write request to the local node.
// The responses are sent to the responses channel.
func (h *Handler) sendLocalWrite(
	ctx context.Context,
	writeDestination endpointReplica,
	tenantHTTP string,
	trackedSeries trackedSeries,
	responses chan<- writeResponse,
) {
	span, tracingCtx := tracing.StartSpan(ctx, "receive_local_tsdb_write")
	defer span.Finish()
	span.SetTag("endpoint", writeDestination.endpoint)
	span.SetTag("replica", writeDestination.replica)

	tenantSeriesMapping := map[string][]prompb.TimeSeries{}
	for _, ts := range trackedSeries.timeSeries {
		var tenant = tenantHTTP
		if h.splitTenantLabelName != "" {
			lbls := labelpb.ZLabelsToPromLabels(ts.Labels)
			if tnt := lbls.Get(h.splitTenantLabelName); tnt != "" {
				tenant = tnt
			}
		}
		tenantSeriesMapping[tenant] = append(tenantSeriesMapping[tenant], ts)
	}

	for tenant, series := range tenantSeriesMapping {
		err := h.writer.Write(tracingCtx, tenant, series)
		if err != nil {
			span.SetTag("error", true)
			span.SetTag("error.msg", err.Error())
			responses <- newWriteResponse(trackedSeries.seriesIDs, err, writeDestination)
			return
		}
	}
	responses <- newWriteResponse(trackedSeries.seriesIDs, nil, writeDestination)

}

// sendRemoteWrite sends a write request to the remote node. It takes care of checking whether the endpoint is up or not
// in the peerGroup, correctly marking them as up or down when appropriate.
// The responses are sent to the responses channel.
func (h *Handler) sendRemoteWrite(
	ctx context.Context,
	tenant string,
	endpointReplica endpointReplica,
	trackedSeries trackedSeries,
	alreadyReplicated bool,
	responses chan writeResponse,
	wg *sync.WaitGroup,
) {
	endpoint := endpointReplica.endpoint
	cl, err := h.peers.getConnection(ctx, endpoint)
	if err != nil {
		if errors.Is(err, errUnavailable) {
			err = errors.Wrapf(errUnavailable, "backing off forward request for endpoint %v", endpointReplica)
		}
		responses <- newWriteResponse(trackedSeries.seriesIDs, err, endpointReplica)
		wg.Done()
		return
	}

	// This is called "real" because it's 1-indexed.
	realReplicationIndex := int64(endpointReplica.replica + 1)
	// Actually make the request against the endpoint we determined should handle these time series.
	cl.RemoteWriteAsync(ctx, &storepb.WriteRequest{
		Timeseries: trackedSeries.timeSeries,
		Tenant:     tenant,
		// Increment replica since on-the-wire format is 1-indexed and 0 indicates un-replicated.
		Replica: realReplicationIndex,
	}, endpointReplica, trackedSeries.seriesIDs, responses, func(err error) {
		if err == nil {
			h.forwardRequests.WithLabelValues(labelSuccess).Inc()
			if !alreadyReplicated {
				h.replications.WithLabelValues(labelSuccess).Inc()
			}
			h.peers.markPeerAvailable(endpoint)
		} else {
			// Only increment error metrics if the error is not AlreadyExists.
			if st, ok := status.FromError(err); !ok || st.Code() != codes.AlreadyExists {
				h.forwardRequests.WithLabelValues(labelError).Inc()
				if !alreadyReplicated {
					h.replications.WithLabelValues(labelError).Inc()
				}
			}

			// Check if peer connection is unavailable, update the peer state to avoid spamming that peer.
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Unavailable {
					h.peers.markPeerUnavailable(endpointReplica.endpoint)
				}
			}
		}
		wg.Done()
	})
}

// writeQuorum returns minimum number of replicas that has to confirm write success before claiming replication success.
func (h *Handler) writeQuorum() int {
	// NOTE(GiedriusS): this is here because otherwise RF=2 doesn't make sense as all writes
	// would need to succeed all the time. Another way to think about it is when migrating
	// from a Sidecar based setup with 2 Prometheus nodes to a Receiver setup, we want to
	// keep the same guarantees.
	if h.options.ReplicationFactor == 2 {
		return 1
	}
	return int((h.options.ReplicationFactor / 2) + 1)
}

// canReturnEarly returns true when every series in the request has a
// determined outcome: either it reached the success threshold or it accumulated
// enough failures that reaching the success threshold is no longer possible.
func canReturnEarly(successes, failures []int, successThreshold, failureThreshold int) bool {
	for i := range successes {
		if successes[i] < successThreshold && failures[i] < failureThreshold {
			return false
		}
	}
	return true
}

// RemoteWrite implements the gRPC remote write handler for storepb.WriteableStore.
func (h *Handler) RemoteWrite(ctx context.Context, r *storepb.WriteRequest) (*storepb.WriteResponse, error) {
	span, ctx := tracing.StartSpan(ctx, "receive_grpc")
	defer span.Finish()

	h.pendingWriteRequests.Set(float64(h.pendingWriteRequestsCounter.Add(1)))
	defer h.pendingWriteRequestsCounter.Add(-1)

	// Fast path for IngestorOnly mode: write directly to local TSDB.
	// This skips distributeTimeseriesToReplicas and sendLocalWrite since
	// the Router already determined this data belongs to this node.
	if h.receiverMode == IngestorOnly {
		err := h.writer.Write(ctx, r.Tenant, r.Timeseries)
		if err != nil {
			level.Debug(h.logger).Log("msg", "failed to write to local TSDB", "err", err)
		}
		switch cause := errors.Cause(err); cause {
		case nil:
			return &storepb.WriteResponse{}, nil
		default:
			if isNotReady(cause) {
				return nil, status.Error(codes.Unavailable, err.Error())
			}
			if isConflict(cause) {
				return nil, status.Error(codes.AlreadyExists, err.Error())
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	_, err := h.handleRequest(ctx, uint64(r.Replica), r.Tenant, &prompb.WriteRequest{Timeseries: r.Timeseries})
	if err != nil {
		level.Debug(h.logger).Log("msg", "failed to handle request", "err", err)
	}
	switch errors.Cause(err) {
	case nil:
		return &storepb.WriteResponse{}, nil
	case errNotReady:
		return nil, status.Error(codes.Unavailable, err.Error())
	case errUnavailable:
		return nil, status.Error(codes.Unavailable, err.Error())
	case errConflict:
		return nil, status.Error(codes.AlreadyExists, err.Error())
	case errBadReplica:
		return nil, status.Error(codes.InvalidArgument, err.Error())
	default:
		return nil, status.Error(codes.Internal, err.Error())
	}
}

// relabel relabels the time series labels in the remote write request.
func (h *Handler) relabel(wreq *prompb.WriteRequest) {
	if len(h.options.RelabelConfigs) == 0 {
		return
	}
	timeSeries := make([]prompb.TimeSeries, 0, len(wreq.Timeseries))
	for _, ts := range wreq.Timeseries {
		var keep bool
		lbls, keep := relabel.Process(labelpb.ZLabelsToPromLabels(ts.Labels), h.options.RelabelConfigs...)
		if !keep {
			continue
		}
		ts.Labels = labelpb.ZLabelsFromPromLabels(lbls)
		timeSeries = append(timeSeries, ts)
	}
	wreq.Timeseries = timeSeries
}

// isConflict returns whether or not the given error represents a conflict.
func isConflict(err error) bool {
	if err == nil {
		return false
	}
	return err == errConflict ||
		isSampleConflictErr(err) ||
		isExemplarConflictErr(err) ||
		isLabelsConflictErr(err) ||
		status.Code(err) == codes.AlreadyExists
}

// isSampleConflictErr returns whether or not the given error represents
// a sample-related conflict.
func isSampleConflictErr(err error) bool {
	return err == storage.ErrDuplicateSampleForTimestamp ||
		err == storage.ErrOutOfOrderSample ||
		err == storage.ErrOutOfBounds ||
		err == storage.ErrTooOldSample
}

// isExemplarConflictErr returns whether or not the given error represents
// a exemplar-related conflict.
func isExemplarConflictErr(err error) bool {
	return err == storage.ErrDuplicateExemplar ||
		err == storage.ErrOutOfOrderExemplar ||
		err == storage.ErrExemplarLabelLength
}

// isLabelsConflictErr returns whether or not the given error represents
// a labels-related conflict.
func isLabelsConflictErr(err error) bool {
	return err == labelpb.ErrDuplicateLabels ||
		err == labelpb.ErrEmptyLabels ||
		err == labelpb.ErrOutOfOrderLabels
}

// isNotReady returns whether or not the given error represents a not ready error.
func isNotReady(err error) bool {
	return err == errNotReady ||
		err == tsdb.ErrNotReady ||
		status.Code(err) == codes.Unavailable
}

// isUnavailable returns whether or not the given error represents an unavailable error.
func isUnavailable(err error) bool {
	return err == errUnavailable ||
		status.Code(err) == codes.Unavailable
}

// retryState encapsulates the number of request attempt made against a peer and,
// next allowed time for the next attempt.
type retryState struct {
	attempt     float64
	nextAllowed time.Time
}

type expectedErrors []*expectedError

type expectedError struct {
	err   error
	cause func(error) bool
	count int
}

func (a expectedErrors) Len() int           { return len(a) }
func (a expectedErrors) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a expectedErrors) Less(i, j int) bool { return a[i].count < a[j].count }

// errorSet is a set of errors.
type errorSet struct {
	reasonSet map[string]struct{}
	errs      []error
}

// Error returns a string containing a deduplicated set of reasons.
func (es errorSet) Error() string {
	if len(es.reasonSet) == 0 {
		return ""
	}
	reasons := make([]string, 0, len(es.reasonSet))
	for reason := range es.reasonSet {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)

	var buf bytes.Buffer
	if len(reasons) > 1 {
		fmt.Fprintf(&buf, "%d errors: ", len(es.reasonSet))
	}

	var more bool
	for _, reason := range reasons {
		if more {
			buf.WriteString("; ")
		}
		buf.WriteString(reason)
		more = true
	}

	return buf.String()
}

// Add adds an error to the errorSet.
func (es *errorSet) Add(err error) {
	if err == nil {
		return
	}

	if len(es.errs) == 0 {
		es.errs = []error{err}
	} else {
		es.errs = append(es.errs, err)
	}
	if es.reasonSet == nil {
		es.reasonSet = make(map[string]struct{})
	}

	switch addedErr := err.(type) {
	case *replicationErrors:
		for reason := range addedErr.reasonSet {
			es.reasonSet[reason] = struct{}{}
		}
	case *writeErrors:
		for reason := range addedErr.reasonSet {
			es.reasonSet[reason] = struct{}{}
		}
	default:
		es.reasonSet[err.Error()] = struct{}{}
	}
}

// writeErrors contains all errors that have
// occurred during a local write of a remote-write request.
type writeErrors struct {
	errorSet
}

// ErrOrNil returns the writeErrors instance if any
// errors are contained in it.
// Otherwise, it returns nil.
func (es *writeErrors) ErrOrNil() error {
	if len(es.errs) == 0 {
		return nil
	}
	return es
}

// Cause returns the primary cause for a write failure.
// If multiple errors have occurred, Cause will prefer
// recoverable over non-recoverable errors.
func (es *writeErrors) Cause() error {
	if len(es.errs) == 0 {
		return nil
	}

	expErrs := expectedErrors{
		{err: errUnavailable, cause: isUnavailable},
		{err: errNotReady, cause: isNotReady},
		{err: errConflict, cause: isConflict},
	}

	var (
		unknownErr error
		knownCause bool
	)
	for _, werr := range es.errs {
		knownCause = false
		cause := errors.Cause(werr)
		for _, exp := range expErrs {
			if exp.cause(cause) {
				knownCause = true
				exp.count++
			}
		}
		if !knownCause {
			unknownErr = cause
		}
	}

	for _, exp := range expErrs {
		if exp.count > 0 {
			return exp.err
		}
	}

	return unknownErr
}

// replicationErrors contains errors that have happened while
// replicating a time series within a remote-write request.
type replicationErrors struct {
	errorSet
	threshold int
}

// Cause extracts a sentinel error with the highest occurrence that
// has happened more than the given threshold.
// If no single error has occurred more than the threshold, but the
// total number of errors meets the threshold,
// replicationErr will return errInternal.
func (es *replicationErrors) Cause() error {
	if len(es.errs) == 0 {
		return errorSet{}
	}

	expErrs := expectedErrors{
		{err: errConflict, cause: isConflict},
		{err: errNotReady, cause: isNotReady},
		{err: errUnavailable, cause: isUnavailable},
	}
	for _, exp := range expErrs {
		exp.count = 0
		for _, err := range es.errs {
			if exp.cause(errors.Cause(err)) {
				exp.count++
			}
		}
	}

	// Determine which error occurred most.
	sort.Sort(sort.Reverse(expErrs))
	if exp := expErrs[0]; exp.count >= es.threshold {
		return exp.err
	}

	if len(es.errs) >= es.threshold {
		return errInternal
	}

	return nil
}

func newReplicationErrors(threshold, numErrors int) []*replicationErrors {
	errs := make([]*replicationErrors, numErrors)
	for i := range errs {
		errs[i] = &replicationErrors{threshold: threshold}
	}
	return errs
}

func newPeerWorker(client peerClient, forwardDelay prometheus.Histogram, asyncWorkerCount uint, maxArtificialDelay time.Duration) *peerWorker {
	return &peerWorker{
		client:             client,
		wp:                 pool.NewWorkerPool(asyncWorkerCount),
		forwardDelay:       forwardDelay,
		maxArtificialDelay: maxArtificialDelay,
	}
}

func (pw *peerWorker) RemoteWrite(ctx context.Context, in *storepb.WriteRequest, opts ...grpc.CallOption) (*storepb.WriteResponse, error) {
	return pw.client.RemoteWrite(ctx, in)
}

type peerClient interface {
	storepb.WriteableStoreClient
	io.Closer
}

type protobufPeer struct {
	storepb.WriteableStoreClient
	conn *grpc.ClientConn
}

func newProtobufPeer(conn *grpc.ClientConn) *protobufPeer {
	return &protobufPeer{
		WriteableStoreClient: storepb.NewWriteableStoreClient(conn),
		conn:                 conn,
	}
}

func (p protobufPeer) Close() error {
	return p.conn.Close()
}

type peerWorker struct {
	client peerClient
	wp     pool.WorkerPool

	forwardDelay       prometheus.Histogram
	maxArtificialDelay time.Duration
}

func newPeerGroup(
	logger log.Logger,
	backoff backoff.Backoff,
	forwardDelay prometheus.Histogram,
	asyncForwardWorkersCount uint,
	maxArtificialDelay time.Duration,
	replicationProtocol ReplicationProtocol,
	dialOpts ...grpc.DialOption,
) *peerGroup {
	return &peerGroup{
		logger:                   logger,
		dialOpts:                 dialOpts,
		connections:              map[Endpoint]*peerWorker{},
		m:                        sync.RWMutex{},
		dialer:                   grpc.NewClient,
		peerStates:               make(map[Endpoint]*retryState),
		expBackoff:               backoff,
		forwardDelay:             forwardDelay,
		maxArtificialDelay:       maxArtificialDelay,
		asyncForwardWorkersCount: asyncForwardWorkersCount,
		replicationProtocol:      replicationProtocol,
	}
}

type peersContainer interface {
	close(Endpoint) error
	getConnection(context.Context, Endpoint) (WriteableStoreAsyncClient, error)
	markPeerUnavailable(Endpoint)
	markPeerAvailable(Endpoint)
	reset()
	io.Closer
}

func (p *peerWorker) RemoteWriteAsync(ctx context.Context, req *storepb.WriteRequest, er endpointReplica, seriesIDs []int, responseWriter chan writeResponse, cb func(error)) {
	now := time.Now()
	p.wp.Go(func() {
		if p.maxArtificialDelay > 0 {
			var randDuration = time.Duration(rand.Int63n(int64(p.maxArtificialDelay)))
			if randDuration < 1*time.Second {
				randDuration = 1 * time.Second
			}

			select {
			case <-time.After(randDuration):
			case <-ctx.Done():
			}
		}
		p.forwardDelay.Observe(time.Since(now).Seconds())

		tracing.DoInSpan(ctx, "receive_forward", func(ctx context.Context) {
			_, err := p.client.RemoteWrite(ctx, req)
			responseWriter <- newWriteResponse(
				seriesIDs,
				errors.Wrapf(err, "forwarding request to endpoint %v", er.endpoint),
				er,
			)
			if err != nil {
				sp := trace.SpanFromContext(ctx)
				sp.SetAttributes(attribute.Bool("error", true))
				sp.SetAttributes(attribute.String("error.msg", err.Error()))
			}
			cb(err)
		}, opentracing.Tags{
			"endpoint": er.endpoint,
			"replica":  er.replica,
		})
	})
}

type peerGroup struct {
	logger                   log.Logger
	dialOpts                 []grpc.DialOption
	connections              map[Endpoint]*peerWorker
	peerStates               map[Endpoint]*retryState
	expBackoff               backoff.Backoff
	forwardDelay             prometheus.Histogram
	asyncForwardWorkersCount uint
	replicationProtocol      ReplicationProtocol
	maxArtificialDelay       time.Duration

	m sync.RWMutex

	conns atomic.Uint64

	// dialer is used for testing.
	dialer func(target string, opts ...grpc.DialOption) (conn *grpc.ClientConn, err error)
}

func (p *peerGroup) Close() error {
	for _, c := range p.connections {
		c.wp.Close()
	}
	return nil
}

func (p *peerGroup) close(endpoint Endpoint) error {
	p.m.Lock()
	defer p.m.Unlock()

	c, ok := p.connections[endpoint]
	if !ok {
		// NOTE(GiedriusS): this could be valid case when the connection
		// was never established.
		return nil
	}

	p.connections[endpoint].wp.Close()
	delete(p.connections, endpoint)
	if err := c.client.Close(); err != nil {
		return fmt.Errorf("closing connection for %s", endpoint)
	}

	return nil
}

func (p *peerGroup) getConnection(ctx context.Context, endpoint Endpoint) (WriteableStoreAsyncClient, error) {
	if !p.isPeerUp(endpoint) {
		return nil, errUnavailable
	}

	// use a RLock first to prevent blocking if we don't need to.
	p.m.RLock()
	c, ok := p.connections[endpoint]
	p.m.RUnlock()
	if ok {
		return c, nil
	}

	p.m.Lock()
	defer p.m.Unlock()
	// Make sure that another caller hasn't created the connection since obtaining the write lock.
	c, ok = p.connections[endpoint]
	if ok {
		return c, nil
	}

	p.conns.Inc()

	var client peerClient
	switch p.replicationProtocol {
	case CapNProtoReplication:
		client = writecapnp.NewRemoteWriteClient(writecapnp.NewTCPDialer(endpoint.CapNProtoAddress), p.logger)

	case ProtobufReplication:
		conn, err := p.dialer(endpoint.Address, p.dialOpts...)
		if err != nil {
			p.markPeerUnavailableUnlocked(endpoint)
			dialError := errors.Wrap(err, "failed to dial peer")
			return nil, errors.Wrap(dialError, errUnavailable.Error())
		}
		client = newProtobufPeer(conn)
	default:
		return nil, errors.Errorf("unknown replication protocol %v", p.replicationProtocol)
	}

	var delay time.Duration
	if p.conns.Load() == 2 {
		delay = p.maxArtificialDelay
	}

	p.connections[endpoint] = newPeerWorker(client, p.forwardDelay, p.asyncForwardWorkersCount, delay)
	return p.connections[endpoint], nil
}

func (p *peerGroup) markPeerUnavailable(addr Endpoint) {
	p.m.Lock()
	defer p.m.Unlock()

	p.markPeerUnavailableUnlocked(addr)
}

func (p *peerGroup) markPeerUnavailableUnlocked(addr Endpoint) {
	state, ok := p.peerStates[addr]
	if !ok {
		state = &retryState{attempt: -1}
	}
	state.attempt++
	state.nextAllowed = time.Now().Add(p.expBackoff.ForAttempt(state.attempt))
	p.peerStates[addr] = state
}

func (p *peerGroup) markPeerAvailable(addr Endpoint) {
	p.m.Lock()
	defer p.m.Unlock()
	delete(p.peerStates, addr)
}

func (p *peerGroup) isPeerUp(addr Endpoint) bool {
	p.m.RLock()
	defer p.m.RUnlock()
	state, ok := p.peerStates[addr]
	if !ok {
		return true
	}
	return time.Now().After(state.nextAllowed)
}

func (p *peerGroup) reset() {
	p.expBackoff.Reset()
	p.peerStates = make(map[Endpoint]*retryState)
}
