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

	"github.com/colega/zeropool"

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
	writev2 "github.com/thanos-io/thanos/pkg/store/storepb/prompb/io/prometheus/write/v2"
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

	errValidation = errors.New("validation error")
)

type WriteableStoreAsyncClient interface {
	storepb.WriteableStoreClient
	RemoteWriteAsync(context.Context, *storepb.WriteRequest, endpointReplica, []int, chan writeResponse, func(error))
	// TryRemoteWriteAsync submits the request without blocking. Returns false if the peer's
	// worker pool is at capacity; the caller should fall back to RemoteWriteAsync.
	TryRemoteWriteAsync(context.Context, *storepb.WriteRequest, endpointReplica, []int, chan writeResponse, func(error)) bool
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

	seriesIDsPool     zeropool.Pool[[]int]
	timeSeriesPool    zeropool.Pool[[]prompb.TimeSeries]
	distributeMapPool zeropool.Pool[map[endpointReplica]map[string]trackedSeries]
	trackedSeries     zeropool.Pool[map[string]trackedSeries]
	intScratchPool    zeropool.Pool[[]int]
}

// getIntScratch returns a zeroed []int of length n from intScratchPool.
// clear is required: reslicing a pooled slice back up exposes stale values.
func (h *Handler) getIntScratch(n int) []int {
	s := h.intScratchPool.Get()
	if cap(s) < n {
		return make([]int, n)
	}
	s = s[:n]
	clear(s)
	return s
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
			promauto.With(registerer).NewHistogramVec(
				prometheus.HistogramOpts{
					Name:                           "thanos_receive_forward_delay_seconds",
					Help:                           "The delay between the time the request was received and the time it was forwarded to a worker. ",
					Buckets:                        prometheus.ExponentialBuckets(0.001, 2, 16),
					NativeHistogramBucketFactor:    1.1,
					NativeHistogramMaxBucketNumber: 100,
				}, []string{"worker"},
			),
			workers,
			o.Endpoint,
			o.Writer,
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

func determineRWVersion(r *http.Request) (int, error) {
	if r.Header.Get("X-Prometheus-Remote-Write-Version") != "2.0.0" {
		return 1, nil
	}
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		return 0, fmt.Errorf("missing Content-Type header")
	}
	if ct == "application/x-protobuf;proto=io.prometheus.write.v2.Request" {
		return 2, nil
	}
	if ct == "application/x-protobuf" {
		return 1, nil
	}
	if ct == "application/x-protobuf;proto=prometheus.WriteRequest" {
		return 1, nil
	}
	return 0, fmt.Errorf("required headers Content-Type and/or X-Prometheus-Remote-Write-Version not found")
}

func translateV2ToV1(w writev2.Request) *prompb.WriteRequest {
	// TODO(GiedriusS): somehow ensure programmatically that all fields are set and we don't miss anything.
	out := &prompb.WriteRequest{
		Timeseries: make([]prompb.TimeSeries, 0, len(w.Timeseries)),
	}

	for _, t := range w.Timeseries {
		v1Ts := prompb.TimeSeries{}

		v1Ts.Labels = make([]labelpb.ZLabel, 0, len(t.LabelsRefs)/2)
		for i := 0; i+1 < len(t.LabelsRefs); i += 2 {
			v1Ts.Labels = append(v1Ts.Labels, labelpb.ZLabel{
				Name:  w.Symbols[t.LabelsRefs[i]],
				Value: w.Symbols[t.LabelsRefs[i+1]],
			})
		}

		if len(t.Samples) > 0 {
			v1Ts.Samples = make([]prompb.Sample, 0, len(t.Samples))
			for _, v2s := range t.Samples {
				v1Ts.Samples = append(v1Ts.Samples, prompb.Sample{
					Timestamp: v2s.Timestamp,
					Value:     v2s.Value,
				})
			}
		}

		if len(t.Exemplars) > 0 {
			v1Ts.Exemplars = make([]prompb.Exemplar, 0, len(t.Exemplars))
			for _, e := range t.Exemplars {
				v1Exemplar := prompb.Exemplar{
					Value:     e.Value,
					Timestamp: e.Timestamp,
					Labels:    make([]labelpb.ZLabel, 0, len(e.LabelsRefs)/2),
				}
				for i := 0; i+1 < len(e.LabelsRefs); i += 2 {
					v1Exemplar.Labels = append(v1Exemplar.Labels, labelpb.ZLabel{
						Name:  w.Symbols[e.LabelsRefs[i]],
						Value: w.Symbols[e.LabelsRefs[i+1]],
					})
				}
				v1Ts.Exemplars = append(v1Ts.Exemplars, v1Exemplar)
			}
		}

		if len(t.Histograms) > 0 {
			v1Ts.Histograms = make([]prompb.Histogram, 0, len(t.Histograms))
			for _, h := range t.Histograms {
				v1Histogram := prompb.Histogram{
					Sum:            h.Sum,
					Schema:         h.Schema,
					ZeroThreshold:  h.ZeroThreshold,
					NegativeSpans:  translateV2SpansToV1(h.NegativeSpans),
					NegativeDeltas: h.NegativeDeltas,
					NegativeCounts: h.NegativeCounts,
					PositiveSpans:  translateV2SpansToV1(h.PositiveSpans),
					PositiveDeltas: h.PositiveDeltas,
					PositiveCounts: h.PositiveCounts,
					ResetHint:      prompb.Histogram_ResetHint(h.ResetHint),
					Timestamp:      h.Timestamp,
					CustomValues:   h.CustomValues,
				}

				switch c := h.Count.(type) {
				case *writev2.Histogram_CountInt:
					v1Histogram.Count = &prompb.Histogram_CountInt{CountInt: c.CountInt}
				case *writev2.Histogram_CountFloat:
					v1Histogram.Count = &prompb.Histogram_CountFloat{CountFloat: c.CountFloat}
				}

				switch zc := h.ZeroCount.(type) {
				case *writev2.Histogram_ZeroCountInt:
					v1Histogram.ZeroCount = &prompb.Histogram_ZeroCountInt{ZeroCountInt: zc.ZeroCountInt}
				case *writev2.Histogram_ZeroCountFloat:
					v1Histogram.ZeroCount = &prompb.Histogram_ZeroCountFloat{ZeroCountFloat: zc.ZeroCountFloat}
				}

				v1Ts.Histograms = append(v1Ts.Histograms, v1Histogram)
			}
		}

		out.Timeseries = append(out.Timeseries, v1Ts)
	}
	return out
}

func translateV2SpansToV1(spans []writev2.BucketSpan) []prompb.BucketSpan {
	if len(spans) == 0 {
		return nil
	}
	out := make([]prompb.BucketSpan, len(spans))
	for i, s := range spans {
		out[i] = prompb.BucketSpan{Offset: s.Offset, Length: s.Length}
	}
	return out
}

func (h *Handler) handleV2HTTP(ctx context.Context, w http.ResponseWriter, r *http.Request, reqBuf []byte, tLogger log.Logger, tenantHTTP string, requestLimiter requestLimiter) {
	var wreq writev2.Request
	if err := proto.Unmarshal(reqBuf, &wreq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	translatedReq := translateV2ToV1(wreq)
	if err := h.handleV1HTTP(ctx, w, r, translatedReq, tLogger, tenantHTTP, requestLimiter); err != nil {
		return
	}

	// NOTE(GiedriusS): This part of the spec is still not 100% clear regarding async
	// writes so just tell Prometheus that we accepted all data.
	var ts, hs, es int

	for _, i := range wreq.Timeseries {
		hs += len(i.Histograms)
		ts += len(i.Samples)
		es += len(i.Exemplars)
	}
	w.Header().Set("X-Prometheus-Remote-Write-Samples-Written", fmt.Sprintf("%d", ts))
	w.Header().Set("X-Prometheus-Remote-Write-Histograms-Written", fmt.Sprintf("%d", hs))
	w.Header().Set("X-Prometheus-Remote-Write-Exemplars-Written", fmt.Sprintf("%d", es))
}

func (h *Handler) handleV1HTTP(ctx context.Context, w http.ResponseWriter, r *http.Request, wreq *prompb.WriteRequest, tLogger log.Logger, tenantHTTP string, requestLimiter requestLimiter) error {
	var err error

	rep := uint64(0)
	// If the header is empty, we assume the request is not yet replicated.
	if replicaRaw := r.Header.Get(h.options.ReplicaHeader); replicaRaw != "" {
		if rep, err = strconv.ParseUint(replicaRaw, 10, 64); err != nil {
			http.Error(w, "could not parse replica header", http.StatusBadRequest)
			return fmt.Errorf("parsing replica header: %w", err)
		}
	}

	// Exit early if the request contained no data. We don't support metadata yet. We also cannot fail here, because
	// this would mean lack of forward compatibility for remote write proto.
	if len(wreq.Timeseries) == 0 {
		// TODO(yeya24): Handle remote write metadata.
		if len(wreq.Metadata) > 0 {
			// TODO(bwplotka): Do we need this error message?
			level.Debug(tLogger).Log("msg", "only metadata from client; metadata ingestion not supported; skipping")
			return nil
		}
		level.Debug(tLogger).Log("msg", "empty remote write request; client bug or newer remote write protocol used?; skipping")
		return nil
	}

	if !requestLimiter.AllowSeries(tenantHTTP, int64(len(wreq.Timeseries))) {
		http.Error(w, "too many timeseries", http.StatusRequestEntityTooLarge)
		return fmt.Errorf("too many timeseries")
	}

	totalSamples := 0
	for _, timeseries := range wreq.Timeseries {
		totalSamples += len(timeseries.Samples)
	}
	if !requestLimiter.AllowSamples(tenantHTTP, int64(totalSamples)) {
		http.Error(w, "too many samples", http.StatusRequestEntityTooLarge)
		return fmt.Errorf("too many samples")
	}

	// Apply relabeling configs.
	h.relabel(wreq)
	if len(wreq.Timeseries) == 0 {
		level.Debug(tLogger).Log("msg", "remote write request dropped due to relabeling.")
		return nil
	}

	responseStatusCode := http.StatusOK
	tenantStats, err := h.handleRequest(ctx, rep, []wreqTenantTuple{
		{
			tenant: tenantHTTP,
			wreq:   wreq,
		},
	})
	if err != nil {
		level.Debug(tLogger).Log("msg", "failed to handle request", "err", err.Error())
		// TODO(GiedriusS): support retry-after.
		switch errors.Cause(err) {
		case errNotReady:
			responseStatusCode = http.StatusServiceUnavailable
		case errUnavailable:
			responseStatusCode = http.StatusServiceUnavailable
		case errConflict:
			responseStatusCode = http.StatusConflict
		case errBadReplica:
			responseStatusCode = http.StatusBadRequest
		case errValidation:
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

	return err
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

	rwVersion, err := determineRWVersion(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch rwVersion {
	case 1:
		// NOTE: Due to zero copy ZLabels, Labels used from WriteRequests keeps memory
		// from the whole request. Ensure that we always copy those when we want to
		// store them for longer time.
		var wreq prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &wreq); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = h.handleV1HTTP(ctx, w, r, &wreq, tLogger, tenantHTTP, requestLimiter)
	case 2:
		h.handleV2HTTP(ctx, w, r, reqBuf, tLogger, tenantHTTP, requestLimiter)
	default:
		panic("unsupported remote_write version")
	}

}

type requestStats struct {
	timeseries   int
	totalSamples int
}

type tenantRequestStats map[string]requestStats

func (h *Handler) handleRequest(ctx context.Context, rep uint64, data []wreqTenantTuple) (tenantRequestStats, error) {
	tLogger := h.logger

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
	return h.forward(ctx, r, data)
}

// forward accepts a write request, batches its time series by
// corresponding endpoint, and forwards them in parallel to the
// correct endpoint. Requests destined for the local node are written
// the local receiver. For a given write request, at most one outgoing
// write request will be made to every other node in the hashring,
// unless the request needs to be replicated.
// The function only returns when all requests have finished
// or the context is canceled.
func (h *Handler) forward(ctx context.Context, r replica, data []wreqTenantTuple) (tenantRequestStats, error) {
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
		data:              data,
		replicas:          replicas,
		alreadyReplicated: r.replicated,
	}

	return h.fanoutForward(ctx, params)
}

type remoteWriteParams struct {
	data              []wreqTenantTuple
	replicas          []uint64
	alreadyReplicated bool
}

func (p *remoteWriteParams) tenantLogTags() []any {
	if len(p.data) == 1 {
		return []any{"tenant", p.data[0].tenant}
	}

	var sb strings.Builder

	for i, d := range p.data {
		fmt.Fprintf(&sb, "%s", d.tenant)
		if i < len(p.data) {
			fmt.Fprintf(&sb, ",")
		}
	}

	return []any{"tenants", sb.String()}
}

func (h *Handler) gatherWriteStats(rf int, writes map[endpointReplica]map[string]trackedSeries) tenantRequestStats {
	var stats = make(tenantRequestStats)

	for er := range writes {
		for tenant, series := range writes[er] {
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

	logTags := params.tenantLogTags()
	if id, ok := middleware.RequestIDFromContext(ctx); ok {
		logTags = append(logTags, "request-id", id)
	}
	requestLogger := log.With(h.logger, logTags...)

	writes, err := h.distributeTimeseriesToReplicas(params.replicas, params.data)
	if err != nil {
		level.Error(requestLogger).Log("msg", "failed to distribute timeseries to replicas", "err", err)
		return stats, err
	}
	stats = h.gatherWriteStats(len(params.replicas), writes)

	// Prepare a buffered channel to receive the responses from the local and remote writes. Remote writes will all go
	// asynchronously and with this capacity we will never block on writing to the channel.
	var maxBufferedResponses int
	for er := range writes {
		maxBufferedResponses += len(writes[er])
	}

	responses := make(chan writeResponse, maxBufferedResponses)
	wg := sync.WaitGroup{}

	go func() {
		h.sendWrites(ctx, &wg, params, writes, responses)
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

			for _, er := range writes {
				for _, v := range er {
					h.seriesIDsPool.Put(v.seriesIDs[:0])
					h.timeSeriesPool.Put(v.timeSeries[:0])
				}

				clear(er)
				h.trackedSeries.Put(er)
			}

			clear(writes)
			h.distributeMapPool.Put(writes)
		}()
	}()

	successThreshold := h.writeQuorum()
	if params.alreadyReplicated {
		successThreshold = 1
	}
	// failureThreshold is the number of failures after which a series can no
	// longer reach the success threshold. For RF=3 and successThreshold=2 this is 2.
	failureThreshold := len(params.replicas) - successThreshold + 1
	var numSeries int
	for _, tup := range params.data {
		numSeries += len(tup.wreq.Timeseries)
	}
	successes := h.getIntScratch(numSeries)
	failures := h.getIntScratch(numSeries)
	// conflictFailures tracks how many replicas returned a permanent conflict for
	// each series. When conflictFailures[i] >= failureThreshold the series can
	// never reach quorum regardless of retries.
	conflictFailures := h.getIntScratch(numSeries)
	defer func() {
		h.intScratchPool.Put(successes[:0])
		h.intScratchPool.Put(failures[:0])
		h.intScratchPool.Put(conflictFailures[:0])
	}()
	seriesErrs := newReplicationErrors(successThreshold, numSeries)
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
				isConflictErr := isConflict(errors.Cause(resp.err))
				for _, seriesID := range resp.seriesIDs {
					seriesErrs[seriesID].Add(resp.err)
					failures[seriesID]++
					if isConflictErr {
						conflictFailures[seriesID]++
					}
				}
			} else {
				for _, seriesID := range resp.seriesIDs {
					successes[seriesID]++
				}
			}

			if canReturnEarly(successes, conflictFailures, successThreshold, failureThreshold) {
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

func (h *Handler) distributeTimeseriesToReplicas(
	replicas []uint64,
	data []wreqTenantTuple,
) (map[endpointReplica]map[string]trackedSeries, error) {
	h.mtx.RLock()
	defer h.mtx.RUnlock()
	writes := h.distributeMapPool.Get()
	if writes == nil {
		writes = make(map[endpointReplica]map[string]trackedSeries)
	}

	seriesID := -1
	for _, tup := range data {
		for _, ts := range tup.wreq.Timeseries {
			seriesID++
			var tenant = tup.tenant

			if h.splitTenantLabelName != "" {
				lbls := labelpb.ZLabelsToPromLabels(ts.Labels)

				tenantLabel := lbls.Get(h.splitTenantLabelName)
				if tenantLabel != "" {
					if err := tenancy.IsTenantValid(tenantLabel); err != nil {
						return nil, errors.Wrap(errValidation, err.Error())
					}
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
					return nil, err
				}
				endpointReplica := endpointReplica{endpoint: endpoint, replica: rn}

				writeableSeries, ok := writes[endpointReplica]
				if !ok {
					writeableSeries = h.trackedSeries.Get()
					if writeableSeries == nil {
						writeableSeries = make(map[string]trackedSeries)
					}

					writeableSeries[tenant] = trackedSeries{
						seriesIDs:  h.seriesIDsPool.Get(),
						timeSeries: h.timeSeriesPool.Get(),
					}
					writes[endpointReplica] = writeableSeries
				}
				tenantSeries := writeableSeries[tenant]

				tenantSeries.timeSeries = append(tenantSeries.timeSeries, ts)
				tenantSeries.seriesIDs = append(tenantSeries.seriesIDs, seriesID)

				writes[endpointReplica][tenant] = tenantSeries
			}
		}
	}

	return writes, nil
}

func isLocalEndpoint(e Endpoint, localEndpoint string) bool {
	return e.HasAddress(localEndpoint)
}

// sendWrites sends the local and remote writes to execute concurrently, controlling them through the provided sync.WaitGroup.
// The responses from the writes are sent to the responses channel.
func (h *Handler) sendWrites(
	ctx context.Context,
	wg *sync.WaitGroup,
	params remoteWriteParams,
	writes map[endpointReplica]map[string]trackedSeries,
	responses chan writeResponse,
) {
	var deferred []endpointReplica

	for writeDestination := range writes {
		wg.Add(1)
		if !h.tryWrite(ctx, writes[writeDestination], writeDestination, params.alreadyReplicated, responses, wg) {
			wg.Done()
			deferred = append(deferred, writeDestination)
		}
	}

	// Second pass: blocking submission for any peer whose pool was saturated during the first pass.
	for _, writeDestination := range deferred {
		wg.Add(1)
		h.sendWrite(ctx, writes[writeDestination], writeDestination, params.alreadyReplicated, responses, wg)
	}
}

// prepareRemoteWrite resolves the peer connection, builds the WriteRequest, and constructs the
// completion callback. Returns (nil, nil, nil) when a connection error has already been written to
// responses and wg.Done called — callers must check for nil before proceeding.
func (h *Handler) prepareRemoteWrite(
	ctx context.Context,
	writes map[string]trackedSeries,
	er endpointReplica,
	alreadyReplicated bool,
	responses chan writeResponse,
	wg *sync.WaitGroup,
	allIDs []int,
) (WriteableStoreAsyncClient, *storepb.WriteRequest, func(error)) {
	endpoint := er.endpoint
	cl, err := h.peers.getConnection(ctx, endpoint)
	if err != nil {
		if errors.Is(err, errUnavailable) {
			err = errors.Wrapf(errUnavailable, "backing off forward request for endpoint %v", er)
		}

		responses <- newWriteResponse(allIDs, err, er)
		wg.Done()
		return nil, nil, nil
	}

	dataTuples := make([]storepb.TimeSeriesTenantTuple, 0, len(writes))
	for wTenant, ts := range writes {
		dataTuples = append(dataTuples, storepb.TimeSeriesTenantTuple{
			Timeseries: ts.timeSeries,
			Tenant:     wTenant,
		})
	}

	// Replica is 1-indexed on the wire; 0 indicates un-replicated.
	req := &storepb.WriteRequest{
		TimeseriesTenantData: dataTuples,
		Replica:              int64(er.replica + 1),
	}
	cb := func(err error) {
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
					h.peers.markPeerUnavailable(er.endpoint)
				}
			}
		}
		wg.Done()
	}
	return cl, req, cb
}

// sendWrite sends a write request to the remote node. It blocks until the peer's worker
// pool accepts the work.
func (h *Handler) sendWrite(
	ctx context.Context,
	writes map[string]trackedSeries,
	er endpointReplica,
	alreadyReplicated bool,
	responses chan writeResponse,
	wg *sync.WaitGroup,
) {
	var totalIDs int
	for _, ts := range writes {
		totalIDs += len(ts.seriesIDs)
	}
	allIDs := make([]int, 0, totalIDs)
	for _, ts := range writes {
		allIDs = append(allIDs, ts.seriesIDs...)
	}

	cl, req, cb := h.prepareRemoteWrite(ctx, writes, er, alreadyReplicated, responses, wg, allIDs)
	if cl == nil {
		return
	}
	cl.RemoteWriteAsync(ctx, req, er, allIDs, responses, cb)
}

// tryWrite is the non-blocking counterpart of sendRemoteWrite. It returns false when the
// peer's worker pool is at capacity; the caller should then fall back to sendRemoteWrite.
// wg.Done is NOT called on a false return — the caller must not have called wg.Add before checking.
func (h *Handler) tryWrite(
	ctx context.Context,
	writes map[string]trackedSeries,
	er endpointReplica,
	alreadyReplicated bool,
	responses chan writeResponse,
	wg *sync.WaitGroup,
) bool {
	var totalIDs int
	for _, ts := range writes {
		totalIDs += len(ts.seriesIDs)
	}
	allIDs := make([]int, 0, totalIDs)
	for _, ts := range writes {
		allIDs = append(allIDs, ts.seriesIDs...)
	}

	cl, req, cb := h.prepareRemoteWrite(ctx, writes, er, alreadyReplicated, responses, wg, allIDs)
	if cl == nil {
		return true
	}
	return cl.TryRemoteWriteAsync(ctx, req, er, allIDs, responses, cb)
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

// canReturnEarly returns true when every series has a determined outcome.
// A series is determined when it either reached the success threshold, or its
// conflict failure count reached the failure threshold meaning it can never
// reach quorum even if every remaining non-conflict replica succeeds.
//
// Non-conflict failures do not trigger early return, we must wait
// for all replica responses so we can count total conflicts accurately and
// decide whether the request is permanently failed (409) or retryable (503).
func canReturnEarly(successes, conflictFailures []int, successThreshold, failureThreshold int) bool {
	for i := range successes {
		if successes[i] < successThreshold && conflictFailures[i] < failureThreshold {
			return false
		}
	}
	return true
}

type wreqTenantTuple struct {
	wreq   *prompb.WriteRequest
	tenant string
}

// RemoteWrite implements the gRPC remote write handler for storepb.WriteableStore.
func (h *Handler) RemoteWrite(ctx context.Context, r *storepb.WriteRequest) (*storepb.WriteResponse, error) {
	span, ctx := tracing.StartSpan(ctx, "receive_grpc")
	defer span.Finish()

	h.pendingWriteRequests.Set(float64(h.pendingWriteRequestsCounter.Inc()))
	defer h.pendingWriteRequestsCounter.Dec()

	data := make([]wreqTenantTuple, 0, len(r.TimeseriesTenantData))
	for _, ts := range r.TimeseriesTenantData {
		data = append(data, wreqTenantTuple{
			wreq: &prompb.WriteRequest{
				Timeseries: ts.Timeseries,
			},
			tenant: ts.Tenant,
		})
	}
	if len(data) == 0 {
		data = append(data, wreqTenantTuple{
			wreq: &prompb.WriteRequest{
				Timeseries: r.Timeseries,
			},
			tenant: r.Tenant,
		})
	}

	// Fast path for IngestorOnly mode: write directly to local TSDB.
	// This skips distributeTimeseriesToReplicas and sendLocalWrite since
	// the Router already determined this data belongs to this node.
	if h.receiverMode == IngestorOnly {
		var errs = make([]error, 0, len(data))
		for _, di := range data {
			err := h.writer.Write(ctx, di.tenant, di.wreq.Timeseries)
			if err != nil {
				level.Debug(h.logger).Log("msg", "failed to write to local TSDB", "err", err, "tenant", di.tenant)

				errs = append(errs, fmt.Errorf("writing %s data to local TSDB: %w", di.tenant, err))
			}
		}

		if len(errs) > 0 {
			returnErr := errs[0]
			err := errors.Unwrap(returnErr)

			if len(errs) > 1 {
				returnErr = fmt.Errorf("got %d errors while writing to multiple tenants, first one: %w", len(errs), returnErr)
			}

			switch cause := errors.Cause(err); cause {
			case nil:
				panic("BUG: errors.Cause returned nil on a non-nil error")
			default:
				if isNotReady(cause) {
					return nil, status.Error(codes.Unavailable, returnErr.Error())
				}
				if isConflict(cause) {
					return nil, status.Error(codes.AlreadyExists, returnErr.Error())
				}
				return nil, status.Error(codes.Internal, returnErr.Error())
			}
		}

		return &storepb.WriteResponse{}, nil

	}

	_, err := h.handleRequest(ctx, uint64(r.Replica), data)
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

// Cause extracts the sentinel error that best describes the replication outcome.
//
// If one error type appears at least threshold times it is returned directly
// (a conflict-dominated series can never succeed).
//
// Otherwise, if total errors meet the threshold but no single type dominates,
// the series failed quorum due to a mix of error types.
//
// Because canReturnEarly only fires early when conflict failures alone reach
// the threshold, reaching this fallback guarantees that conflict_count < failureThreshold,
// the request could succeed on retry once transient failures resolve.
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
		// conflict count is below the threshold so retry may
		// succeed once transient replicas recover.
		return errUnavailable
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

func newPeerWorker(client peerClient, forwardDelay prometheus.Observer, asyncWorkerCount uint, maxArtificialDelay time.Duration) *peerWorker {
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

	forwardDelay       prometheus.Observer
	maxArtificialDelay time.Duration
}

func newPeerGroup(
	logger log.Logger,
	backoff backoff.Backoff,
	forwardDelay *prometheus.HistogramVec,
	asyncForwardWorkersCount uint,
	localEndpoint string,
	writer *Writer,
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
		localEndpoint:            localEndpoint,
		writer:                   writer,
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

func (p *peerWorker) buildWork(ctx context.Context, req *storepb.WriteRequest, er endpointReplica, seriesIDs []int, responseWriter chan writeResponse, cb func(error)) pool.Work {
	now := time.Now()
	return func() {
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
	}
}

func (p *peerWorker) RemoteWriteAsync(ctx context.Context, req *storepb.WriteRequest, er endpointReplica, seriesIDs []int, responseWriter chan writeResponse, cb func(error)) {
	if err := p.wp.Go(ctx, p.buildWork(ctx, req, er, seriesIDs, responseWriter, cb)); err != nil {
		tracing.DoInSpan(ctx, "receive_forward", func(ctx context.Context) {
			sp := trace.SpanFromContext(ctx)
			sp.SetAttributes(attribute.Bool("error", true))
			sp.SetAttributes(attribute.String("error.msg", err.Error()))
			responseWriter <- newWriteResponse(
				seriesIDs,
				errors.Wrapf(err, "scheduling forward request for endpoint %v", er.endpoint),
				er,
			)
			cb(err)
		}, opentracing.Tags{
			"endpoint": er.endpoint,
			"replica":  er.replica,
		})
	}
}

func (p *peerWorker) TryRemoteWriteAsync(ctx context.Context, req *storepb.WriteRequest, er endpointReplica, seriesIDs []int, responseWriter chan writeResponse, cb func(error)) bool {
	return p.wp.TryGo(p.buildWork(ctx, req, er, seriesIDs, responseWriter, cb))
}

type peerGroup struct {
	logger                   log.Logger
	dialOpts                 []grpc.DialOption
	connections              map[Endpoint]*peerWorker
	peerStates               map[Endpoint]*retryState
	expBackoff               backoff.Backoff
	forwardDelay             *prometheus.HistogramVec
	asyncForwardWorkersCount uint
	replicationProtocol      ReplicationProtocol
	maxArtificialDelay       time.Duration
	localEndpoint            string
	writer                   *Writer

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

	p.forwardDelay.Delete(prometheus.Labels{"worker": endpoint.Address})
	p.connections[endpoint].wp.Close()
	delete(p.connections, endpoint)
	if err := c.client.Close(); err != nil {
		return fmt.Errorf("closing connection for %s", endpoint)
	}

	return nil
}

type localAsyncWriter struct {
	w *Writer
}

func (lw *localAsyncWriter) Close() error {
	return nil
}

func (lw *localAsyncWriter) RemoteWrite(ctx context.Context, in *storepb.WriteRequest, opts ...grpc.CallOption) (*storepb.WriteResponse, error) {
	if len(in.TimeseriesTenantData) == 0 {
		panic("BUG: localAsyncWriter.RemoteWrite called without TimeseriesTenantData")
	}

	for _, ts := range in.TimeseriesTenantData {
		if err := lw.w.Write(ctx, ts.Tenant, ts.Timeseries); err != nil {
			return nil, errors.Wrap(err, "writing locally")
		}
	}

	return &storepb.WriteResponse{}, nil
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
	if isLocalEndpoint(endpoint, p.localEndpoint) {
		client = &localAsyncWriter{
			w: p.writer,
		}
	} else {
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
	}

	var delay time.Duration
	if p.conns.Load() == 2 {
		delay = p.maxArtificialDelay
	}

	p.connections[endpoint] = newPeerWorker(client, p.forwardDelay.WithLabelValues(endpoint.Address), p.asyncForwardWorkersCount, delay)
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
	p.m.Lock()
	defer p.m.Unlock()

	p.expBackoff.Reset()
	p.peerStates = make(map[Endpoint]*retryState)
}
