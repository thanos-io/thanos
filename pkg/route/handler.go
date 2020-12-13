// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package route

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	stdlog "log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/jpillora/backoff"
	"github.com/mwitkow/go-conntrack"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/server/http/middleware"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

const (
	// DefaultTenantHeader is the default header used to designate the tenant making a write request.
	DefaultTenantHeader = "THANOS-TENANT"
	// DefaultTenant is the default value used for when no tenant is passed via the tenant header.
	DefaultTenant = "default-tenant"
	// DefaultTenantLabel is the default label-name used for when no tenant is passed via the tenant header.
	DefaultTenantLabel = "tenant_id"
	// Labels for metrics.
	labelSuccess = "success"
	labelError   = "error"
)

var (
	// conflictErr is returned whenever an operation fails due to any conflict-type error.
	conflictErr = errors.New("conflict")

	errBadReplica  = errors.New("replica count exceeds replication factor")
	errNotReady    = errors.New("target not ready")
	errUnavailable = errors.New("target not available")
)


// replica encapsulates the replica number of a request and if the request is
// already replicated.
type replica struct {
	n          uint64
	replicated bool
}

// Options for the web Handler.
type Options struct {
	ListenAddress     string
	Registry          prometheus.Registerer
	TenantHeader      string
	DefaultTenantID   string
	ReplicationFactor uint64
	Tracer            opentracing.Tracer
	TLSConfig         *tls.Config
	DialOpts          []grpc.DialOption
	ForwardTimeout    time.Duration
}

// Handler serves a Prometheus remote write receiving HTTP endpoint.
type Handler struct {
	logger   log.Logger
	router   *route.Router
	options  *Options
	listener net.Listener
	mtx        sync.RWMutex
	hashring   Hashring
	peers      *peerGroup
	expBackoff backoff.Backoff
	peerStates map[string]*retryState
	forwardRequests   *prometheus.CounterVec
	replications      *prometheus.CounterVec
	replicationFactor prometheus.Gauge
}

func NewHandler(logger log.Logger, o *Options) *Handler {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	h := &Handler{
		logger:  logger,
		router:  route.New(),
		options: o,
		peers:   newPeerGroup(o.DialOpts...),
		expBackoff: backoff.Backoff{
			Factor: 2,
			Min:    100 * time.Millisecond,
			Max:    30 * time.Second,
			Jitter: true,
		},
		forwardRequests: promauto.With(o.Registry).NewCounterVec(
			prometheus.CounterOpts{
				Name: "thanos_receive_forward_requests_total",
				Help: "The number of forward requests.",
			}, []string{"result"},
		),
		replications: promauto.With(o.Registry).NewCounterVec(
			prometheus.CounterOpts{
				Name: "thanos_receive_replications_total",
				Help: "The number of replication operations done by the receiver. The success of replication is fulfilled when a quorum is met.",
			}, []string{"result"},
		),
		replicationFactor: promauto.With(o.Registry).NewGauge(
			prometheus.GaugeOpts{
				Name: "thanos_receive_replication_factor",
				Help: "The number of times to replicate incoming write requests.",
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
		ins = extpromhttp.NewInstrumentationMiddleware(o.Registry)
	}

	readyf := h.testReady
	instrf := func(name string, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
		if o.Tracer != nil {
			next = tracing.HTTPMiddleware(o.Tracer, name, logger, http.HandlerFunc(next))
		}
		return ins.NewHandler(name, http.HandlerFunc(next))
	}

	h.router.Post("/api/v1/receive", instrf("receive", readyf(middleware.RequestID(http.HandlerFunc(h.receiveHTTP)))))

	return h
}

// Hashring sets the hashring for the handler and marks the hashring as ready.
// The hashring must be set to a non-nil value in order for the
// handler to be ready and usable.
// If the hashring is nil, then the handler is marked as not ready.
func (h *Handler) Hashring(hashring Hashring) {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	h.hashring = hashring
	h.expBackoff.Reset()
	h.peerStates = make(map[string]*retryState)
}

// Verifies whether the server is ready or not.
func (h *Handler) isReady() bool {
	h.mtx.RLock()
	hr := h.hashring != nil
	h.mtx.RUnlock()
	return hr
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

	errlog := stdlog.New(log.NewStdlibAdapter(level.Error(h.logger)), "", 0)

	httpSrv := &http.Server{
		Handler:   h.router,
		ErrorLog:  errlog,
		TLSConfig: h.options.TLSConfig,
	}

	if h.options.TLSConfig != nil {
		level.Info(h.logger).Log("msg", "Serving HTTPS", "address", h.options.ListenAddress)
		// Cert & Key are already being passed in via TLSConfig.
		return httpSrv.ServeTLS(h.listener, "", "")
	}

	level.Info(h.logger).Log("msg", "Serving plain HTTP", "address", h.options.ListenAddress)
	return httpSrv.Serve(h.listener)
}


func (h *Handler) handleRequest(ctx context.Context, tenant string, wreq *prompb.WriteRequest) error {

	// Forward any time series as necessary.
	// Time series will be replicated as necessary.
	if err := h.forward(ctx, tenant, wreq); err != nil {
		if countCause(err, isConflict) > 0 {
			return conflictErr
		}
		return err
	}


	return nil
}

func (h *Handler) receiveHTTP(w http.ResponseWriter, r *http.Request) {
	span, ctx := tracing.StartSpan(r.Context(), "receive_http")
	defer span.Finish()

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
	if len(tenant) == 0 {
		tenant = h.options.DefaultTenantID
	}

	err = h.handleRequest(ctx, tenant, &wreq)
	switch err {
	case nil:
		return
	case errNotReady:
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case errUnavailable:
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case conflictErr:
		http.Error(w, err.Error(), http.StatusConflict)
	case errBadReplica:
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		level.Error(h.logger).Log("err", err, "msg", "internal server error")
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
func (h *Handler) forward(ctx context.Context, tenant string, wreq *prompb.WriteRequest) error {
	span, ctx := tracing.StartSpan(ctx, "receive_fanout_forward")
	defer span.Finish()
	replicas := make(map[string]replica)
	wreqs := make(map[string]*prompb.WriteRequest)

	// It is possible that hashring is ready in testReady() but unready now,
	// so need to lock here.
	h.mtx.RLock()
	if h.hashring == nil {
		h.mtx.RUnlock()
		return errors.New("hashring is not ready")
	}

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
			h.mtx.RUnlock()
			return err
		}
		if _, ok := wreqs[endpoint]; !ok {
			wreqs[endpoint] = &prompb.WriteRequest{}
			replicas[endpoint] = replica{
				replicated: false,
				n: 0,
			}
		}
		wr := wreqs[endpoint]
		wr.Timeseries = append(wr.Timeseries, wreq.Timeseries[i])
	}
	h.mtx.RUnlock()

	return h.fanoutForward(ctx, tenant, replicas, wreqs, len(wreqs))
}

// writeQuorum returns minimum number of replicas that has to confirm write success before claiming replication success.
func (h *Handler) writeQuorum() int {
	return int((h.options.ReplicationFactor / 2) + 1)
}

// fanoutForward fans out concurrently given set of write requests. It returns status immediately when quorum of
// requests succeeds or fails or if context is canceled.
func (h *Handler) fanoutForward(pctx context.Context, tenant string, replicas map[string]replica, wreqs map[string]*prompb.WriteRequest, successThreshold int) error {
	var errs terrors.MultiError

	fctx, cancel := context.WithTimeout(tracing.CopyTraceContext(context.Background(), pctx), h.options.ForwardTimeout)
	defer func() {
		if errs.Err() != nil {
			// NOTICE: The cancel function is not used on all paths intentionally,
			// if there is no error when quorum successThreshold is reached,
			// let forward requests to optimistically run until timeout.
			cancel()
		}
	}()

	logger := log.With(h.logger, "tenant", tenant)
	if id, ok := middleware.RequestIDFromContext(pctx); ok {
		logger = log.With(logger, "request-id", id)
	}

	ec := make(chan error)

	var wg sync.WaitGroup
	for endpoint := range wreqs {
		wg.Add(1)

		// If the request is not yet replicated, let's replicate it.
		// If the replication factor isn't greater than 1, let's
		// just forward the requests.
		if !replicas[endpoint].replicated && h.options.ReplicationFactor > 1 {
			go func(endpoint string) {
				defer wg.Done()

				var err error
				tracing.DoInSpan(fctx, "receive_replicate", func(ctx context.Context) {
					err = h.replicate(ctx, tenant, wreqs[endpoint])
				})
				if err != nil {
					h.replications.WithLabelValues(labelError).Inc()
					ec <- errors.Wrapf(err, "replicate write request, endpoint %v", endpoint)
					return
				}

				h.replications.WithLabelValues(labelSuccess).Inc()
				ec <- nil
			}(endpoint)

			continue
		}



		// Make a request to the specified endpoint.
		go func(endpoint string) {
			defer wg.Done()

			var (
				err error
				cl  storepb.WriteableStoreClient
			)
			defer func() {
				// This is an actual remote forward request so report metric here.
				if err != nil {
					h.forwardRequests.WithLabelValues(labelError).Inc()
					return
				}
				h.forwardRequests.WithLabelValues(labelSuccess).Inc()
			}()

			cl, err = h.peers.get(fctx, endpoint)
			if err != nil {
				ec <- errors.Wrapf(err, "get peer connection for endpoint %v", endpoint)
				return
			}

			h.mtx.RLock()
			b, ok := h.peerStates[endpoint]
			if ok {
				if time.Now().Before(b.nextAllowed) {
					h.mtx.RUnlock()
					ec <- errors.Wrapf(errUnavailable, "backing off forward request for endpoint %v", endpoint)
					return
				}
			}
			h.mtx.RUnlock()

			// Create a span to track the request made to another receive node.
			tracing.DoInSpan(fctx, "receive_forward", func(ctx context.Context) {
				// Actually make the request against the endpoint we determined should handle these time series.
				_, err = cl.RemoteWrite(ctx, &storepb.WriteRequest{
					Timeseries: wreqs[endpoint].Timeseries,
					Tenant:     tenant,
					// Increment replica since on-the-wire format is 1-indexed and 0 indicates un-replicated.
					Replica: int64(replicas[endpoint].n + 1),
				})
			})
			if err != nil {
				// Check if peer connection is unavailable, don't attempt to send requests constantly.
				if st, ok := status.FromError(err); ok {
					if st.Code() == codes.Unavailable {
						h.mtx.Lock()
						if b, ok := h.peerStates[endpoint]; ok {
							b.attempt++
							dur := h.expBackoff.ForAttempt(b.attempt)
							b.nextAllowed = time.Now().Add(dur)
							level.Debug(h.logger).Log("msg", "target unavailable backing off", "for", dur)
						} else {
							h.peerStates[endpoint] = &retryState{nextAllowed: time.Now().Add(h.expBackoff.ForAttempt(0))}
						}
						h.mtx.Unlock()
					}
				}
				ec <- errors.Wrapf(err, "forwarding request to endpoint %v", endpoint)
				return
			}
			h.mtx.Lock()
			delete(h.peerStates, endpoint)
			h.mtx.Unlock()

			ec <- nil
		}(endpoint)
	}

	go func() {
		wg.Wait()
		close(ec)
	}()

	// At the end, make sure to exhaust the channel, letting remaining unnecessary requests finish asynchronously.
	// This is needed if context is canceled or if we reached success of fail quorum faster.
	defer func() {
		go func() {
			for err := range ec {
				if err != nil {
					level.Debug(logger).Log("msg", "request failed, but not needed to achieve quorum", "err", err)
				}
			}
		}()
	}()

	var success int
	for {
		select {
		case <-fctx.Done():
			return fctx.Err()
		case err, more := <-ec:
			if !more {
				return errs
			}
			if err == nil {
				success++
				if success >= successThreshold {
					// In case the success threshold is lower than the total
					// number of requests, then we can finish early here. This
					// is the case for quorum writes for example.
					return nil
				}
				continue
			}
			errs.Add(err)
		}
	}
}

// replicate replicates a write request to (replication-factor) nodes
// selected by the tenant and time series.
// The function only returns when all replication requests have finished
// or the context is canceled.
func (h *Handler) replicate(ctx context.Context, tenant string, wreq *prompb.WriteRequest) error {
	wreqs := make(map[string]*prompb.WriteRequest)
	replicas := make(map[string]replica)
	var i uint64

	// It is possible that hashring is ready in testReady() but unready now,
	// so need to lock here.
	h.mtx.RLock()
	if h.hashring == nil {
		h.mtx.RUnlock()
		return errors.New("hashring is not ready")
	}

	for i = 0; i < h.options.ReplicationFactor; i++ {
		endpoint, err := h.hashring.GetN(tenant, &wreq.Timeseries[0], i)
		if err != nil {
			h.mtx.RUnlock()
			return err
		}
		wreqs[endpoint] = wreq
		replicas[endpoint] = replica{i, true}
	}
	h.mtx.RUnlock()

	quorum := h.writeQuorum()
	// fanoutForward only returns an error if successThreshold (quorum) is not reached.
	if err := h.fanoutForward(ctx, tenant, replicas, wreqs, quorum); err != nil {
		if countCause(err, isNotReady) >= quorum {
			return errors.Wrap(errNotReady, "replicate: quorum not reached")
		}
		if countCause(err, isConflict) >= quorum {
			return errors.Wrap(conflictErr, "replicate: quorum not reached")
		}
		if countCause(err, isUnavailable) >= quorum {
			return errors.Wrap(errUnavailable, "replicate: quorum not reached")
		}
		return errors.Wrap(err, "unexpected error, before quorum is reached")
	}

	return nil
}


// countCause counts the number of errors within the given error
// whose causes satisfy the given function.
// countCause will inspect the error's cause or, if the error is a MultiError,
// the cause of each contained error but will not traverse any deeper.
func countCause(err error, f func(error) bool) int {
	errs, ok := err.(terrors.MultiError)
	if !ok {
		errs = []error{err}
	}
	var n int
	for i := range errs {
		if f(errors.Cause(errs[i])) {
			n++
		}
	}
	return n
}

// isConflict returns whether or not the given error represents a conflict.
func isConflict(err error) bool {
	if err == nil {
		return false
	}
	return err == conflictErr ||
		err == storage.ErrDuplicateSampleForTimestamp ||
		err == storage.ErrOutOfOrderSample ||
		err == storage.ErrOutOfBounds ||
		err.Error() == strconv.Itoa(http.StatusConflict) ||
		status.Code(err) == codes.AlreadyExists
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

func newPeerGroup(dialOpts ...grpc.DialOption) *peerGroup {
	return &peerGroup{
		dialOpts: dialOpts,
		cache:    map[string]storepb.WriteableStoreClient{},
		m:        sync.RWMutex{},
		dialer:   grpc.DialContext,
	}
}

type peerGroup struct {
	dialOpts []grpc.DialOption
	cache    map[string]storepb.WriteableStoreClient
	m        sync.RWMutex

	// dialer is used for testing.
	dialer func(ctx context.Context, target string, opts ...grpc.DialOption) (conn *grpc.ClientConn, err error)
}

func (p *peerGroup) get(ctx context.Context, addr string) (storepb.WriteableStoreClient, error) {
	// use a RLock first to prevent blocking if we don't need to.
	p.m.RLock()
	c, ok := p.cache[addr]
	p.m.RUnlock()
	if ok {
		return c, nil
	}

	p.m.Lock()
	defer p.m.Unlock()
	// Make sure that another caller hasn't created the connection since obtaining the write lock.
	c, ok = p.cache[addr]
	if ok {
		return c, nil
	}
	conn, err := p.dialer(ctx, addr, p.dialOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to dial peer")
	}

	client := storepb.NewWriteableStoreClient(conn)
	p.cache[addr] = client
	return client, nil
}
