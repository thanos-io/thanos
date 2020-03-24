// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

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

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	conntrack "github.com/mwitkow/go-conntrack"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/storage"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

const (
	// DefaultTenantHeader is the default header used to designate the tenant making a write request.
	DefaultTenantHeader = "THANOS-TENANT"
	// DefaultReplicaHeader is the default header used to designate the replica count of a write request.
	DefaultReplicaHeader = "THANOS-REPLICA"
)

// conflictErr is returned whenever an operation fails due to any conflict-type error.
var conflictErr = errors.New("conflict")

var errBadReplica = errors.New("replica count exceeds replication factor")

// Options for the web Handler.
type Options struct {
	Writer            *Writer
	ListenAddress     string
	Registry          prometheus.Registerer
	TenantHeader      string
	ReplicaHeader     string
	Endpoint          string
	ReplicationFactor uint64
	Tracer            opentracing.Tracer
	TLSConfig         *tls.Config
	DialOpts          []grpc.DialOption
}

// Handler serves a Prometheus remote write receiving HTTP endpoint.
type Handler struct {
	logger   log.Logger
	writer   *Writer
	router   *route.Router
	options  *Options
	listener net.Listener

	mtx      sync.RWMutex
	hashring Hashring
	peers    *peerGroup

	// Metrics.
	forwardRequestsTotal *prometheus.CounterVec
}

func NewHandler(logger log.Logger, o *Options) *Handler {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	h := &Handler{
		logger:  logger,
		writer:  o.Writer,
		router:  route.New(),
		options: o,
		peers:   newPeerGroup(o.DialOpts...),
		forwardRequestsTotal: promauto.With(o.Registry).NewCounterVec(
			prometheus.CounterOpts{
				Name: "thanos_receive_forward_requests_total",
				Help: "The number of forward requests.",
			}, []string{"result"},
		),
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

	h.router.Post("/api/v1/receive", instrf("receive", readyf(h.receiveHTTP)))

	return h
}

// SetWriter sets the writer.
// The writer must be set to a non-nil value in order for the
// handler to be ready and usable.
// If the writer is nil, then the handler is marked as not ready.
func (h *Handler) SetWriter(w *Writer) {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	h.writer = w
}

// Hashring sets the hashring for the handler and marks the hashring as ready.
// The hashring must be set to a non-nil value in order for the
// handler to be ready and usable.
// If the hashring is nil, then the handler is marked as not ready.
func (h *Handler) Hashring(hashring Hashring) {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	h.hashring = hashring
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

// replica encapsulates the replica number of a request and if the request is
// already replicated.
type replica struct {
	n          uint64
	replicated bool
}

func (h *Handler) handleRequest(ctx context.Context, rep uint64, tenant string, wreq *prompb.WriteRequest) error {
	// The replica value in the header is one-indexed, thus we need >.
	if rep > h.options.ReplicationFactor {
		return errBadReplica
	}

	r := replica{
		n:          rep,
		replicated: rep != 0,
	}

	// on-the-wire format is 1-indexed and in-code is 0-indexed so we decrement the value if it was already replicated.
	if r.replicated {
		r.n--
	}

	// Forward any time series as necessary. All time series
	// destined for the local node will be written to the receiver.
	// Time series will be replicated as necessary.
	if err := h.forward(ctx, tenant, r, wreq); err != nil {
		if countCause(err, isConflict) > 0 {
			return conflictErr
		}
		return err
	}
	return nil
}

func (h *Handler) receiveHTTP(w http.ResponseWriter, r *http.Request) {
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

	rep := uint64(0)
	// If the header is empty, we assume the request is not yet replicated.
	if replicaRaw := r.Header.Get(h.options.ReplicaHeader); replicaRaw != "" {
		if rep, err = strconv.ParseUint(replicaRaw, 10, 64); err != nil {
			http.Error(w, "could not parse replica header", http.StatusBadRequest)
			return
		}
	}

	tenant := r.Header.Get(h.options.TenantHeader)

	err = h.handleRequest(r.Context(), rep, tenant, &wreq)
	switch err {
	case nil:
		return
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
func (h *Handler) forward(ctx context.Context, tenant string, r replica, wreq *prompb.WriteRequest) error {
	wreqs := make(map[string]*prompb.WriteRequest)
	replicas := make(map[string]replica)

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
		endpoint, err := h.hashring.GetN(tenant, &wreq.Timeseries[i], r.n)
		if err != nil {
			h.mtx.RUnlock()
			return err
		}
		if _, ok := wreqs[endpoint]; !ok {
			wreqs[endpoint] = &prompb.WriteRequest{}
			replicas[endpoint] = r
		}
		wr := wreqs[endpoint]
		wr.Timeseries = append(wr.Timeseries, wreq.Timeseries[i])
	}
	h.mtx.RUnlock()

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
				var err error
				h.mtx.RLock()
				if h.writer == nil {
					err = errors.New("storage is not ready")
				} else {
					err = h.writer.Write(wreqs[endpoint])
					// When a MultiError is added to another MultiError, the error slices are concatenated, not nested.
					// To avoid breaking the counting logic, we need to flatten the error.
					if errs, ok := err.(terrors.MultiError); ok {
						if countCause(errs, isConflict) > 0 {
							err = errors.Wrap(conflictErr, errs.Error())
						} else {
							err = errors.New(errs.Error())
						}
					}
				}
				h.mtx.RUnlock()
				if err != nil {
					level.Error(h.logger).Log("msg", "storing locally", "err", err, "endpoint", endpoint)
				}
				ec <- err
			}(endpoint)
			continue
		}
		// Make a request to the specified endpoint.
		go func(endpoint string) {
			var err error

			// Increment the counters as necessary now that
			// the requests will go out.
			defer func() {
				if err != nil {
					h.forwardRequestsTotal.WithLabelValues("error").Inc()
					return
				}
				h.forwardRequestsTotal.WithLabelValues("success").Inc()
			}()

			cl, err := h.peers.get(ctx, endpoint)
			if err != nil {
				level.Error(h.logger).Log("msg", "failed to get peer connection to forward request", "err", err, "endpoint", endpoint)
				ec <- err
				return
			}
			// Create a span to track the request made to another receive node.
			tracing.DoInSpan(ctx, "thanos_receive_forward", func(ctx context.Context) {
				// Actually make the request against the endpoint
				// we determined should handle these time series.
				_, err = cl.RemoteWrite(ctx, &storepb.WriteRequest{
					Timeseries: wreqs[endpoint].Timeseries,
					Tenant:     tenant,
					Replica:    int64(replicas[endpoint].n + 1), // increment replica since on-the-wire format is 1-indexed and 0 indicates unreplicated.
				})
				if err != nil {
					level.Error(h.logger).Log("msg", "forwarding request", "err", err, "endpoint", endpoint)
					ec <- err
					return
				}
				ec <- nil
			})
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

	err := h.parallelizeRequests(ctx, tenant, replicas, wreqs)
	if errs, ok := err.(terrors.MultiError); ok {
		if uint64(countCause(errs, isConflict)) >= (h.options.ReplicationFactor+1)/2 {
			return errors.Wrap(conflictErr, "did not meet replication threshold")
		}
		if uint64(len(errs)) >= (h.options.ReplicationFactor+1)/2 {
			return errors.Wrap(err, "did not meet replication threshold")
		}
		return nil
	}
	return errors.Wrap(err, "could not replicate write request")
}

// RemoteWrite implements the gRPC remote write handler for storepb.WriteableStore.
func (h *Handler) RemoteWrite(ctx context.Context, r *storepb.WriteRequest) (*storepb.WriteResponse, error) {
	err := h.handleRequest(ctx, uint64(r.Replica), r.Tenant, &prompb.WriteRequest{Timeseries: r.Timeseries})
	switch err {
	case nil:
		return &storepb.WriteResponse{}, nil
	case conflictErr:
		return nil, status.Error(codes.AlreadyExists, err.Error())
	case errBadReplica:
		return nil, status.Error(codes.InvalidArgument, err.Error())
	default:
		return nil, status.Error(codes.Internal, err.Error())
	}
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
