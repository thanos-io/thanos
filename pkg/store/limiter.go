// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"sync"

	"github.com/alecthomas/units"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type ChunksLimiter interface {
	// Reserve num chunks out of the total number of chunks enforced by the limiter.
	// Returns an error if the limit has been exceeded. This function must be
	// goroutine safe.
	Reserve(num uint64) error
}

type SeriesLimiter interface {
	// Reserve num series out of the total number of series enforced by the limiter.
	// Returns an error if the limit has been exceeded. This function must be
	// goroutine safe.
	Reserve(num uint64) error
}

type BytesLimiter interface {
	// Reserve bytes out of the total amount of bytes enforced by the limiter.
	// Returns an error if the limit has been exceeded. This function must be
	// goroutine safe.
	Reserve(num uint64) error
}

// ChunksLimiterFactory is used to create a new ChunksLimiter. The factory is useful for
// projects depending on Thanos (eg. Cortex) which have dynamic limits.
type ChunksLimiterFactory func(failedCounter prometheus.Counter) ChunksLimiter

// SeriesLimiterFactory is used to create a new SeriesLimiter.
type SeriesLimiterFactory func(failedCounter prometheus.Counter) SeriesLimiter

// BytesLimiterFactory is used to create a new BytesLimiter.
type BytesLimiterFactory func(failedCounter prometheus.Counter) BytesLimiter

// Limiter is a simple mechanism for checking if something has passed a certain threshold.
type Limiter struct {
	limit    uint64
	reserved atomic.Uint64

	// Counter metric which we will increase if limit is exceeded.
	failedCounter prometheus.Counter
	failedOnce    sync.Once
}

// NewLimiter returns a new limiter with a specified limit. 0 disables the limit.
func NewLimiter(limit uint64, ctr prometheus.Counter) *Limiter {
	return &Limiter{limit: limit, failedCounter: ctr}
}

// Reserve implements ChunksLimiter.
func (l *Limiter) Reserve(num uint64) error {
	if l == nil {
		return nil
	}
	if l.limit == 0 {
		return nil
	}
	if reserved := l.reserved.Add(num); reserved > l.limit {
		// We need to protect from the counter being incremented twice due to concurrency
		// while calling Reserve().
		l.failedOnce.Do(l.failedCounter.Inc)
		return errors.Errorf("limit %v violated (got %v)", l.limit, reserved)
	}
	return nil
}

// NewChunksLimiterFactory makes a new ChunksLimiterFactory with a static limit.
func NewChunksLimiterFactory(limit uint64) ChunksLimiterFactory {
	return func(failedCounter prometheus.Counter) ChunksLimiter {
		return NewLimiter(limit, failedCounter)
	}
}

// NewSeriesLimiterFactory makes a new NewSeriesLimiterFactory with a static limit.
func NewSeriesLimiterFactory(limit uint64) SeriesLimiterFactory {
	return func(failedCounter prometheus.Counter) SeriesLimiter {
		return NewLimiter(limit, failedCounter)
	}
}

// NewBytesLimiterFactory makes a new NewSeriesLimiterFactory with a static limit.
func NewBytesLimiterFactory(limit units.Base2Bytes) BytesLimiterFactory {
	return func(failedCounter prometheus.Counter) BytesLimiter {
		return NewLimiter(uint64(limit), failedCounter)
	}
}

type RateLimits struct {
	SeriesPerRequest uint64
	ChunksPerRequest uint64
}

func (l *RateLimits) RegisterFlags(cmd extkingpin.FlagClause) {
	cmd.Flag("store.grpc.series-limit", "The maximum series allowed for a single Series request. The Series call fails if this limit is exceeded. 0 means no limit.").Default("0").Uint64Var(&l.SeriesPerRequest)
	cmd.Flag("store.grpc.chunks-limit", "The maximum chunks allowed for a single Series request, The Series call fails if this limit is exceeded. 0 means no limit.").Default("0").Uint64Var(&l.ChunksPerRequest)
}

// rateLimitedStoreServer is a storepb.StoreServer that can apply rate limits against Series requests.
type rateLimitedStoreServer struct {
	storepb.StoreServer
	newSeriesLimiter      SeriesLimiterFactory
	newChunksLimiter      ChunksLimiterFactory
	failedRequestsCounter *prometheus.CounterVec
}

// NewRateLimitedStoreServer creates a new rateLimitedStoreServer.
func NewRateLimitedStoreServer(store storepb.StoreServer, reg prometheus.Registerer, rateLimits RateLimits) storepb.StoreServer {
	return &rateLimitedStoreServer{
		StoreServer:      store,
		newSeriesLimiter: NewSeriesLimiterFactory(rateLimits.SeriesPerRequest),
		newChunksLimiter: NewChunksLimiterFactory(rateLimits.ChunksPerRequest),
		failedRequestsCounter: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_selects_dropped_total",
			Help: "Number of select queries that were dropped due to configured limits.",
		}, []string{"reason"}),
	}
}

func (s *rateLimitedStoreServer) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	seriesLimiter := s.newSeriesLimiter(s.failedRequestsCounter.WithLabelValues("series"))
	chunksLimiter := s.newChunksLimiter(s.failedRequestsCounter.WithLabelValues("chunks"))
	rateLimitedSrv := newRateLimitedServer(srv, seriesLimiter, chunksLimiter)
	if err := s.StoreServer.Series(req, rateLimitedSrv); err != nil {
		return err
	}

	return nil
}

// rateLimitedServer is a storepb.Store_SeriesServer that tracks statistics about sent series.
type rateLimitedServer struct {
	storepb.Store_SeriesServer
	seriesLimiter SeriesLimiter
	chunksLimiter ChunksLimiter
}

func newRateLimitedServer(upstream storepb.Store_SeriesServer, seriesLimiter SeriesLimiter, chunksLimiter ChunksLimiter) *rateLimitedServer {
	return &rateLimitedServer{
		Store_SeriesServer: upstream,
		seriesLimiter:      seriesLimiter,
		chunksLimiter:      chunksLimiter,
	}
}

func (i *rateLimitedServer) Send(response *storepb.SeriesResponse) error {
	series := response.GetSeries()
	if series == nil {
		return i.Store_SeriesServer.Send(response)
	}

	if err := i.seriesLimiter.Reserve(1); err != nil {
		return errors.Wrapf(err, "failed to send series")
	}
	if err := i.chunksLimiter.Reserve(uint64(len(series.Chunks))); err != nil {
		return errors.Wrapf(err, "failed to send chunks")
	}

	return i.Store_SeriesServer.Send(response)
}
