// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type RateLimits struct {
	SeriesPerRequest uint64
	ChunksPerRequest uint64
}

func (l *RateLimits) RegisterFlags(cmd extkingpin.FlagClause) {
	cmd.Flag("store.grpc.series-limit", "The maximum series allowed for a single Series request. The Series call fails if this limit is exceeded. 0 means no limit.").Default("0").Uint64Var(&l.SeriesPerRequest)
	cmd.Flag("store.grpc.chunks-limit", "The maximum chunks allowed for a single Series request, The Series call fails if this limit is exceeded. 0 means no limit.").Default("0").Uint64Var(&l.SeriesPerRequest)
}

// rateLimitedStoreServer is a storepb.StoreServer that can apply rate limits against Series requests.
type rateLimitedStoreServer struct {
	storepb.StoreServer
	rateLimits RateLimits
}

// NewRateLimitedStoreServer creates a new rateLimitedStoreServer.
func NewRateLimitedStoreServer(store storepb.StoreServer, rateLimits RateLimits) storepb.StoreServer {
	return &rateLimitedStoreServer{
		StoreServer: store,
		rateLimits:  rateLimits,
	}
}

func (s *rateLimitedStoreServer) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	rateLimitedSrv := newRateLimitedServer(newInstrumentedServer(srv), s.rateLimits)
	if err := s.StoreServer.Series(req, rateLimitedSrv); err != nil {
		return err
	}

	return nil
}

// rateLimitedServer is a storepb.Store_SeriesServer that tracks statistics about sent series.
type rateLimitedServer struct {
	*instrumentedServer
	limiters []limiterFunc
}

func newRateLimitedServer(upstream *instrumentedServer, rateLimits RateLimits) *rateLimitedServer {
	limiters := []limiterFunc{
		seriesLimiter(upstream, rateLimits.SeriesPerRequest),
		chunksLimiter(upstream, rateLimits.ChunksPerRequest),
	}

	return &rateLimitedServer{
		instrumentedServer: upstream,
		limiters:           limiters,
	}
}

func (i *rateLimitedServer) Send(response *storepb.SeriesResponse) error {
	for _, limiter := range i.limiters {
		if err := limiter(response); err != nil {
			return err
		}
	}
	return i.instrumentedServer.Send(response)
}

type limiterFunc func(response *storepb.SeriesResponse) error

func nopLimiter(*storepb.SeriesResponse) error { return nil }

func seriesLimiter(server *instrumentedServer, limit uint64) limiterFunc {
	if limit == 0 {
		return nopLimiter
	}
	return func(response *storepb.SeriesResponse) error {
		if server.seriesSent >= float64(limit) {
			return errors.Errorf("store series limit of %d exceeded", limit)
		}
		return nil
	}
}

func chunksLimiter(server *instrumentedServer, limit uint64) limiterFunc {
	if limit == 0 {
		return nopLimiter
	}
	return func(response *storepb.SeriesResponse) error {
		if server.chunksSent >= float64(limit) {
			return errors.Errorf("store chunks limit of %d exceeded", limit)
		}
		return nil
	}
}
