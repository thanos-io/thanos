// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type MultiTSDBStore struct {
	logger     log.Logger
	component  component.SourceStoreAPI
	tsdbStores func() []*TSDBStore
}

// NewMultiTSDBStore creates a new TSDBStore.
func NewMultiTSDBStore(logger log.Logger, _ prometheus.Registerer, component component.SourceStoreAPI, tsdbStores func() []*TSDBStore) *MultiTSDBStore {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &MultiTSDBStore{
		logger:     logger,
		component:  component,
		tsdbStores: tsdbStores,
	}
}

// Info returns store information about the Prometheus instance.
func (s *MultiTSDBStore) Info(ctx context.Context, req *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	stores := s.tsdbStores()

	resp := &storepb.InfoResponse{
		StoreType: s.component.ToProto(),
	}
	if len(stores) == 0 {
		return resp, nil
	}

	infos := make([]*storepb.InfoResponse, 0, len(stores))
	for _, store := range stores {
		info, err := store.Info(ctx, req)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}

	resp.MinTime = infos[0].MinTime
	resp.MaxTime = infos[0].MaxTime

	for i := 1; i < len(infos); i++ {
		if resp.MinTime > infos[i].MinTime {
			resp.MinTime = infos[i].MinTime
		}
		if resp.MaxTime < infos[i].MaxTime {
			resp.MaxTime = infos[i].MaxTime
		}
	}

	// We can rely on every underlying TSDB to only have one labelset, so this
	// will always allocate the correct length immediately.
	resp.LabelSets = make([]storepb.LabelSet, 0, len(infos))
	for _, info := range infos {
		resp.LabelSets = append(resp.LabelSets, info.LabelSets...)
	}

	return resp, nil
}

// Series returns all series for a requested time range and label matcher. The returned data may
// exceed the requested time bounds.
func (s *MultiTSDBStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	stores := s.tsdbStores()
	for _, store := range stores {
		err := store.Series(r, srv)
		if err != nil {
			return err
		}
	}
	return nil
}

// LabelNames returns all known label names.
func (s *MultiTSDBStore) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	names := map[string]struct{}{}
	warnings := map[string]struct{}{}

	stores := s.tsdbStores()
	for _, store := range stores {
		r, err := store.LabelNames(ctx, req)
		if err != nil {
			return nil, err
		}

		for _, l := range r.Names {
			names[l] = struct{}{}
		}

		for _, l := range r.Warnings {
			warnings[l] = struct{}{}
		}
	}

	return &storepb.LabelNamesResponse{
		Names:    keys(names),
		Warnings: keys(warnings),
	}, nil
}

func keys(m map[string]struct{}) []string {
	res := make([]string, 0, len(m))
	for k := range m {
		res = append(res, k)
	}

	return res
}

// LabelValues returns all known label values for a given label name.
func (s *MultiTSDBStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	values := map[string]struct{}{}
	warnings := map[string]struct{}{}

	stores := s.tsdbStores()
	for _, store := range stores {
		r, err := store.LabelValues(ctx, req)
		if err != nil {
			return nil, err
		}

		for _, l := range r.Values {
			values[l] = struct{}{}
		}

		for _, l := range r.Warnings {
			warnings[l] = struct{}{}
		}
	}

	return &storepb.LabelValuesResponse{
		Values:   keys(values),
		Warnings: keys(warnings),
	}, nil
}
