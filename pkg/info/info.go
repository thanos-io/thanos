// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package info

import (
	"context"

	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"google.golang.org/grpc"
)

type InfoServer struct {
	infopb.UnimplementedInfoServer

	component string

	getLabelSet           func() []labelpb.ZLabelSet
	getStoreInfo          func() *infopb.StoreInfo
	getExemplarsInfo      func() *infopb.ExemplarsInfo
	getRulesInfo          func() *infopb.RulesInfo
	getTargetsInfo        func() *infopb.TargetsInfo
	getMetricMetadataInfo func() *infopb.MetricMetadataInfo
}

func NewInfoServer(
	component string,
	options ...ServerOption,
) *InfoServer {
	srv := &InfoServer{
		component: component,
	}

	for _, o := range options {
		o.applyServerOption(srv)
	}

	return srv
}

type ServerOption interface {
	applyServerOption(*InfoServer)
}

type serverOptionFunc func(*InfoServer)

func (fn serverOptionFunc) applyServerOption(cfg *InfoServer) {
	fn(cfg)
}

func WithLabelSetFunc(getLabelSet ...func() []labelpb.ZLabelSet) ServerOption {
	if len(getLabelSet) == 0 {
		return serverOptionFunc(func(s *InfoServer) {
			s.getLabelSet = func() []labelpb.ZLabelSet { return []labelpb.ZLabelSet{} }
		})
	}

	return serverOptionFunc(func(s *InfoServer) {
		s.getLabelSet = getLabelSet[0]
	})
}

func WithStoreInfoFunc(getStoreInfo ...func() *infopb.StoreInfo) ServerOption {
	if len(getStoreInfo) == 0 {
		return serverOptionFunc(func(s *InfoServer) {
			s.getStoreInfo = func() *infopb.StoreInfo { return &infopb.StoreInfo{} }
		})
	}

	return serverOptionFunc(func(s *InfoServer) {
		s.getStoreInfo = getStoreInfo[0]
	})
}

func WithRulesInfoFunc(getRulesInfo ...func() *infopb.RulesInfo) ServerOption {
	if len(getRulesInfo) == 0 {
		return serverOptionFunc(func(s *InfoServer) {
			s.getRulesInfo = func() *infopb.RulesInfo { return &infopb.RulesInfo{} }
		})
	}

	return serverOptionFunc(func(s *InfoServer) {
		s.getRulesInfo = getRulesInfo[0]
	})
}

func WithExemplarsInfoFunc(getExemplarsInfo ...func() *infopb.ExemplarsInfo) ServerOption {
	if len(getExemplarsInfo) == 0 {
		return serverOptionFunc(func(s *InfoServer) {
			s.getExemplarsInfo = func() *infopb.ExemplarsInfo { return &infopb.ExemplarsInfo{} }
		})
	}

	return serverOptionFunc(func(s *InfoServer) {
		s.getExemplarsInfo = getExemplarsInfo[0]
	})
}

func WithTargetsInfoFunc(getTargetsInfo ...func() *infopb.TargetsInfo) ServerOption {
	if len(getTargetsInfo) == 0 {
		return serverOptionFunc(func(s *InfoServer) {
			s.getTargetsInfo = func() *infopb.TargetsInfo { return &infopb.TargetsInfo{} }
		})
	}

	return serverOptionFunc(func(s *InfoServer) {
		s.getTargetsInfo = getTargetsInfo[0]
	})
}

func WithMetricMetadataInfoFunc(getMetricMetadataInfo ...func() *infopb.MetricMetadataInfo) ServerOption {
	if len(getMetricMetadataInfo) == 0 {
		return serverOptionFunc(func(s *InfoServer) {
			s.getMetricMetadataInfo = func() *infopb.MetricMetadataInfo { return &infopb.MetricMetadataInfo{} }
		})
	}

	return serverOptionFunc(func(s *InfoServer) {
		s.getMetricMetadataInfo = getMetricMetadataInfo[0]
	})
}

// RegisterInfoServer register info server.
func RegisterInfoServer(infoSrv infopb.InfoServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		infopb.RegisterInfoServer(s, infoSrv)
	}
}

func (srv *InfoServer) Info(ctx context.Context, req *infopb.InfoRequest) (*infopb.InfoResponse, error) {

	if srv.getLabelSet == nil {
		srv.getLabelSet = func() []labelpb.ZLabelSet { return nil }
	}

	if srv.getStoreInfo == nil {
		srv.getStoreInfo = func() *infopb.StoreInfo { return nil }
	}

	if srv.getExemplarsInfo == nil {
		srv.getExemplarsInfo = func() *infopb.ExemplarsInfo { return nil }
	}

	if srv.getRulesInfo == nil {
		srv.getRulesInfo = func() *infopb.RulesInfo { return nil }
	}

	if srv.getTargetsInfo == nil {
		srv.getTargetsInfo = func() *infopb.TargetsInfo { return nil }
	}

	if srv.getMetricMetadataInfo == nil {
		srv.getMetricMetadataInfo = func() *infopb.MetricMetadataInfo { return nil }
	}

	resp := &infopb.InfoResponse{
		LabelSets:      srv.getLabelSet(),
		ComponentType:  srv.component,
		Store:          srv.getStoreInfo(),
		Exemplars:      srv.getExemplarsInfo(),
		Rules:          srv.getRulesInfo(),
		Targets:        srv.getTargetsInfo(),
		MetricMetadata: srv.getMetricMetadataInfo(),
	}

	return resp, nil
}
