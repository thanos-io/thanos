// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package info

import (
	"context"

	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"google.golang.org/grpc"
)

// InfoServer implements the corresponding protobuf interface
// to provide information on which APIs are exposed by the given
// component.
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

// NewInfoServer creates a new server instance for given component
// and with the specified options.
func NewInfoServer(
	component string,
	options ...ServerOptionFunc,
) *InfoServer {
	srv := &InfoServer{
		component: component,
	}

	for _, o := range options {
		o(srv)
	}

	return srv
}

// ServerOptionFunc represents a functional option to configure info server.
type ServerOptionFunc func(*InfoServer)

// WithLabelSetFunc determines the function that should be executed to obtain
// the label set information. If no function is provided, the default empty
// label set is returned. Only the first function from the list is considered.
func WithLabelSetFunc(getLabelSet ...func() []labelpb.ZLabelSet) ServerOptionFunc {
	if len(getLabelSet) == 0 {
		return func(s *InfoServer) {
			s.getLabelSet = func() []labelpb.ZLabelSet { return []labelpb.ZLabelSet{} }
		}
	}

	return func(s *InfoServer) {
		s.getLabelSet = getLabelSet[0]
	}
}

// WithStoreInfoFunc determines the function that should be executed to obtain
// the store information. If no function is provided, the default empty
// store info is returned. Only the first function from the list is considered.
func WithStoreInfoFunc(getStoreInfo ...func() *infopb.StoreInfo) ServerOptionFunc {
	if len(getStoreInfo) == 0 {
		return func(s *InfoServer) {
			s.getStoreInfo = func() *infopb.StoreInfo { return &infopb.StoreInfo{} }
		}
	}

	return func(s *InfoServer) {
		s.getStoreInfo = getStoreInfo[0]
	}
}

// WithRulesInfoFunc determines the function that should be executed to obtain
// the rules information. If no function is provided, the default empty
// rules info is returned. Only the first function from the list is considered.
func WithRulesInfoFunc(getRulesInfo ...func() *infopb.RulesInfo) ServerOptionFunc {
	if len(getRulesInfo) == 0 {
		return func(s *InfoServer) {
			s.getRulesInfo = func() *infopb.RulesInfo { return &infopb.RulesInfo{} }
		}
	}

	return func(s *InfoServer) {
		s.getRulesInfo = getRulesInfo[0]
	}
}

// WithExemplarsInfoFunc determines the function that should be executed to obtain
// the exemplars information. If no function is provided, the default empty
// exemplars info is returned. Only the first function from the list is considered.
func WithExemplarsInfoFunc(getExemplarsInfo ...func() *infopb.ExemplarsInfo) ServerOptionFunc {
	if len(getExemplarsInfo) == 0 {
		return func(s *InfoServer) {
			s.getExemplarsInfo = func() *infopb.ExemplarsInfo { return &infopb.ExemplarsInfo{} }
		}
	}

	return func(s *InfoServer) {
		s.getExemplarsInfo = getExemplarsInfo[0]
	}
}

// WithTargetsInfoFunc determines the function that should be executed to obtain
// the targets information. If no function is provided, the default empty
// targets info is returned. Only the first function from the list is considered.
func WithTargetsInfoFunc(getTargetsInfo ...func() *infopb.TargetsInfo) ServerOptionFunc {
	if len(getTargetsInfo) == 0 {
		return func(s *InfoServer) {
			s.getTargetsInfo = func() *infopb.TargetsInfo { return &infopb.TargetsInfo{} }
		}
	}

	return func(s *InfoServer) {
		s.getTargetsInfo = getTargetsInfo[0]
	}
}

// WithTargetsInfoFunc determines the function that should be executed to obtain
// the targets information. If no function is provided, the default empty
// targets info is returned. Only the first function from the list is considered.
func WithMetricMetadataInfoFunc(getMetricMetadataInfo ...func() *infopb.MetricMetadataInfo) ServerOptionFunc {
	if len(getMetricMetadataInfo) == 0 {
		return func(s *InfoServer) {
			s.getMetricMetadataInfo = func() *infopb.MetricMetadataInfo { return &infopb.MetricMetadataInfo{} }
		}
	}

	return func(s *InfoServer) {
		s.getMetricMetadataInfo = getMetricMetadataInfo[0]
	}
}

// RegisterInfoServer registers the info server.
func RegisterInfoServer(infoSrv infopb.InfoServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		infopb.RegisterInfoServer(s, infoSrv)
	}
}

// Info returns the information about label set and available APIs exposed by the component.
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
