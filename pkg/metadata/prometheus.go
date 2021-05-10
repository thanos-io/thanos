// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"net/url"

	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/promclient"
)

// Prometheus implements metadatapb.Metadata gRPC service that allows to fetch metric metadata from Prometheus HTTP /api/v1/metadata endpoint.
type Prometheus struct {
	base   *url.URL
	client *promclient.Client
}

// NewPrometheus creates a new metadata.Prometheus.
func NewPrometheus(base *url.URL, client *promclient.Client) *Prometheus {
	return &Prometheus{
		base:   base,
		client: client,
	}
}

// MetricMetadata returns all specified metric metadata from Prometheus.
func (p *Prometheus) MetricMetadata(r *metadatapb.MetricMetadataRequest, s metadatapb.Metadata_MetricMetadataServer) error {
	md, err := p.client.MetricMetadataInGRPC(s.Context(), p.base, r.Metric, int(r.Limit))
	if err != nil {
		return err
	}

	return s.Send(&metadatapb.MetricMetadataResponse{Result: &metadatapb.MetricMetadataResponse_Metadata{
		Metadata: metadatapb.FromMetadataMap(md)}})
}
