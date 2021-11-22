// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplars

import (
	"context"
	"net/url"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// Prometheus implements exemplarspb.Exemplars gRPC that allows to fetch exemplars from Prometheus.
type Prometheus struct {
	base   *url.URL
	client *promclient.Client

	extLabels func() labels.Labels
}

// NewPrometheus creates new exemplars.Prometheus.
func NewPrometheus(base *url.URL, client *promclient.Client, extLabels func() labels.Labels) *Prometheus {
	return &Prometheus{
		base:      base,
		client:    client,
		extLabels: extLabels,
	}
}

// Exemplars returns all specified exemplars from Prometheus.
func (p *Prometheus) Exemplars(r *exemplarspb.ExemplarsRequest, s exemplarspb.Exemplars_ExemplarsServer) error {
	exemplars, err := p.client.ExemplarsInGRPC(s.Context(), p.base, r.Query, r.Start, r.End)
	if err != nil {
		return err
	}

	// Prometheus does not add external labels, so we need to add on our own.
	extLset := p.extLabels()
	for _, e := range exemplars {
		// Make sure the returned series labels are sorted.
		e.SetSeriesLabels(labelpb.ExtendSortedLabels(e.SeriesLabels.PromLabels(), extLset))

		var err error
		tracing.DoInSpan(s.Context(), "send_exemplars_response", func(_ context.Context) {
			err = s.Send(&exemplarspb.ExemplarsResponse{Result: &exemplarspb.ExemplarsResponse_Data{Data: e}})
		})
		if err != nil {
			return err
		}
	}
	return nil
}
