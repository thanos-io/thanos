// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"net/http"
	"strconv"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	tprompb "github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"github.com/thanos-io/thanos/pkg/tracing"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func (h *Handler) receiveOTLPHTTP(w http.ResponseWriter, r *http.Request) {
	var err error
	span, ctx := tracing.StartSpan(r.Context(), "receiveOTLPHTTP")
	span.SetTag("receiver.mode", string(h.receiverMode))
	defer span.Finish()

	tenant, err := tenancy.GetTenantFromHTTP(r, h.options.TenantHeader, h.options.DefaultTenantID, h.options.TenantField)
	if err != nil {
		level.Error(h.logger).Log("msg", "error getting tenant from HTTP", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	tLogger := log.With(h.logger, "tenant", tenant)
	span.SetTag("tenant", tenant)

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

	under, err := h.Limiter.HeadSeriesLimiter().isUnderLimit(tenant)
	if err != nil {
		level.Error(tLogger).Log("msg", "error while limiting", "err", err.Error())
	}

	// Fail request fully if tenant has exceeded set limit.
	if !under {
		http.Error(w, "tenant is above active series limit", http.StatusTooManyRequests)
		return
	}

	requestLimiter := h.Limiter.RequestLimiter()
	req, err := remote.DecodeOTLPWriteRequest(r)
	if err != nil {
		level.Error(h.logger).Log("msg", "Error decoding remote write request", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	promTimeSeries, err := h.convertToPrometheusFormat(ctx, req.Metrics())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	prwMetrics := make([]tprompb.TimeSeries, 0, len(promTimeSeries))
	totalSamples := 0
	var tpromTs tprompb.TimeSeries

	for _, ts := range promTimeSeries {
		tpromTs = tprompb.TimeSeries{
			Labels:    makeLabels(ts.Labels),
			Samples:   makeSamples(ts.Samples),
			Exemplars: makeExemplars(ts.Exemplars),
		}
		totalSamples += len(ts.Samples)
		prwMetrics = append(prwMetrics, tpromTs)
	}

	if !requestLimiter.AllowSeries(tenant, int64(len(prwMetrics))) {
		http.Error(w, "too many timeseries", http.StatusRequestEntityTooLarge)
		return
	}

	if !requestLimiter.AllowSamples(tenant, int64(totalSamples)) {
		http.Error(w, "too many samples", http.StatusRequestEntityTooLarge)
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

	wreq := tprompb.WriteRequest{
		Timeseries: prwMetrics,
		// TODO Handle metadata, requires thanos receiver support ingesting metadata
		//Metadata: otlptranslator.OtelMetricsToMetadata(),
	}

	// Exit early if the request contained no data. We don't support metadata yet. We also cannot fail here, because
	// this would mean lack of forward compatibility for remote write proto.
	if len(wreq.Timeseries) == 0 {
		// TODO(yeya24): Handle remote write metadata.
		if len(wreq.Metadata) > 0 {
			// TODO(bwplotka): Do we need this error message?
			level.Debug(tLogger).Log("msg", "only metadata from client; metadata ingestion not supported; skipping")
			return
		}
		level.Debug(tLogger).Log("msg", "empty remote write request; client bug or newer remote write protocol used?; skipping")
		return
	}

	// Apply relabeling configs.
	h.relabel(&wreq)
	if len(wreq.Timeseries) == 0 {
		level.Debug(tLogger).Log("msg", "remote write request dropped due to relabeling.")
		return
	}

	responseStatusCode := http.StatusOK
	tenantStats, err := h.handleRequest(ctx, rep, tenant, &wreq)
	if err != nil {
		level.Debug(tLogger).Log("msg", "failed to handle request", "err", err.Error())
		switch errors.Cause(err) {
		case errNotReady:
			responseStatusCode = http.StatusServiceUnavailable
		case errUnavailable:
			responseStatusCode = http.StatusServiceUnavailable
		case errConflict:
			responseStatusCode = http.StatusConflict
		case errBadReplica:
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

}

func (h *Handler) convertToPrometheusFormat(ctx context.Context, pmetrics pmetric.Metrics) ([]prompb.TimeSeries, error) {
	promConverter := prometheusremotewrite.NewPrometheusConverter()
	settings := prometheusremotewrite.Settings{
		AddMetricSuffixes:         true,
		DisableTargetInfo:         true,                                          // this must to be configured
		PromoteResourceAttributes: []string{"service.name", "service.namespace"}, // this must to be configured
	}

	annots, err := promConverter.FromMetrics(ctx, pmetrics, settings)
	ws, _ := annots.AsStrings("", 0, 0)
	if len(ws) > 0 {
		level.Warn(h.logger).Log("msg", "Warnings translating OTLP metrics to Prometheus write request", "warnings", ws)
	}

	if err != nil {
		level.Error(h.logger).Log("msg", "Error translating OTLP metrics to Prometheus write request", "err", err)
		return nil, err
	}

	return promConverter.TimeSeries(), nil
}

func makeLabels(in []prompb.Label) []labelpb.ZLabel {
	out := make([]labelpb.ZLabel, 0, len(in))
	for _, l := range in {
		out = append(out, labelpb.ZLabel{Name: l.Name, Value: l.Value})
	}
	return out
}

func makeSamples(in []prompb.Sample) []tprompb.Sample {
	out := make([]tprompb.Sample, 0, len(in))
	for _, s := range in {
		out = append(out, tprompb.Sample{
			Value:     s.Value,
			Timestamp: s.Timestamp,
		})
	}
	return out
}

func makeExemplars(in []prompb.Exemplar) []tprompb.Exemplar {
	out := make([]tprompb.Exemplar, 0, len(in))
	for _, e := range in {
		out = append(out, tprompb.Exemplar{
			Labels:    makeLabels(e.Labels),
			Value:     e.Value,
			Timestamp: e.Timestamp,
		})
	}
	return out
}
