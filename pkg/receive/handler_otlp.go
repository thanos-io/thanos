// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	otlptranslator "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	tprompb "github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"github.com/thanos-io/thanos/pkg/tracing"
	"net/http"
	"strconv"
)

func (h *Handler) receiveOTLPHTTP(w http.ResponseWriter, r *http.Request) {
	var err error
	span, ctx := tracing.StartSpan(r.Context(), "receive_http")
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
	// io.ReadAll dynamically adjust the byte slice for read data, starting from 512B.
	// Since this is receive hot path, grow upfront saving allocations and CPU time.
	// TODO Fix getting rid of the memory limiting stuff for now
	//compressed := bytes.Buffer{}
	//if r.ContentLength >= 0 {
	//	if !requestLimiter.AllowSizeBytes(tenant, r.ContentLength) {
	//		http.Error(w, "write request too large", http.StatusRequestEntityTooLarge)
	//		return
	//	}
	//	compressed.Grow(int(r.ContentLength))
	//} else {
	//	compressed.Grow(512)
	//}
	//_, err = io.Copy(&compressed, r.Body)
	//if err != nil {
	//	http.Error(w, errors.Wrap(err, "read compressed request body").Error(), http.StatusInternalServerError)
	//	return
	//}
	//reqBuf, err := s2.Decode(nil, compressed.Bytes())
	//if err != nil {
	//	level.Error(tLogger).Log("msg", "snappy decode error", "err", err)
	//	http.Error(w, errors.Wrap(err, "snappy decode error").Error(), http.StatusBadRequest)
	//	return
	//}
	//
	//if !requestLimiter.AllowSizeBytes(tenant, int64(len(reqBuf))) {
	//	http.Error(w, "write request too large", http.StatusRequestEntityTooLarge)
	//	return
	//}

	// NOTE: Due to zero copy ZLabels, Labels used from WriteRequests keeps memory
	// from the whole request. Ensure that we always copy those when we want to
	// store them for longer time.
	// TODO Fix getting rid of this for now
	//var wreq prompb.WriteRequest
	//if err := proto.Unmarshal(reqBuf, &wreq); err != nil {
	//	http.Error(w, err.Error(), http.StatusBadRequest)
	//	return
	//}

	req, err := remote.DecodeOTLPWriteRequest(r)
	if err != nil {
		level.Error(h.logger).Log("msg", "Error decoding remote write request", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// TODO expose otlptranslator.Settings as config options
	prwMetricsMap, errs := otlptranslator.FromMetrics(req.Metrics(), otlptranslator.Settings{
		AddMetricSuffixes: true,
	})
	if errs != nil {
		level.Warn(h.logger).Log("msg", "Error translating OTLP metrics to Prometheus write request", "err", errs)
	}

	prwMetrics := make([]tprompb.TimeSeries, 0, 100)

	for _, ts := range prwMetricsMap {
		t := tprompb.TimeSeries{
			Labels:    makeLabels(ts.Labels),
			Samples:   makeSamples(ts.Samples),
			Exemplars: makeExemplars(ts.Exemplars),
			// TODO handle historgrams
		}
		prwMetrics = append(prwMetrics, t)
	}

	wreq := tprompb.WriteRequest{
		Timeseries: prwMetrics,
		// TODO Handle metadata, requires thanos receiver support ingesting metadata
		//Metadata: otlptranslator.OtelMetricsToMetadata(),
	}

	rep := uint64(0)
	// If the header is empty, we assume the request is not yet replicated.
	if replicaRaw := r.Header.Get(h.options.ReplicaHeader); replicaRaw != "" {
		if rep, err = strconv.ParseUint(replicaRaw, 10, 64); err != nil {
			http.Error(w, "could not parse replica header", http.StatusBadRequest)
			return
		}
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

	if !requestLimiter.AllowSeries(tenant, int64(len(wreq.Timeseries))) {
		http.Error(w, "too many timeseries", http.StatusRequestEntityTooLarge)
		return
	}

	totalSamples := 0
	for _, timeseries := range wreq.Timeseries {
		totalSamples += len(timeseries.Samples)
	}
	if !requestLimiter.AllowSamples(tenant, int64(totalSamples)) {
		http.Error(w, "too many samples", http.StatusRequestEntityTooLarge)
		return
	}

	// Apply relabeling configs.
	h.relabel(&wreq)
	if len(wreq.Timeseries) == 0 {
		level.Debug(tLogger).Log("msg", "remote write request dropped due to relabeling.")
		return
	}

	responseStatusCode := http.StatusOK
	if err := h.handleRequest(ctx, rep, tenant, &wreq); err != nil {
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
	h.writeTimeseriesTotal.WithLabelValues(strconv.Itoa(responseStatusCode), tenant).Observe(float64(len(wreq.Timeseries)))
	h.writeSamplesTotal.WithLabelValues(strconv.Itoa(responseStatusCode), tenant).Observe(float64(totalSamples))
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
