// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// Appendable returns an Appender.
type Appendable interface {
	Appender(ctx context.Context) (storage.Appender, error)
}

type TenantStorage interface {
	TenantAppendable(string) (Appendable, error)
}

// Wraps storage.Appender to add validation and logging.
type ReceiveAppender struct {
	tLogger        log.Logger
	tooFarInFuture int64 // Unit: nanoseconds
	storage.Appender
}

func (ra *ReceiveAppender) Append(ref storage.SeriesRef, lset labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	if ra.tooFarInFuture > 0 {
		tooFar := model.Now().Add(time.Duration(ra.tooFarInFuture))
		if tooFar.Before(model.Time(t)) {
			level.Warn(ra.tLogger).Log("msg", "block metric too far in the future", "lset", lset,
				"timestamp", t, "bound", tooFar)
			// now + tooFarInFutureTimeWindow < sample timestamp
			return 0, storage.ErrOutOfBounds
		}
	}
	return ra.Appender.Append(ref, lset, t, v)
}

type WriterOptions struct {
	Intern                   bool
	TooFarInFutureTimeWindow int64 // Unit: nanoseconds
}

type Writer struct {
	logger    log.Logger
	multiTSDB TenantStorage
	opts      *WriterOptions
}

func NewWriter(logger log.Logger, multiTSDB TenantStorage, opts *WriterOptions) *Writer {
	if opts == nil {
		opts = &WriterOptions{}
	}
	return &Writer{
		logger:    logger,
		multiTSDB: multiTSDB,
		opts:      opts,
	}
}

func (r *Writer) Write(ctx context.Context, tenantID string, wreq []prompb.TimeSeries) error {
	tLogger := log.With(r.logger, "tenant", tenantID)

	s, err := r.multiTSDB.TenantAppendable(tenantID)
	if err != nil {
		return errors.Wrap(err, "get tenant appendable")
	}

	app, err := s.Appender(ctx)
	if err == tsdb.ErrNotReady {
		return err
	}
	if err != nil {
		return errors.Wrap(err, "get appender")
	}
	getRef := app.(storage.GetRef)
	var (
		ref          storage.SeriesRef
		errorTracker writeErrorTracker
	)
	app = &ReceiveAppender{
		tLogger:        tLogger,
		tooFarInFuture: r.opts.TooFarInFutureTimeWindow,
		Appender:       app,
	}

	for _, t := range wreq {
		// Check if time series labels are valid. If not, skip the time series
		// and report the error.
		if err := labelpb.ValidateLabels(t.Labels); err != nil {
			lset := &labelpb.ZLabelSet{Labels: t.Labels}
			errorTracker.addLabelsError(err, lset, tLogger)
			continue
		}

		lset := labelpb.ZLabelsToPromLabels(t.Labels)

		// Check if the TSDB has cached reference for those labels.
		ref, lset = getRef.GetRef(lset, lset.Hash())
		if ref == 0 {
			// If not, copy labels, as TSDB will hold those strings long term. Given no
			// copy unmarshal we don't want to keep memory for whole protobuf, only for labels.
			labelpb.ReAllocZLabelsStrings(&t.Labels, r.opts.Intern)
			lset = labelpb.ZLabelsToPromLabels(t.Labels)
		}

		// Append as many valid samples as possible, but keep track of the errors.
		for _, s := range t.Samples {
			ref, err = app.Append(ref, lset, s.Timestamp, s.Value)
			errorTracker.addSampleError(err, tLogger, lset, s.Timestamp, s.Value)
		}

		b := labels.ScratchBuilder{}
		b.Labels()

		for _, hp := range t.Histograms {
			var (
				h  *histogram.Histogram
				fh *histogram.FloatHistogram
			)

			if hp.IsFloatHistogram() {
				fh = prompb.FloatHistogramProtoToFloatHistogram(hp)
			} else {
				h = prompb.HistogramProtoToHistogram(hp)
			}

			ref, err = app.AppendHistogram(ref, lset, hp.Timestamp, h, fh)
			errorTracker.addHistogramError(err, tLogger, lset, hp.Timestamp)
		}

		// Current implementation of app.AppendExemplar doesn't create a new series, so it must be already present.
		// We drop the exemplars in case the series doesn't exist.
		if ref != 0 && len(t.Exemplars) > 0 {
			for _, ex := range t.Exemplars {
				exLset := labelpb.ZLabelsToPromLabels(ex.Labels)
				exLogger := log.With(tLogger, "exemplarLset", exLset, "exemplar", ex.String())

				if _, err = app.AppendExemplar(ref, lset, exemplar.Exemplar{
					Labels: exLset,
					Value:  ex.Value,
					Ts:     ex.Timestamp,
					HasTs:  true,
				}); err != nil {
					errorTracker.addExemplarError(err, exLogger)
				}
			}
		}
	}

	errs := errorTracker.collectErrors(tLogger)
	if err := app.Commit(); err != nil {
		errs.Add(errors.Wrap(err, "commit samples"))
	}
	return errs.ErrOrNil()
}
