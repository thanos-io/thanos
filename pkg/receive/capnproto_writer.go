// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"strings"

	"github.com/thanos-io/thanos/pkg/store/labelpb"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
)

type CapNProtoWriterOptions struct {
	TooFarInFutureTimeWindow int64 // Unit: nanoseconds
}

type CapNProtoWriter struct {
	logger    log.Logger
	multiTSDB TenantStorage
	opts      *CapNProtoWriterOptions
}

func NewCapNProtoWriter(logger log.Logger, multiTSDB TenantStorage, opts *CapNProtoWriterOptions) *CapNProtoWriter {
	if opts == nil {
		opts = &CapNProtoWriterOptions{}
	}
	return &CapNProtoWriter{
		logger:    logger,
		multiTSDB: multiTSDB,
		opts:      opts,
	}
}

func (r *CapNProtoWriter) Write(ctx context.Context, tenantID string, wreq *writecapnp.Request) error {
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
		errorTracker = &writeErrorTracker{}
	)
	app = &ReceiveAppender{
		tLogger:        tLogger,
		tooFarInFuture: r.opts.TooFarInFutureTimeWindow,
		Appender:       app,
	}

	var (
		series  writecapnp.Series
		builder labels.ScratchBuilder
	)
	for wreq.Next() {
		if err := wreq.At(&series); err != nil {
			return errors.Wrap(err, "request.At")
		}

		// Check if time series labels are valid. If not, skip the time series
		// and report the error.
		if err := validateLabels(series.Labels); err != nil {
			lset := &labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(series.Labels)}
			errorTracker.addLabelsError(err, lset, tLogger)
			continue
		}

		var lset labels.Labels
		// Check if the TSDB has cached reference for those labels.
		ref, lset = getRef.GetRef(series.Labels, series.Labels.Hash())
		if ref == 0 {
			// NOTE(GiedriusS): do a deep copy because the labels are reused in the capnp message.
			// Creation of new series is much rarer compared to adding extra samples
			// to an existing series.
			builder.Reset()
			series.Labels.Range(func(l labels.Label) {
				builder.Add(strings.Clone(l.Name), strings.Clone(l.Value))
			})
			lset = builder.Labels()
		}

		// Append as many valid samples as possible, but keep track of the errors.
		for _, s := range series.Samples {
			ref, err = app.Append(ref, lset, s.Timestamp, s.Value)
			errorTracker.addSampleError(err, tLogger, lset, s.Timestamp, s.Value)
		}

		for _, hp := range series.Histograms {
			ref, err = app.AppendHistogram(ref, lset, hp.Timestamp, hp.Histogram, hp.FloatHistogram)
			errorTracker.addHistogramError(err, tLogger, lset, hp.Timestamp)
		}

		// Current implementation of app.AppendExemplar doesn't create a new series, so it must be already present.
		// We drop the exemplars in case the series doesn't exist.
		if ref != 0 && len(series.Exemplars) > 0 {
			for _, ex := range series.Exemplars {
				exLogger := log.With(tLogger, "exemplarLset", ex.Labels)

				if _, err = app.AppendExemplar(ref, lset, exemplar.Exemplar{
					Labels: ex.Labels,
					Value:  ex.Value,
					Ts:     ex.Ts,
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

// ValidateLabels validates label names and values (checks for empty
// names and values, out of order labels and duplicate label names)
// Returns appropriate error if validation fails on a label.
func validateLabels(lbls labels.Labels) error {
	if lbls.Len() == 0 {
		return labelpb.ErrEmptyLabels
	}

	var (
		isFirst  = true
		prevName string
	)
	return lbls.Validate(func(l labels.Label) error {
		if l.Name == "" || l.Value == "" {
			return labelpb.ErrEmptyLabels
		}
		if isFirst {
			prevName = l.Name
			isFirst = false
			return nil
		}

		cmp := strings.Compare(l.Name, prevName)
		switch cmp {
		case 0:
			return labelpb.ErrDuplicateLabels
		case -1:
			return labelpb.ErrOutOfOrderLabels
		}

		prevName = l.Name
		return nil
	})
}
