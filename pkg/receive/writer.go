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
	"github.com/thanos-io/thanos/pkg/tracing"
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
<<<<<<< HEAD
			switch err {
			case storage.ErrOutOfOrderSample:
				numSamplesOutOfOrder++
				level.Debug(tLogger).Log("msg", "Out of order sample", "lset", lset, "value", s.Value, "timestamp", s.Timestamp)
			case storage.ErrDuplicateSampleForTimestamp:
				// we don't care about duplicated sample for the same timestamp
				continue
				//numSamplesDuplicates++
				//level.Debug(tLogger).Log("msg", "Duplicate sample for timestamp", "lset", lset, "value", s.Value, "timestamp", s.Timestamp)
			case storage.ErrOutOfBounds:
				numSamplesOutOfBounds++
				level.Debug(tLogger).Log("msg", "Out of bounds metric", "lset", lset, "value", s.Value, "timestamp", s.Timestamp)
			case storage.ErrTooOldSample:
				numSamplesTooOld++
				level.Debug(tLogger).Log("msg", "Sample is too old", "lset", lset, "value", s.Value, "timestamp", s.Timestamp)
			default:
				if err != nil {
					level.Debug(tLogger).Log("msg", "Error ingesting sample", "err", err)
				}
			}
=======
			errorTracker.addSampleError(err, tLogger, lset, s.Timestamp, s.Value)
>>>>>>> thanos-io-main
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
<<<<<<< HEAD
					switch err {
					case storage.ErrOutOfOrderExemplar:
						numExemplarsOutOfOrder++
						level.Debug(exLogger).Log("msg", "Out of order exemplar")
					case storage.ErrDuplicateExemplar:
						numExemplarsDuplicate++
						level.Debug(exLogger).Log("msg", "Duplicate exemplar")
					case storage.ErrExemplarLabelLength:
						numExemplarsLabelLength++
						level.Debug(exLogger).Log("msg", "Label length for exemplar exceeds max limit", "limit", exemplar.ExemplarMaxLabelSetLength)
					default:
						level.Debug(exLogger).Log("msg", "Error ingesting exemplar", "err", err)
					}
=======
					errorTracker.addExemplarError(err, exLogger)
>>>>>>> thanos-io-main
				}
			}
		}
	}

<<<<<<< HEAD
	if numLabelsOutOfOrder > 0 {
		level.Info(tLogger).Log("msg", "Error on series with out-of-order labels", "numDropped", numLabelsOutOfOrder)
		errs.Add(errors.Wrapf(labelpb.ErrOutOfOrderLabels, "add %d series", numLabelsOutOfOrder))
	}
	if numLabelsDuplicates > 0 {
		level.Info(tLogger).Log("msg", "Error on series with duplicate labels", "numDropped", numLabelsDuplicates)
		errs.Add(errors.Wrapf(labelpb.ErrDuplicateLabels, "add %d series", numLabelsDuplicates))
	}
	if numLabelsEmpty > 0 {
		level.Info(tLogger).Log("msg", "Error on series with empty label name or value", "numDropped", numLabelsEmpty)
		errs.Add(errors.Wrapf(labelpb.ErrEmptyLabels, "add %d series", numLabelsEmpty))
	}

	if numSamplesOutOfOrder > 0 {
		level.Info(tLogger).Log("msg", "Error on ingesting out-of-order samples", "numDropped", numSamplesOutOfOrder)
		errs.Add(errors.Wrapf(storage.ErrOutOfOrderSample, "add %d samples", numSamplesOutOfOrder))
	}
	if numSamplesDuplicates > 0 {
		level.Info(tLogger).Log("msg", "Error on ingesting samples with different value but same timestamp", "numDropped", numSamplesDuplicates)
		errs.Add(errors.Wrapf(storage.ErrDuplicateSampleForTimestamp, "add %d samples", numSamplesDuplicates))
	}
	if numSamplesOutOfBounds > 0 {
		level.Info(tLogger).Log("msg", "Error on ingesting samples that are too old or are too far into the future", "numDropped", numSamplesOutOfBounds)
		errs.Add(errors.Wrapf(storage.ErrOutOfBounds, "add %d samples", numSamplesOutOfBounds))
	}
	if numSamplesTooOld > 0 {
		level.Info(tLogger).Log("msg", "Error on ingesting samples that are outside of the allowed out-of-order time window", "numDropped", numSamplesTooOld)
		errs.Add(errors.Wrapf(storage.ErrTooOldSample, "add %d samples", numSamplesTooOld))
	}

	if numExemplarsOutOfOrder > 0 {
		level.Info(tLogger).Log("msg", "Error on ingesting out-of-order exemplars", "numDropped", numExemplarsOutOfOrder)
		errs.Add(errors.Wrapf(storage.ErrOutOfOrderExemplar, "add %d exemplars", numExemplarsOutOfOrder))
	}
	if numExemplarsDuplicate > 0 {
		level.Info(tLogger).Log("msg", "Error on ingesting duplicate exemplars", "numDropped", numExemplarsDuplicate)
		errs.Add(errors.Wrapf(storage.ErrDuplicateExemplar, "add %d exemplars", numExemplarsDuplicate))
	}
	if numExemplarsLabelLength > 0 {
		level.Info(tLogger).Log("msg", "Error on ingesting exemplars with label length exceeding maximum limit", "numDropped", numExemplarsLabelLength)
		errs.Add(errors.Wrapf(storage.ErrExemplarLabelLength, "add %d exemplars", numExemplarsLabelLength))
	}
	span, _ := tracing.StartSpan(ctx, "receive_commit")
	defer span.Finish()
=======
	errs := errorTracker.collectErrors(tLogger)
>>>>>>> thanos-io-main
	if err := app.Commit(); err != nil {
		errs.Add(errors.Wrap(err, "commit samples"))
	}
	return errs.ErrOrNil()
}
