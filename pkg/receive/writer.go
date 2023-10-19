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

func (r *Writer) Write(ctx context.Context, tenantID string, wreq *prompb.WriteRequest) error {
	tLogger := log.With(r.logger, "tenant", tenantID)

	var (
		numLabelsOutOfOrder = 0
		numLabelsDuplicates = 0
		numLabelsEmpty      = 0

		numSamplesOutOfOrder  = 0
		numSamplesDuplicates  = 0
		numSamplesOutOfBounds = 0
		numSamplesTooOld      = 0

		numExemplarsOutOfOrder  = 0
		numExemplarsDuplicate   = 0
		numExemplarsLabelLength = 0
	)

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
		ref  storage.SeriesRef
		errs writeErrors
	)
	app = &ReceiveAppender{
		tLogger:        tLogger,
		tooFarInFuture: r.opts.TooFarInFutureTimeWindow,
		Appender:       app,
	}
	for _, t := range wreq.Timeseries {
		// Check if time series labels are valid. If not, skip the time series
		// and report the error.
		if err := labelpb.ValidateLabels(t.Labels); err != nil {
			lset := &labelpb.ZLabelSet{Labels: t.Labels}
			switch err {
			case labelpb.ErrOutOfOrderLabels:
				numLabelsOutOfOrder++
				level.Debug(tLogger).Log("msg", "Out of order labels in the label set", "lset", lset.String())
			case labelpb.ErrDuplicateLabels:
				numLabelsDuplicates++
				level.Debug(tLogger).Log("msg", "Duplicate labels in the label set", "lset", lset.String())
			case labelpb.ErrEmptyLabels:
				numLabelsEmpty++
				level.Debug(tLogger).Log("msg", "Labels with empty name in the label set", "lset", lset.String())
			default:
				level.Debug(tLogger).Log("msg", "Error validating labels", "err", err)
			}

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
			switch err {
			case storage.ErrOutOfOrderSample:
				numSamplesOutOfOrder++
				level.Debug(tLogger).Log("msg", "Out of order sample", "lset", lset, "value", s.Value, "timestamp", s.Timestamp)
			case storage.ErrDuplicateSampleForTimestamp:
				numSamplesDuplicates++
				level.Debug(tLogger).Log("msg", "Duplicate sample for timestamp", "lset", lset, "value", s.Value, "timestamp", s.Timestamp)
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
		}

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
			switch err {
			case storage.ErrOutOfOrderSample:
				numSamplesOutOfOrder++
				level.Debug(tLogger).Log("msg", "Out of order histogram", "lset", lset, "timestamp", hp.Timestamp)
			case storage.ErrDuplicateSampleForTimestamp:
				numSamplesDuplicates++
				level.Debug(tLogger).Log("msg", "Duplicate histogram for timestamp", "lset", lset, "timestamp", hp.Timestamp)
			case storage.ErrOutOfBounds:
				numSamplesOutOfBounds++
				level.Debug(tLogger).Log("msg", "Out of bounds metric", "lset", lset, "timestamp", hp.Timestamp)
			case storage.ErrTooOldSample:
				numSamplesTooOld++
				level.Debug(tLogger).Log("msg", "Histogram is too old", "lset", lset, "timestamp", hp.Timestamp)
			default:
				if err != nil {
					level.Debug(tLogger).Log("msg", "Error ingesting histogram", "err", err)
				}
			}
		}

		// Current implemetation of app.AppendExemplar doesn't create a new series, so it must be already present.
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
						if err != nil {
							level.Debug(exLogger).Log("msg", "Error ingesting exemplar", "err", err)
						}
					}
				}
			}
		}
	}

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

	if err := app.Commit(); err != nil {
		errs.Add(errors.Wrap(err, "commit samples"))
	}
	return errs.ErrOrNil()
}
