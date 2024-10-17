// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

type writeErrorTracker struct {
	numLabelsOutOfOrder int
	numLabelsDuplicates int
	numLabelsEmpty      int

	numSamplesOutOfOrder  int
	numSamplesDuplicates  int
	numSamplesOutOfBounds int
	numSamplesTooOld      int

	numExemplarsOutOfOrder  int
	numExemplarsDuplicate   int
	numExemplarsLabelLength int
}

func (a *writeErrorTracker) addLabelsError(err error, lset *labelpb.ZLabelSet, logger log.Logger) {
	if err == nil {
		return
	}

	switch err {
	case labelpb.ErrOutOfOrderLabels:
		a.numLabelsOutOfOrder++
		level.Debug(logger).Log("msg", "Out of order labels in the label set", "lset", lset.String())
	case labelpb.ErrDuplicateLabels:
		a.numLabelsDuplicates++
		level.Debug(logger).Log("msg", "Duplicate labels in the label set", "lset", lset.String())
	case labelpb.ErrEmptyLabels:
		a.numLabelsEmpty++
		level.Debug(logger).Log("msg", "Labels with empty name in the label set", "lset", lset.String())
	default:
		level.Debug(logger).Log("msg", "Error validating labels", "err", err)
	}
}

func (a *writeErrorTracker) addSampleError(err error, tLogger log.Logger, lset labels.Labels, t int64, v float64) {
	if err == nil {
		return
	}

	switch {
	case errors.Is(err, storage.ErrOutOfOrderSample):
		a.numSamplesOutOfOrder++
		level.Debug(tLogger).Log("msg", "Out of order sample", "lset", lset, "value", v, "timestamp", t)
	case errors.Is(err, storage.ErrDuplicateSampleForTimestamp):
		a.numSamplesDuplicates++
		level.Debug(tLogger).Log("msg", "Duplicate sample for timestamp", "lset", lset, "value", v, "timestamp", t)
	case errors.Is(err, storage.ErrOutOfBounds):
		a.numSamplesOutOfBounds++
		level.Debug(tLogger).Log("msg", "Out of bounds metric", "lset", lset, "value", v, "timestamp", t)
	case errors.Is(err, storage.ErrTooOldSample):
		a.numSamplesTooOld++
		level.Debug(tLogger).Log("msg", "Sample is too old", "lset", lset, "value", v, "timestamp", t)
	default:
		level.Debug(tLogger).Log("msg", "Error ingesting sample", "err", err)
	}
}

func (a *writeErrorTracker) addHistogramError(err error, tLogger log.Logger, lset labels.Labels, timestamp int64) {
	if err == nil {
		return
	}
	switch {
	case errors.Is(err, storage.ErrOutOfOrderSample):
		a.numSamplesOutOfOrder++
		level.Debug(tLogger).Log("msg", "Out of order histogram", "lset", lset, "timestamp", timestamp)
	case errors.Is(err, storage.ErrDuplicateSampleForTimestamp):
		a.numSamplesDuplicates++
		level.Debug(tLogger).Log("msg", "Duplicate histogram for timestamp", "lset", lset, "timestamp", timestamp)
	case errors.Is(err, storage.ErrOutOfBounds):
		a.numSamplesOutOfBounds++
		level.Debug(tLogger).Log("msg", "Out of bounds metric", "lset", lset, "timestamp", timestamp)
	case errors.Is(err, storage.ErrTooOldSample):
		a.numSamplesTooOld++
		level.Debug(tLogger).Log("msg", "Histogram is too old", "lset", lset, "timestamp", timestamp)
	default:
		level.Debug(tLogger).Log("msg", "Error ingesting histogram", "err", err)
	}
}

func (a *writeErrorTracker) addExemplarError(err error, exLogger log.Logger) {
	if err == nil {
		return
	}

	switch {
	case errors.Is(err, storage.ErrOutOfOrderExemplar):
		a.numExemplarsOutOfOrder++
		level.Debug(exLogger).Log("msg", "Out of order exemplar")
	case errors.Is(err, storage.ErrDuplicateExemplar):
		a.numExemplarsDuplicate++
		level.Debug(exLogger).Log("msg", "Duplicate exemplar")
	case errors.Is(err, storage.ErrExemplarLabelLength):
		a.numExemplarsLabelLength++
		level.Debug(exLogger).Log("msg", "Label length for exemplar exceeds max limit", "limit", exemplar.ExemplarMaxLabelSetLength)
	default:
		level.Debug(exLogger).Log("msg", "Error ingesting exemplar", "err", err)
	}
}

func (a *writeErrorTracker) collectErrors(tLogger log.Logger) writeErrors {
	var errs writeErrors
	if a.numLabelsOutOfOrder > 0 {
		level.Warn(tLogger).Log("msg", "Error on series with out-of-order labels", "numDropped", a.numLabelsOutOfOrder)
		errs.Add(errors.Wrapf(labelpb.ErrOutOfOrderLabels, "add %d series", a.numLabelsOutOfOrder))
	}
	if a.numLabelsDuplicates > 0 {
		level.Warn(tLogger).Log("msg", "Error on series with duplicate labels", "numDropped", a.numLabelsDuplicates)
		errs.Add(errors.Wrapf(labelpb.ErrDuplicateLabels, "add %d series", a.numLabelsDuplicates))
	}
	if a.numLabelsEmpty > 0 {
		level.Warn(tLogger).Log("msg", "Error on series with empty label name or value", "numDropped", a.numLabelsEmpty)
		errs.Add(errors.Wrapf(labelpb.ErrEmptyLabels, "add %d series", a.numLabelsEmpty))
	}

	if a.numSamplesOutOfOrder > 0 {
		level.Warn(tLogger).Log("msg", "Error on ingesting out-of-order samples", "numDropped", a.numSamplesOutOfOrder)
		errs.Add(errors.Wrapf(storage.ErrOutOfOrderSample, "add %d samples", a.numSamplesOutOfOrder))
	}
	if a.numSamplesDuplicates > 0 {
		level.Warn(tLogger).Log("msg", "Error on ingesting samples with different value but same timestamp", "numDropped", a.numSamplesDuplicates)
		errs.Add(errors.Wrapf(storage.ErrDuplicateSampleForTimestamp, "add %d samples", a.numSamplesDuplicates))
	}
	if a.numSamplesOutOfBounds > 0 {
		level.Warn(tLogger).Log("msg", "Error on ingesting samples that are too old or are too far into the future", "numDropped", a.numSamplesOutOfBounds)
		errs.Add(errors.Wrapf(storage.ErrOutOfBounds, "add %d samples", a.numSamplesOutOfBounds))
	}
	if a.numSamplesTooOld > 0 {
		level.Warn(tLogger).Log("msg", "Error on ingesting samples that are outside of the allowed out-of-order time window", "numDropped", a.numSamplesTooOld)
		errs.Add(errors.Wrapf(storage.ErrTooOldSample, "add %d samples", a.numSamplesTooOld))
	}

	if a.numExemplarsOutOfOrder > 0 {
		level.Warn(tLogger).Log("msg", "Error on ingesting out-of-order exemplars", "numDropped", a.numExemplarsOutOfOrder)
		errs.Add(errors.Wrapf(storage.ErrOutOfOrderExemplar, "add %d exemplars", a.numExemplarsOutOfOrder))
	}
	if a.numExemplarsDuplicate > 0 {
		level.Warn(tLogger).Log("msg", "Error on ingesting duplicate exemplars", "numDropped", a.numExemplarsDuplicate)
		errs.Add(errors.Wrapf(storage.ErrDuplicateExemplar, "add %d exemplars", a.numExemplarsDuplicate))
	}
	if a.numExemplarsLabelLength > 0 {
		level.Warn(tLogger).Log("msg", "Error on ingesting exemplars with label length exceeding maximum limit", "numDropped", a.numExemplarsLabelLength)
		errs.Add(errors.Wrapf(storage.ErrExemplarLabelLength, "add %d exemplars", a.numExemplarsLabelLength))
	}
	return errs
}
