// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"

	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// Appendable returns an Appender.
type Appendable interface {
	Appender(ctx context.Context) (storage.Appender, error)
}

type TenantStorage interface {
	TenantAppendable(string) (Appendable, error)
}

type Writer struct {
	logger    log.Logger
	multiTSDB TenantStorage
}

func NewWriter(logger log.Logger, multiTSDB TenantStorage) *Writer {
	return &Writer{
		logger:    logger,
		multiTSDB: multiTSDB,
	}
}

func (r *Writer) Write(ctx context.Context, tenantID string, wreq *prompb.WriteRequest) error {
	var (
		numOutOfOrder           = 0
		numDuplicates           = 0
		numOutOfBounds          = 0
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
		ref  uint64
		errs errutil.MultiError
	)
	for _, t := range wreq.Timeseries {
		lset := labelpb.ZLabelsToPromLabels(t.Labels)

		// Check if the TSDB has cached reference for those labels.
		ref, lset = getRef.GetRef(lset)
		if ref == 0 {
			// If not, copy labels, as TSDB will hold those strings long term. Given no
			// copy unmarshal we don't want to keep memory for whole protobuf, only for labels.
			labelpb.ReAllocZLabelsStrings(&t.Labels)
			lset = labelpb.ZLabelsToPromLabels(t.Labels)
		}

		// Append as many valid samples as possible, but keep track of the errors.
		for _, s := range t.Samples {
			ref, err = app.Append(ref, lset, s.Timestamp, s.Value)
			switch err {
			case storage.ErrOutOfOrderSample:
				numOutOfOrder++
				level.Debug(r.logger).Log("msg", "Out of order sample", "lset", lset, "sample", s)
			case storage.ErrDuplicateSampleForTimestamp:
				numDuplicates++
				level.Debug(r.logger).Log("msg", "Duplicate sample for timestamp", "lset", lset, "sample", s)
			case storage.ErrOutOfBounds:
				numOutOfBounds++
				level.Debug(r.logger).Log("msg", "Out of bounds metric", "lset", lset, "sample", s)
			}
		}

		// Current implemetation of app.AppendExemplar doesn't create a new series, so it must be already present.
		// We drop the exemplars in case the series doesn't exist.
		if ref != 0 && len(t.Exemplars) > 0 {
			for _, ex := range t.Exemplars {
				exLset := labelpb.ZLabelsToPromLabels(ex.Labels)
				logger := log.With(r.logger, "exemplarLset", exLset, "exemplar", ex.String())

				_, err = app.AppendExemplar(ref, lset, exemplar.Exemplar{
					Labels: exLset,
					Value:  ex.Value,
					Ts:     ex.Timestamp,
					HasTs:  true,
				})
				switch err {
				case storage.ErrOutOfOrderExemplar:
					numExemplarsOutOfOrder++
					level.Debug(logger).Log("msg", "Out of order exemplar")
				case storage.ErrDuplicateExemplar:
					numExemplarsDuplicate++
					level.Debug(logger).Log("msg", "Duplicate exemplar")
				case storage.ErrExemplarLabelLength:
					numExemplarsLabelLength++
					level.Debug(logger).Log("msg", "Label length for exemplar exceeds max limit", "limit", exemplar.ExemplarMaxLabelSetLength)
				default:
					if err != nil {
						level.Debug(logger).Log("msg", "Error ingesting exemplar", "err", err)
					}
				}
			}
		}
	}

	if numOutOfOrder > 0 {
		level.Warn(r.logger).Log("msg", "Error on ingesting out-of-order samples", "numDropped", numOutOfOrder)
		errs.Add(errors.Wrapf(storage.ErrOutOfOrderSample, "add %d samples", numOutOfOrder))
	}
	if numDuplicates > 0 {
		level.Warn(r.logger).Log("msg", "Error on ingesting samples with different value but same timestamp", "numDropped", numDuplicates)
		errs.Add(errors.Wrapf(storage.ErrDuplicateSampleForTimestamp, "add %d samples", numDuplicates))
	}
	if numOutOfBounds > 0 {
		level.Warn(r.logger).Log("msg", "Error on ingesting samples that are too old or are too far into the future", "numDropped", numOutOfBounds)
		errs.Add(errors.Wrapf(storage.ErrOutOfBounds, "add %d samples", numOutOfBounds))
	}
	if numExemplarsOutOfOrder > 0 {
		level.Warn(r.logger).Log("msg", "Error on ingesting out-of-order exemplars", "numDropped", numExemplarsOutOfOrder)
		errs.Add(errors.Wrapf(storage.ErrOutOfOrderExemplar, "add %d exemplars", numExemplarsOutOfOrder))
	}
	if numExemplarsDuplicate > 0 {
		level.Warn(r.logger).Log("msg", "Error on ingesting duplicate exemplars", "numDropped", numExemplarsDuplicate)
		errs.Add(errors.Wrapf(storage.ErrDuplicateExemplar, "add %d exemplars", numExemplarsDuplicate))
	}
	if numExemplarsLabelLength > 0 {
		level.Warn(r.logger).Log("msg", "Error on ingesting exemplars with label length exceeding maximum limit", "numDropped", numExemplarsLabelLength)
		errs.Add(errors.Wrapf(storage.ErrExemplarLabelLength, "add %d exemplars", numExemplarsLabelLength))
	}

	if err := app.Commit(); err != nil {
		errs.Add(errors.Wrap(err, "commit samples"))
	}
	return errs.Err()
}
