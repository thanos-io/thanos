// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"strings"
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
	// TenantExtLabels returns the external labels (configured via --label and
	// hashring external_labels, plus the tenant label) that the receiver
	// applies to the tenant's series at query time.
	TenantExtLabels(string) labels.Labels
}

// resolveLabelConflicts renames incoming labels whose names collide with the
// tenant's external labels by prefixing them with "exported_", mirroring
// Prometheus' honor_labels=false behavior. Without this, the external labels
// applied at query time silently overwrite the colliding incoming label values,
// losing data (see thanos-io/thanos#8130).
//
// If the "exported_<name>" name is itself taken (by another incoming label or an
// external label), the prefix is applied repeatedly until a free name is found,
// matching Prometheus' resolveConflictingExposedLabels. It returns the resulting
// labels and whether any rename happened; when there is no collision the input is
// returned unchanged.
func resolveLabelConflicts(lset, extLset labels.Labels) (labels.Labels, bool) {
	if extLset.IsEmpty() {
		return lset, false
	}

	var b *labels.Builder
	extLset.Range(func(l labels.Label) {
		v := lset.Get(l.Name)
		if v == "" {
			// No incoming label collides with this external label name.
			return
		}
		if b == nil {
			b = labels.NewBuilder(lset)
		}
		newName := l.Name
		for {
			newName = model.ExportedLabelPrefix + newName
			// The renamed label must not collide with another incoming label
			// nor with an external label (which would overwrite it again at
			// query time).
			if b.Get(newName) == "" && extLset.Get(newName) == "" {
				break
			}
		}
		b.Del(l.Name)
		b.Set(newName, v)
	})

	if b == nil {
		return lset, false
	}
	return b.Labels(), true
}

// detachLabels returns a deep copy of lset with freshly allocated strings so that
// the TSDB, which holds labels long term, does not retain references to the
// underlying request buffer.
func detachLabels(lset labels.Labels) labels.Labels {
	builder := labels.NewScratchBuilder(lset.Len())
	lset.Range(func(l labels.Label) {
		builder.Add(strings.Clone(l.Name), strings.Clone(l.Value))
	})
	builder.Sort()
	return builder.Labels()
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

	extLset := r.multiTSDB.TenantExtLabels(tenantID)

	for _, t := range wreq {
		// Check if time series labels are valid. If not, skip the time series
		// and report the error.
		if err := labelpb.ValidateLabels(t.Labels); err != nil {
			lset := &labelpb.ZLabelSet{Labels: t.Labels}
			errorTracker.addLabelsError(err, lset, tLogger)
			continue
		}

		lset := labelpb.ZLabelsToPromLabels(t.Labels)

		// Rename incoming labels that collide with the tenant's external labels
		// to exported_<name>, so their values are not overwritten at query time.
		lset, renamed := resolveLabelConflicts(lset, extLset)

		// Check if the TSDB has cached reference for those labels.
		var cachedLset labels.Labels
		ref, cachedLset = getRef.GetRef(lset, lset.Hash())
		switch {
		case ref != 0:
			// Reuse the labels held by the TSDB for this existing series.
			lset = cachedLset
		case renamed:
			// New series with rebuilt labels. They may still reference the request
			// buffer, so detach their strings, as TSDB will hold them long term.
			lset = detachLabels(lset)
		default:
			// New series. Copy labels, as TSDB will hold those strings long term. Given no
			// copy unmarshal we don't want to keep memory for whole protobuf, only for labels.
			// Do the reallocation here instead of one level higher because this ensures that we
			// do _not_ intern all strings even if they are already exist. This is a high likelihood
			// that this is the case because new series are created much rarer.
			lbls := append([]labelpb.ZLabel(nil), t.Labels...)
			labelpb.ReAllocZLabelsStrings(&lbls)
			lset = labelpb.ZLabelsToPromLabels(lbls)
		}

		// Append as many valid samples as possible, but keep track of the errors.
		for _, s := range t.Samples {
			ref, err = app.Append(ref, lset, s.Timestamp, s.Value)
			errorTracker.addSampleError(err, tLogger, lset, s.Timestamp, s.Value)
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
			errorTracker.addHistogramError(err, tLogger, lset, hp.Timestamp)
		}

		// Current implementation of app.AppendExemplar doesn't create a new series, so it must be already present.
		// We drop the exemplars in case the series doesn't exist.
		if ref != 0 && len(t.Exemplars) > 0 {
			for _, ex := range t.Exemplars {
				labelpb.ReAllocZLabelsStrings(&ex.Labels)
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
