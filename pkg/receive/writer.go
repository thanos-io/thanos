package receive

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/prompb"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
)

// Appendable returns an Appender.
type Appendable interface {
	Appender() (storage.Appender, error)
}

type Writer struct {
	logger log.Logger
	append Appendable
}

func NewWriter(logger log.Logger, app Appendable) *Writer {
	return &Writer{
		logger: logger,
		append: app,
	}
}

func (r *Writer) Write(wreq *prompb.WriteRequest) error {
	var (
		numOutOfOrder  = 0
		numDuplicates  = 0
		numOutOfBounds = 0
	)

	app, err := r.append.Appender()

	if err != nil {
		return errors.Wrap(err, "failed to get appender")
	}

	var errs terrors.MultiError
	for _, t := range wreq.Timeseries {
		lset := make(labels.Labels, len(t.Labels))
		for j := range t.Labels {
			lset[j] = labels.Label{
				Name:  t.Labels[j].Name,
				Value: t.Labels[j].Value,
			}
		}

		// Append as many valid samples as possible, but keep track of the errors.
		for _, s := range t.Samples {
			_, err = app.Add(lset, s.Timestamp, s.Value)
			switch err {
			case nil:
				continue
			case storage.ErrOutOfOrderSample:
				numOutOfOrder++
				level.Debug(r.logger).Log("msg", "Out of order sample", "lset", lset.String(), "sample", s.String())
			case storage.ErrDuplicateSampleForTimestamp:
				numDuplicates++
				level.Debug(r.logger).Log("msg", "Duplicate sample for timestamp", "lset", lset.String(), "sample", s.String())
			case storage.ErrOutOfBounds:
				numOutOfBounds++
				level.Debug(r.logger).Log("msg", "Out of bounds metric", "lset", lset.String(), "sample", s.String())
			}
		}
	}

	if numOutOfOrder > 0 {
		level.Warn(r.logger).Log("msg", "Error on ingesting out-of-order samples", "num_dropped", numOutOfOrder)
		errs.Add(errors.Wrapf(storage.ErrOutOfOrderSample, "failed to non-fast add %d samples", numOutOfOrder))
	}
	if numDuplicates > 0 {
		level.Warn(r.logger).Log("msg", "Error on ingesting samples with different value but same timestamp", "num_dropped", numDuplicates)
		errs.Add(errors.Wrapf(storage.ErrDuplicateSampleForTimestamp, "failed to non-fast add %d samples", numDuplicates))
	}
	if numOutOfBounds > 0 {
		level.Warn(r.logger).Log("msg", "Error on ingesting samples that are too old or are too far into the future", "num_dropped", numOutOfBounds)
		errs.Add(errors.Wrapf(storage.ErrOutOfBounds, "failed to non-fast add %d samples", numOutOfBounds))
	}

	if err := app.Commit(); err != nil {
		errs.Add(errors.Wrap(err, "failed to commit"))
	}

	return errs.Err()
}
