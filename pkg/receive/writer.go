package receive

import (
	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/store/prompb"
	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
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

func (r *Writer) Receive(wreq *prompb.WriteRequest) error {
	app, err := r.append.Appender()
	if err != nil {
		return errors.Wrap(err, "failed to get appender")
	}

	for _, t := range wreq.Timeseries {
		lset := make(labels.Labels, len(t.Labels))
		for j := range t.Labels {
			lset[j] = labels.Label{
				Name:  t.Labels[j].Name,
				Value: t.Labels[j].Value,
			}
		}

		for _, s := range t.Samples {
			_, err = app.Add(lset, s.Timestamp, s.Value)
			if err != nil {
				return errors.Wrap(err, "failed to non-fast add")
			}
		}
	}

	if err := app.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit")
	}

	return nil
}
