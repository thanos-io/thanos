// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

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
		numOutOfOrder  = 0
		numDuplicates  = 0
		numOutOfBounds = 0
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

	errs := &errutil.MultiError{}
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
		errs.Add(errors.Wrap(err, "commit samples"))
	}

	return errs.Err()
}

type FakeTenantAppendable struct {
	f *FakeAppendable
}

func NewFakeTenantAppendable(f *FakeAppendable) *FakeTenantAppendable {
	return &FakeTenantAppendable{f: f}
}

func (t *FakeTenantAppendable) TenantAppendable(tenantID string) (Appendable, error) {
	return t.f, nil
}

type FakeAppendable struct {
	App         storage.Appender
	AppenderErr func() error
}

var _ Appendable = &FakeAppendable{}

func nilErrFn() error {
	return nil
}

func (f *FakeAppendable) Appender(_ context.Context) (storage.Appender, error) {
	errf := f.AppenderErr
	if errf == nil {
		errf = nilErrFn
	}
	return f.App, errf()
}

type FakeAppender struct {
	sync.Mutex
	samples     map[uint64][]prompb.Sample
	addErr      func() error
	addFastErr  func() error
	commitErr   func() error
	rollbackErr func() error
}

var _ storage.Appender = &FakeAppender{}

// TODO(kakkoyun): Linter - `addFastErr` always receives `nil`.
func NewFakeAppender(addErr, addFastErr, commitErr, rollbackErr func() error) *FakeAppender { //nolint:unparam
	if addErr == nil {
		addErr = nilErrFn
	}
	if addFastErr == nil {
		addFastErr = nilErrFn
	}
	if commitErr == nil {
		commitErr = nilErrFn
	}
	if rollbackErr == nil {
		rollbackErr = nilErrFn
	}
	return &FakeAppender{
		samples:     make(map[uint64][]prompb.Sample),
		addErr:      addErr,
		addFastErr:  addFastErr,
		commitErr:   commitErr,
		rollbackErr: rollbackErr,
	}
}

func (f *FakeAppender) Get(l labels.Labels) []prompb.Sample {
	f.Lock()
	defer f.Unlock()
	s := f.samples[l.Hash()]
	res := make([]prompb.Sample, len(s))
	copy(res, s)
	return res
}

func (f *FakeAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	f.Lock()
	defer f.Unlock()
	ref := l.Hash()
	f.samples[ref] = append(f.samples[ref], prompb.Sample{Value: v, Timestamp: t})
	return ref, f.addErr()
}

func (f *FakeAppender) AddFast(ref uint64, t int64, v float64) error {
	f.Lock()
	defer f.Unlock()
	f.samples[ref] = append(f.samples[ref], prompb.Sample{Value: v, Timestamp: t})
	return f.addFastErr()
}

func (f *FakeAppender) Commit() error {
	return f.commitErr()
}

func (f *FakeAppender) Rollback() error {
	return f.rollbackErr()
}
