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
	terrors "github.com/prometheus/prometheus/tsdb/errors"
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

	var errs terrors.MultiError
	var oooSamples, dupSamples, outOfBoundsSamples []prompb.Sample
	for _, t := range wreq.Timeseries {
		lset := labelpb.LabelsToPromLabels(t.Labels)

		var ref uint64
		// Append as many valid samples as possible, but keep track of the errors.
		for i, s := range t.Samples {
			if i == 0 {
				ref, err = app.Add(lset, s.Timestamp, s.Value)
			} else {
				err = app.AddFast(ref, s.Timestamp, s.Value)
			}

			switch err {
			case nil:
				continue
			case storage.ErrOutOfOrderSample:
				oooSamples = append(oooSamples, s)
			case storage.ErrDuplicateSampleForTimestamp:
				dupSamples = append(dupSamples, s)
			case storage.ErrOutOfBounds:
				outOfBoundsSamples = append(outOfBoundsSamples, s)
			}
		}
		if len(oooSamples) > 0 || len(outOfBoundsSamples) > 0 || len(dupSamples) > 0 {
			level.Warn(r.logger).Log("msg", "Skipped problematic samples", "outOfOrder", oooSamples,
				"duplicates", dupSamples, "outOfBounds", outOfBoundsSamples, "lset", lset)

			numOutOfOrder += len(oooSamples)
			numDuplicates += len(dupSamples)
			numOutOfBounds += len(outOfBoundsSamples)
			oooSamples = oooSamples[:0]
			dupSamples = dupSamples[:0]
			outOfBoundsSamples = outOfBoundsSamples[:0]
		}
	}

	if numOutOfOrder > 0 {
		errs.Add(errors.Wrapf(storage.ErrOutOfOrderSample, " add %d samples", numOutOfOrder))
	}
	if numDuplicates > 0 {
		errs.Add(errors.Wrapf(storage.ErrDuplicateSampleForTimestamp, "add %d samples", numDuplicates))
	}
	if numOutOfBounds > 0 {
		errs.Add(errors.Wrapf(storage.ErrOutOfBounds, "add %d samples", numOutOfBounds))
	}

	if err := app.Commit(); err != nil {
		errs.Add(errors.Wrap(err, "commit samples"))
	}

	return errs.Err()
}

type fakeTenantAppendable struct {
	f *fakeAppendable
}

func newFakeTenantAppendable(f *fakeAppendable) *fakeTenantAppendable {
	return &fakeTenantAppendable{f: f}
}

func (t *fakeTenantAppendable) TenantAppendable(tenantID string) (Appendable, error) {
	return t.f, nil
}

type fakeAppendable struct {
	appender    storage.Appender
	appenderErr func() error
}

var _ Appendable = &fakeAppendable{}

func nilErrFn() error {
	return nil
}

func (f *fakeAppendable) Appender(_ context.Context) (storage.Appender, error) {
	errf := f.appenderErr
	if errf == nil {
		errf = nilErrFn
	}
	return f.appender, errf()
}

type fakeAppender struct {
	sync.Mutex
	samples     map[uint64][]prompb.Sample
	addErr      func() error
	addFastErr  func() error
	commitErr   func() error
	rollbackErr func() error
}

var _ storage.Appender = &fakeAppender{}

// TODO(kakkoyun): Linter - `addFastErr` always receives `nil`.
func newFakeAppender(addErr, addFastErr, commitErr, rollbackErr func() error) *fakeAppender { //nolint:unparam
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
	return &fakeAppender{
		samples:     make(map[uint64][]prompb.Sample),
		addErr:      addErr,
		addFastErr:  addFastErr,
		commitErr:   commitErr,
		rollbackErr: rollbackErr,
	}
}

func (f *fakeAppender) Get(l labels.Labels) []prompb.Sample {
	f.Lock()
	defer f.Unlock()
	s := f.samples[l.Hash()]
	res := make([]prompb.Sample, len(s))
	copy(res, s)
	return res
}

func (f *fakeAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	f.Lock()
	defer f.Unlock()
	ref := l.Hash()
	f.samples[ref] = append(f.samples[ref], prompb.Sample{Value: v, Timestamp: t})
	return ref, f.addErr()
}

func (f *fakeAppender) AddFast(ref uint64, t int64, v float64) error {
	f.Lock()
	defer f.Unlock()
	f.samples[ref] = append(f.samples[ref], prompb.Sample{Value: v, Timestamp: t})
	return f.addFastErr()
}

func (f *fakeAppender) Commit() error {
	return f.commitErr()
}

func (f *fakeAppender) Rollback() error {
	return f.rollbackErr()
}
