// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// This is copied from https://github.com/grafana/agent/blob/a23bd5cf27c2ac99695b7449d38fb12444941a1c/pkg/prom/wal/wal_test.go
// TODO(idoqo): Migrate to prometheus package when https://github.com/prometheus/prometheus/pull/8785 is ready.
package remotewrite

import (
	"context"
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/stretchr/testify/require"
)

func TestStorage_InvalidSeries(t *testing.T) {
	walDir, err := ioutil.TempDir(os.TempDir(), "wal")
	require.NoError(t, err)
	defer os.RemoveAll(walDir)

	s, err := NewStorage(log.NewNopLogger(), nil, walDir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, s.Close())
	}()

	app := s.Appender(context.Background())

	_, err = app.Append(0, labels.Labels{}, 0, 0)
	require.Error(t, err, "should reject empty labels")

	_, err = app.Append(0, labels.Labels{{Name: "a", Value: "1"}, {Name: "a", Value: "2"}}, 0, 0)
	require.Error(t, err, "should reject duplicate labels")

	// Sanity check: valid series
	_, err = app.Append(0, labels.Labels{{Name: "a", Value: "1"}}, 0, 0)
	require.NoError(t, err, "should not reject valid series")
}

func TestStoraeg_TruncateAfterClose(t *testing.T) {
	walDir, err := ioutil.TempDir(os.TempDir(), "wal")
	require.NoError(t, err)
	defer os.RemoveAll(walDir)

	s, err := NewStorage(log.NewNopLogger(), nil, walDir)
	require.NoError(t, err)

	require.NoError(t, s.Close())
	require.Error(t, ErrWALClosed, s.Truncate(0))
}

type sample struct {
	ts  int64
	val float64
}

type series struct {
	name    string
	samples []sample

	ref *uint64
}

func (s *series) Write(t *testing.T, app storage.Appender) {
	t.Helper()

	lbls := labels.FromMap(map[string]string{"__name__": s.name})

	offset := 0
	if s.ref == nil {
		// Write first sample to get ref ID
		ref, err := app.Append(0, lbls, s.samples[0].ts, s.samples[0].val)
		require.NoError(t, err)

		s.ref = &ref
		offset = 1
	}

	// Write other data points with AddFast
	for _, sample := range s.samples[offset:] {
		_, err := app.Append(*s.ref, lbls, sample.ts, sample.val)
		require.NoError(t, err)
	}
}

type seriesList []*series

// Filter creates a new seriesList with series filtered by a sample
// keep predicate function.
func (s seriesList) Filter(fn func(s sample) bool) seriesList {
	var ret seriesList

	for _, entry := range s {
		var samples []sample

		for _, sample := range entry.samples {
			if fn(sample) {
				samples = append(samples, sample)
			}
		}

		if len(samples) > 0 {
			ret = append(ret, &series{
				name:    entry.name,
				ref:     entry.ref,
				samples: samples,
			})
		}
	}

	return ret
}

func (s seriesList) SeriesNames() []string {
	names := make([]string, 0, len(s))
	for _, series := range s {
		names = append(names, series.name)
	}
	return names
}

// ExpectedSamples returns the list of expected samples, sorted by ref ID and timestamp.
func (s seriesList) ExpectedSamples() []record.RefSample {
	expect := []record.RefSample{}
	for _, series := range s {
		for _, sample := range series.samples {
			expect = append(expect, record.RefSample{
				Ref: *series.ref,
				T:   sample.ts,
				V:   sample.val,
			})
		}
	}
	sort.Sort(byRefSample(expect))
	return expect
}

type byRefSample []record.RefSample

func (b byRefSample) Len() int      { return len(b) }
func (b byRefSample) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b byRefSample) Less(i, j int) bool {
	if b[i].Ref == b[j].Ref {
		return b[i].T < b[j].T
	}
	return b[i].Ref < b[j].Ref
}
