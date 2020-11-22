// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMultiTSDB(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	logger := log.NewLogfmtLogger(os.Stderr)
	t.Run("run fresh", func(t *testing.T) {
		m := NewMultiTSDB(
			dir, logger, prometheus.NewRegistry(), &tsdb.Options{
				MinBlockDuration:  int64(2 * time.Hour / time.Millisecond),
				MaxBlockDuration:  int64(2 * time.Hour / time.Millisecond),
				RetentionDuration: int64(6 * time.Hour / time.Millisecond),
				NoLockfile:        true,
			},
			labels.FromStrings("replica", "01"),
			"tenant_id",
			nil,
			false,
			metadata.NoneFunc,
		)
		defer func() { testutil.Ok(t, m.Close()) }()

		testutil.Ok(t, m.Flush())
		testutil.Ok(t, m.Open())

		app, err := m.TenantAppendable("foo")
		testutil.Ok(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var a storage.Appender
		testutil.Ok(t, runutil.Retry(1*time.Second, ctx.Done(), func() error {
			a, err = app.Appender(context.Background())
			return err
		}))

		_, err = a.Add(labels.FromStrings("a", "1", "b", "2"), 1, 2.41241)
		testutil.Ok(t, err)
		_, err = a.Add(labels.FromStrings("a", "1", "b", "2"), 2, 3.41241)
		testutil.Ok(t, err)
		_, err = a.Add(labels.FromStrings("a", "1", "b", "2"), 3, 4.41241)
		testutil.Ok(t, err)
		testutil.Ok(t, a.Commit())

		// Check if not leaking.
		_, err = m.TenantAppendable("foo")
		testutil.Ok(t, err)
		_, err = m.TenantAppendable("foo")
		testutil.Ok(t, err)
		_, err = m.TenantAppendable("foo")
		testutil.Ok(t, err)

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		app, err = m.TenantAppendable("bar")
		testutil.Ok(t, err)

		testutil.Ok(t, runutil.Retry(1*time.Second, ctx.Done(), func() error {
			a, err = app.Appender(context.Background())
			return err
		}))

		_, err = a.Add(labels.FromStrings("a", "1", "b", "2"), 1, 20.41241)
		testutil.Ok(t, err)
		_, err = a.Add(labels.FromStrings("a", "1", "b", "2"), 2, 30.41241)
		testutil.Ok(t, err)
		_, err = a.Add(labels.FromStrings("a", "1", "b", "2"), 3, 40.41241)
		testutil.Ok(t, err)
		testutil.Ok(t, a.Commit())

		testMulitTSDBSeries(t, m)
	})
	t.Run("run on existing storage", func(t *testing.T) {
		m := NewMultiTSDB(
			dir, logger, prometheus.NewRegistry(), &tsdb.Options{
				MinBlockDuration:  int64(2 * time.Hour / time.Millisecond),
				MaxBlockDuration:  int64(2 * time.Hour / time.Millisecond),
				RetentionDuration: int64(6 * time.Hour / time.Millisecond),
				NoLockfile:        true,
			},
			labels.FromStrings("replica", "01"),
			"tenant_id",
			nil,
			false,
			metadata.NoneFunc,
		)
		defer func() { testutil.Ok(t, m.Close()) }()

		testutil.Ok(t, m.Flush())
		testutil.Ok(t, m.Open())

		// Get appender just for test.
		app, err := m.TenantAppendable("foo")
		testutil.Ok(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		testutil.Ok(t, runutil.Retry(1*time.Second, ctx.Done(), func() error {
			_, err := app.Appender(context.Background())
			return err
		}))

		// Check if not leaking.
		_, err = m.TenantAppendable("foo")
		testutil.Ok(t, err)
		_, err = m.TenantAppendable("foo")
		testutil.Ok(t, err)
		_, err = m.TenantAppendable("foo")
		testutil.Ok(t, err)

		testMulitTSDBSeries(t, m)
	})
}

var (
	expectedFooResp = []storepb.Series{
		{
			Labels: []labelpb.ZLabel{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "replica", Value: "01"}, {Name: "tenant_id", Value: "foo"}},
			Chunks: []storepb.AggrChunk{{MinTime: 1, MaxTime: 3, Raw: &storepb.Chunk{Data: []byte("\000\003\002@\003L\235\2354X\315\001\330\r\257Mui\251\327:U")}}},
		},
	}
	expectedBarResp = []storepb.Series{
		{
			Labels: []labelpb.ZLabel{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "replica", Value: "01"}, {Name: "tenant_id", Value: "bar"}},
			Chunks: []storepb.AggrChunk{{MinTime: 1, MaxTime: 3, Raw: &storepb.Chunk{Data: []byte("\000\003\002@4i\223\263\246\213\032\001\330\035i\337\322\352\323S\256t\270")}}},
		},
	}
)

func testMulitTSDBSeries(t *testing.T, m *MultiTSDB) {
	g := &errgroup.Group{}
	respFoo := make(chan []storepb.Series)
	respBar := make(chan []storepb.Series)
	for i := 0; i < 100; i++ {
		s := m.TSDBStores()
		testutil.Assert(t, len(s) == 2)

		g.Go(func() error {
			srv := newStoreSeriesServer(context.Background())
			if err := s["foo"].Series(&storepb.SeriesRequest{
				MinTime:  0,
				MaxTime:  10,
				Matchers: []storepb.LabelMatcher{{Name: "a", Value: ".*", Type: storepb.LabelMatcher_RE}},
			}, srv); err != nil {
				return err
			}
			respFoo <- srv.SeriesSet
			return nil
		})
		g.Go(func() error {
			srv := newStoreSeriesServer(context.Background())
			if err := s["bar"].Series(&storepb.SeriesRequest{
				MinTime:  0,
				MaxTime:  10,
				Matchers: []storepb.LabelMatcher{{Name: "a", Value: ".*", Type: storepb.LabelMatcher_RE}},
			}, srv); err != nil {
				return err
			}
			respBar <- srv.SeriesSet
			return nil
		})
	}
	var err error
	go func() {
		err = g.Wait()
		close(respFoo)
		close(respBar)
	}()
Outer:
	for {
		select {
		case r, ok := <-respFoo:
			if !ok {
				break Outer
			}
			testutil.Equals(t, expectedFooResp, r)
		case r, ok := <-respBar:
			if !ok {
				break Outer
			}
			testutil.Equals(t, expectedBarResp, r)
		}
	}
	testutil.Ok(t, err)
}

// storeSeriesServer is test gRPC storeAPI series server.
// TODO(bwplotka): Make this part of some common library. We copy and paste this also in pkg/store.
type storeSeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer

	ctx context.Context

	SeriesSet []storepb.Series
	Warnings  []string
	HintsSet  []*types.Any

	Size int64
}

func newStoreSeriesServer(ctx context.Context) *storeSeriesServer {
	return &storeSeriesServer{ctx: ctx}
}

func (s *storeSeriesServer) Send(r *storepb.SeriesResponse) error {
	s.Size += int64(r.Size())

	if r.GetWarning() != "" {
		s.Warnings = append(s.Warnings, r.GetWarning())
		return nil
	}

	if r.GetSeries() != nil {
		s.SeriesSet = append(s.SeriesSet, *r.GetSeries())
		return nil
	}

	if r.GetHints() != nil {
		s.HintsSet = append(s.HintsSet, r.GetHints())
		return nil
	}

	// Unsupported field, skip.
	return nil
}

func (s *storeSeriesServer) Context() context.Context {
	return s.ctx
}
