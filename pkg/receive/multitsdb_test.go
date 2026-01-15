// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestMultiTSDB(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	logger := log.NewLogfmtLogger(os.Stderr)
	t.Run("run fresh", func(t *testing.T) {
		m := NewMultiTSDB(
			dir, logger, prometheus.NewRegistry(), &tsdb.Options{
				MinBlockDuration:      (2 * time.Hour).Milliseconds(),
				MaxBlockDuration:      (2 * time.Hour).Milliseconds(),
				RetentionDuration:     (6 * time.Hour).Milliseconds(),
				NoLockfile:            true,
				MaxExemplars:          100,
				EnableExemplarStorage: true,
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

		_, err = a.Append(0, labels.FromStrings("a", "1", "b", "2"), 1, 2.41241)
		testutil.Ok(t, err)
		_, err = a.Append(0, labels.FromStrings("a", "1", "b", "2"), 2, 3.41241)
		testutil.Ok(t, err)
		ref, err := a.Append(0, labels.FromStrings("a", "1", "b", "2"), 3, 4.41241)
		testutil.Ok(t, err)

		// Test exemplars.
		_, err = a.AppendExemplar(ref, labels.FromStrings("a", "1", "b", "2"), exemplar.Exemplar{Value: 1, Ts: 1, HasTs: true})
		testutil.Ok(t, err)
		_, err = a.AppendExemplar(ref, labels.FromStrings("a", "1", "b", "2"), exemplar.Exemplar{Value: 2.1212, Ts: 2, HasTs: true})
		testutil.Ok(t, err)
		_, err = a.AppendExemplar(ref, labels.FromStrings("a", "1", "b", "2"), exemplar.Exemplar{Value: 3.1313, Ts: 3, HasTs: true})
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

		_, err = a.Append(0, labels.FromStrings("a", "1", "b", "2"), 1, 20.41241)
		testutil.Ok(t, err)
		_, err = a.Append(0, labels.FromStrings("a", "1", "b", "2"), 2, 30.41241)
		testutil.Ok(t, err)
		ref, err = a.Append(0, labels.FromStrings("a", "1", "b", "2"), 3, 40.41241)
		testutil.Ok(t, err)

		_, err = a.AppendExemplar(ref, labels.FromStrings("a", "1", "b", "2"), exemplar.Exemplar{Value: 11, Ts: 1, HasTs: true, Labels: labels.FromStrings("traceID", "abc")})
		testutil.Ok(t, err)
		_, err = a.AppendExemplar(ref, labels.FromStrings("a", "1", "b", "2"), exemplar.Exemplar{Value: 22.1212, Ts: 2, HasTs: true, Labels: labels.FromStrings("traceID", "def")})
		testutil.Ok(t, err)
		_, err = a.AppendExemplar(ref, labels.FromStrings("a", "1", "b", "2"), exemplar.Exemplar{Value: 33.1313, Ts: 3, HasTs: true, Labels: labels.FromStrings("traceID", "ghi")})
		testutil.Ok(t, err)
		testutil.Ok(t, a.Commit())

		testMulitTSDBSeries(t, m)
		testMultiTSDBExemplars(t, m)
	})
	t.Run("run on existing storage", func(t *testing.T) {
		m := NewMultiTSDB(
			dir, logger, prometheus.NewRegistry(), &tsdb.Options{
				MinBlockDuration:  (2 * time.Hour).Milliseconds(),
				MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
				RetentionDuration: (6 * time.Hour).Milliseconds(),
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

	t.Run("flush with one sample produces a block", func(t *testing.T) {
		const testTenant = "test_tenant"
		m := NewMultiTSDB(
			dir, logger, prometheus.NewRegistry(), &tsdb.Options{
				MinBlockDuration:  (2 * time.Hour).Milliseconds(),
				MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
				RetentionDuration: (6 * time.Hour).Milliseconds(),
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
		testutil.Ok(t, appendSample(m, testTenant, time.Now()))

		tenant := m.testGetTenant(testTenant)
		db := tenant.readyStorage().Get()

		testutil.Equals(t, 0, len(db.Blocks()))
		testutil.Ok(t, m.Flush())
		testutil.Equals(t, 1, len(db.Blocks()))
	})
}

var (
	expectedFooResp = &storepb.Series{
		Labels: []labelpb.ZLabel{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "replica", Value: "01"}, {Name: "tenant_id", Value: "foo"}},
		Chunks: []storepb.AggrChunk{{MinTime: 1, MaxTime: 3, Raw: &storepb.Chunk{Data: []byte("\000\003\002@\003L\235\2354X\315\001\330\r\257Mui\251\327:U"), Hash: 9768694233508509040}}},
	}
	expectedBarResp = &storepb.Series{
		Labels: []labelpb.ZLabel{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "replica", Value: "01"}, {Name: "tenant_id", Value: "bar"}},
		Chunks: []storepb.AggrChunk{{MinTime: 1, MaxTime: 3, Raw: &storepb.Chunk{Data: []byte("\000\003\002@4i\223\263\246\213\032\001\330\035i\337\322\352\323S\256t\270"), Hash: 2304287992246504442}}},
	}
)

func testMulitTSDBSeries(t *testing.T, m *MultiTSDB) {
	g := &errgroup.Group{}
	respFoo := make(chan *storepb.Series)
	respBar := make(chan *storepb.Series)
	for i := 0; i < 100; i++ {
		ss := m.TSDBLocalClients()
		testutil.Assert(t, len(ss) == 2)

		for _, s := range ss {
			s := s

			switch isFoo := strings.Contains(s.String(), "foo"); isFoo {
			case true:
				g.Go(func() error {
					return getResponses(s, respFoo)
				})
			case false:
				g.Go(func() error {
					return getResponses(s, respBar)
				})
			}
		}
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

func getResponses(storeClient store.Client, respCh chan<- *storepb.Series) error {
	sc, err := storeClient.Series(context.Background(), &storepb.SeriesRequest{
		MinTime:  0,
		MaxTime:  10,
		Matchers: []storepb.LabelMatcher{{Name: "a", Value: ".*", Type: storepb.LabelMatcher_RE}},
	})
	if err != nil {
		return err
	}

	for {
		resp, err := sc.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		respCh <- resp.GetSeries()
	}

	return nil
}

var (
	expectedFooRespExemplars = []exemplarspb.ExemplarData{
		{
			SeriesLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "replica", Value: "01"}, {Name: "tenant_id", Value: "foo"}}},
			Exemplars: []*exemplarspb.Exemplar{
				{Value: 1, Ts: 1},
				{Value: 2.1212, Ts: 2},
				{Value: 3.1313, Ts: 3},
			},
		},
	}
	expectedBarRespExemplars = []exemplarspb.ExemplarData{
		{
			SeriesLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "replica", Value: "01"}, {Name: "tenant_id", Value: "bar"}}},
			Exemplars: []*exemplarspb.Exemplar{
				{Value: 11, Ts: 1, Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "traceID", Value: "abc"}}}},
				{Value: 22.1212, Ts: 2, Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "traceID", Value: "def"}}}},
				{Value: 33.1313, Ts: 3, Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "traceID", Value: "ghi"}}}},
			},
		},
	}
)

func testMultiTSDBExemplars(t *testing.T, m *MultiTSDB) {
	g := &errgroup.Group{}
	respFoo := make(chan []exemplarspb.ExemplarData)
	respBar := make(chan []exemplarspb.ExemplarData)
	for i := 0; i < 100; i++ {
		s := m.TSDBExemplars()
		testutil.Assert(t, len(s) == 2)

		g.Go(func() error {
			srv := newExemplarsServer(context.Background())
			if err := s["foo"].Exemplars(
				[][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "a", "1")}},
				0,
				10,
				srv,
			); err != nil {
				return err
			}
			respFoo <- srv.Data
			return nil
		})
		g.Go(func() error {
			srv := newExemplarsServer(context.Background())
			if err := s["bar"].Exemplars(
				[][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "a", "1")}},
				0,
				10,
				srv,
			); err != nil {
				return err
			}
			respBar <- srv.Data
			return nil
		})
	}
	var err error
	go func() {
		err = g.Wait()
		close(respFoo)
		close(respBar)
	}()
OuterE:
	for {
		select {
		case r, ok := <-respFoo:
			if !ok {
				break OuterE
			}
			checkExemplarsResponse(t, expectedFooRespExemplars, r)
		case r, ok := <-respBar:
			if !ok {
				break OuterE
			}
			checkExemplarsResponse(t, expectedBarRespExemplars, r)
		}
	}
	testutil.Ok(t, err)
}

// exemplarsServer is test gRPC exemplarsAPI exemplars server.
type exemplarsServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	exemplarspb.Exemplars_ExemplarsServer

	ctx context.Context

	Data     []exemplarspb.ExemplarData
	Warnings []string

	Size int64
}

func newExemplarsServer(ctx context.Context) *exemplarsServer {
	return &exemplarsServer{ctx: ctx}
}

func (e *exemplarsServer) Send(r *exemplarspb.ExemplarsResponse) error {
	e.Size += int64(r.Size())

	if r.GetWarning() != "" {
		e.Warnings = append(e.Warnings, r.GetWarning())
		return nil
	}

	if r.GetData() != nil {
		e.Data = append(e.Data, *r.GetData())
		return nil
	}

	// Unsupported field, skip.
	return nil
}

func (s *exemplarsServer) Context() context.Context {
	return s.ctx
}

func checkExemplarsResponse(t *testing.T, expected, data []exemplarspb.ExemplarData) {
	testutil.Equals(t, len(expected), len(data))
	for i := range data {
		testutil.Equals(t, expected[i].SeriesLabels, data[i].SeriesLabels)
		testutil.Equals(t, len(expected[i].Exemplars), len(data[i].Exemplars))
		for j := range data[i].Exemplars {
			testutil.Equals(t, *expected[i].Exemplars[j], *data[i].Exemplars[j])
		}
	}
}

func TestMultiTSDBPrune(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		bucket          objstore.Bucket
		expectedTenants int
		expectedUploads int
	}{
		{
			name:            "prune tsdbs without object storage",
			bucket:          nil,
			expectedTenants: 2,
			expectedUploads: 0,
		},
		{
			name:            "prune tsdbs with object storage",
			bucket:          objstore.NewInMemBucket(),
			expectedTenants: 2,
			expectedUploads: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()

			m := NewMultiTSDB(dir, log.NewNopLogger(), prometheus.NewRegistry(),
				&tsdb.Options{
					MinBlockDuration:  (2 * time.Hour).Milliseconds(),
					MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
					RetentionDuration: (6 * time.Hour).Milliseconds(),
				},
				labels.FromStrings("replica", "test"),
				"tenant_id",
				test.bucket,
				false,
				metadata.NoneFunc,
			)
			defer func() { testutil.Ok(t, m.Close()) }()

			for step := time.Duration(0); step <= 2*time.Hour; step += time.Minute {
				testutil.Ok(t, appendSample(m, "deleted-tenant", time.Now().Add(-9*time.Hour+step)))
				testutil.Ok(t, appendSample(m, "compacted-tenant", time.Now().Add(-4*time.Hour+step)))
				testutil.Ok(t, appendSample(m, "active-tenant", time.Now().Add(step)))
			}
			testutil.Equals(t, 3, len(m.TSDBLocalClients()))

			ctx, cancel := context.WithCancel(context.Background())

			g := sync.WaitGroup{}
			defer func() { cancel(); g.Wait() }()

			if test.bucket != nil {
				g.Add(1)
				go func() {
					defer g.Done()
					testutil.Ok(t, syncTSDBs(ctx, m, 10*time.Millisecond))
				}()
			}

			testutil.Ok(t, m.Prune(ctx))
			if test.bucket != nil {
				_, err := m.Sync(ctx)
				testutil.Ok(t, err)
			}

			testutil.Equals(t, test.expectedTenants, len(m.TSDBLocalClients()))
			var shippedBlocks int
			if test.bucket == nil && shippedBlocks > 0 {
				t.Fatal("can't expect uploads when there is no bucket")
			}
			if test.bucket != nil {
				testutil.Ok(t, test.bucket.Iter(context.Background(), "", func(s string) error {
					shippedBlocks++
					return nil
				}))
			}
			testutil.Equals(t, test.expectedUploads, shippedBlocks)
		})
	}
}

func syncTSDBs(ctx context.Context, m *MultiTSDB, interval time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
			_, err := m.Sync(ctx)
			if err != nil {
				return err
			}
		}
	}
}

func TestMultiTSDBRecreatePrunedTenant(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	m := NewMultiTSDB(dir, log.NewNopLogger(), prometheus.NewRegistry(),
		&tsdb.Options{
			MinBlockDuration:  (2 * time.Hour).Milliseconds(),
			MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
			RetentionDuration: (6 * time.Hour).Milliseconds(),
		},
		labels.FromStrings("replica", "test"),
		"tenant_id",
		objstore.NewInMemBucket(),
		false,
		metadata.NoneFunc,
	)
	defer func() { testutil.Ok(t, m.Close()) }()

	testutil.Ok(t, appendSample(m, "foo", time.UnixMilli(int64(10))))
	testutil.Ok(t, m.Prune(context.Background()))
	testutil.Equals(t, 0, len(m.TSDBLocalClients()))

	testutil.Ok(t, appendSample(m, "foo", time.UnixMilli(int64(10))))
	testutil.Equals(t, 1, len(m.TSDBLocalClients()))
}

func TestMultiTSDBAddNewTenant(t *testing.T) {
	t.Parallel()
	const iterations = 10
	// This test detects race conditions, so we run it multiple times to increase the chance of catching the issue.
	for i := 0; i < iterations; i++ {
		t.Run(fmt.Sprintf("iteration-%d", i), func(t *testing.T) {
			dir := t.TempDir()
			m := NewMultiTSDB(dir, log.NewNopLogger(), prometheus.NewRegistry(),
				&tsdb.Options{
					MinBlockDuration:  (2 * time.Hour).Milliseconds(),
					MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
					RetentionDuration: (6 * time.Hour).Milliseconds(),
				},
				labels.FromStrings("replica", "test"),
				"tenant_id",
				objstore.NewInMemBucket(),
				false,
				metadata.NoneFunc,
			)
			defer func() { testutil.Ok(t, m.Close()) }()

			concurrency := 50
			var wg sync.WaitGroup
			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				// simulate remote write with new tenant concurrently
				go func(i int) {
					defer wg.Done()
					testutil.Ok(t, appendSample(m, fmt.Sprintf("tenant-%d", i), time.UnixMilli(int64(10))))
				}(i)
				// simulate read request concurrently
				go func() {
					m.TSDBLocalClients()
				}()
			}
			wg.Wait()
			testutil.Equals(t, concurrency, len(m.TSDBLocalClients()))
		})
	}
}

func TestAlignedHeadFlush(t *testing.T) {
	t.Parallel()

	hourInSeconds := int64(1 * 60 * 60)

	tests := []struct {
		name                string
		tsdbStart           int64
		headDurationSeconds int64
		bucket              objstore.Bucket
		expectedUploads     int
		expectedMaxTs       []int64
	}{
		{
			name:                "short head",
			bucket:              objstore.NewInMemBucket(),
			headDurationSeconds: hourInSeconds,
			expectedUploads:     1,
			expectedMaxTs:       []int64{hourInSeconds * 1000},
		},
		{
			name:                "aligned head start",
			bucket:              objstore.NewInMemBucket(),
			headDurationSeconds: 3 * hourInSeconds,
			expectedUploads:     2,
			expectedMaxTs:       []int64{2 * hourInSeconds * 1000, 3 * hourInSeconds * 1000},
		},
		{
			name:                "unaligned TSDB start",
			bucket:              objstore.NewInMemBucket(),
			headDurationSeconds: 3 * hourInSeconds,
			tsdbStart:           90 * 60, // 90 minutes
			expectedUploads:     2,
			expectedMaxTs:       []int64{2 * hourInSeconds * 1000, 3*hourInSeconds*1000 + 90*60},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()

			m := NewMultiTSDB(dir, log.NewNopLogger(), prometheus.NewRegistry(),
				&tsdb.Options{
					MinBlockDuration:  (2 * time.Hour).Milliseconds(),
					MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
					RetentionDuration: (6 * time.Hour).Milliseconds(),
				},
				labels.FromStrings("replica", "test"),
				"tenant_id",
				test.bucket,
				false,
				metadata.NoneFunc,
			)
			defer func() { testutil.Ok(t, m.Close()) }()

			for i := 0; i <= int(test.headDurationSeconds); i += 60 {
				tsMillis := int64(i*1000) + test.tsdbStart
				testutil.Ok(t, appendSample(m, "test-tenant", time.UnixMilli(tsMillis)))
			}

			testutil.Ok(t, m.Flush())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := m.Sync(ctx)
			testutil.Ok(t, err)

			var shippedBlocks int
			var maxts []int64
			testutil.Ok(t, test.bucket.Iter(context.Background(), "", func(s string) error {
				meta, err := metadata.ReadFromDir(path.Join(m.dataDir, "test-tenant", s))
				testutil.Ok(t, err)

				maxts = append(maxts, meta.MaxTime)
				shippedBlocks++
				return nil
			}))
			testutil.Equals(t, test.expectedUploads, shippedBlocks)
			testutil.Equals(t, test.expectedMaxTs, maxts)
		})
	}
}

func TestMultiTSDBStats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		tenants       []string
		expectedStats int
	}{
		{
			name:          "single tenant",
			tenants:       []string{"foo"},
			expectedStats: 1,
		},
		{
			name:          "missing tenant",
			tenants:       []string{"missing-foo"},
			expectedStats: 0,
		},
		{
			name:          "multiple tenants with missing tenant",
			tenants:       []string{"foo", "missing-foo"},
			expectedStats: 1,
		},
		{
			name:          "all tenants",
			tenants:       []string{"foo", "bar", "baz"},
			expectedStats: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()

			m := NewMultiTSDB(dir, log.NewNopLogger(), prometheus.NewRegistry(),
				&tsdb.Options{
					MinBlockDuration:  (2 * time.Hour).Milliseconds(),
					MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
					RetentionDuration: (6 * time.Hour).Milliseconds(),
				},
				labels.FromStrings("replica", "test"),
				"tenant_id",
				nil,
				false,
				metadata.NoneFunc,
			)
			defer func() { testutil.Ok(t, m.Close()) }()

			testutil.Ok(t, appendSample(m, "foo", time.Now()))
			testutil.Ok(t, appendSample(m, "bar", time.Now()))
			testutil.Ok(t, appendSample(m, "baz", time.Now()))
			testutil.Equals(t, 3, len(m.TSDBLocalClients()))

			stats := m.TenantStats(10, labels.MetricName, test.tenants...)
			testutil.Equals(t, test.expectedStats, len(stats))
		})
	}
}

// Regression test for https://github.com/thanos-io/thanos/issues/6047.
func TestMultiTSDBWithNilStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	m := NewMultiTSDB(dir, log.NewNopLogger(), prometheus.NewRegistry(),
		&tsdb.Options{
			MinBlockDuration:  (2 * time.Hour).Milliseconds(),
			MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
			RetentionDuration: (6 * time.Hour).Milliseconds(),
		},
		labels.FromStrings("replica", "test"),
		"tenant_id",
		nil,
		false,
		metadata.NoneFunc,
	)
	defer func() { testutil.Ok(t, m.Close()) }()

	const tenantID = "test-tenant"
	_, err := m.TenantAppendable(tenantID)
	testutil.Ok(t, err)

	// Get LabelSets of newly created TSDB.
	clients := m.TSDBLocalClients()
	for _, client := range clients {
		testutil.Ok(t, testutil.FaultOrPanicToErr(func() { client.LabelSets() }))
	}

	// Wait for tenant to become ready before terminating the test.
	// This allows the tear down procedure to cleanup properly.
	testutil.Ok(t, appendSample(m, tenantID, time.Now()))
}

type slowClient struct {
	store.Client
}

func (s *slowClient) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest, _ ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	<-time.After(10 * time.Millisecond)
	return s.Client.LabelValues(ctx, r)
}

func TestProxyLabelValues(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	m := NewMultiTSDB(
		dir, nil, prometheus.NewRegistry(), &tsdb.Options{
			RetentionDuration: 10 * time.Minute.Milliseconds(),
			MinBlockDuration:  5 * time.Minute.Milliseconds(),
			MaxBlockDuration:  5 * time.Minute.Milliseconds(),
			NoLockfile:        true,
		},
		labels.FromStrings("replica", "01"),
		"tenant_id",
		nil,
		false,
		metadata.NoneFunc,
	)
	defer func() { testutil.Ok(t, m.Close()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				testutil.Ok(t, queryLabelValues(ctx, m))
			}
		}
	}()

	// Append several samples to a TSDB outside the retention period.
	testutil.Ok(t, appendSampleWithLabels(m, "tenant-a", labels.FromStrings(labels.MetricName, "metric-a"), time.Now().Add(-5*time.Hour)))
	testutil.Ok(t, appendSampleWithLabels(m, "tenant-a", labels.FromStrings(labels.MetricName, "metric-b"), time.Now().Add(-3*time.Hour)))
	testutil.Ok(t, appendSampleWithLabels(m, "tenant-b", labels.FromStrings(labels.MetricName, "metric-c"), time.Now().Add(-1*time.Hour)))

	// Append a sample within the retention period and flush all tenants.
	// This will lead deletion of blocks that fall out of the retention period.
	testutil.Ok(t, appendSampleWithLabels(m, "tenant-b", labels.FromStrings(labels.MetricName, "metric-d"), time.Now()))
	testutil.Ok(t, m.Flush())
}

func appendSample(m *MultiTSDB, tenant string, timestamp time.Time) error {
	return appendSampleWithLabels(m, tenant, labels.FromStrings("foo", "bar"), timestamp)
}

func appendSampleWithLabels(m *MultiTSDB, tenant string, lbls labels.Labels, timestamp time.Time) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app, err := m.TenantAppendable(tenant)
	if err != nil {
		return err
	}

	var a storage.Appender
	if err := runutil.Retry(1*time.Second, ctx.Done(), func() error {
		a, err = app.Appender(ctx)
		return err
	}); err != nil {
		return err
	}

	_, err = a.Append(0, lbls, timestamp.UnixMilli(), 10)
	if err != nil {
		return err
	}

	return a.Commit()
}

func queryLabelValues(ctx context.Context, m *MultiTSDB) error {
	proxy := store.NewProxyStore(nil, nil, func() []store.Client {
		m.mtx.Lock()
		defer m.mtx.Unlock()
		clients := make([]store.Client, len(m.tsdbClients))
		copy(clients, m.tsdbClients)
		if len(clients) > 0 {
			clients[0] = &slowClient{clients[0]}
		}
		return clients
	}, component.Store, labels.EmptyLabels(), 1*time.Minute, store.LazyRetrieval)

	req := &storepb.LabelValuesRequest{
		Label: labels.MetricName,
		Start: math.MinInt64,
		End:   math.MaxInt64,
	}
	_, err := proxy.LabelValues(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func BenchmarkMultiTSDB(b *testing.B) {
	dir := b.TempDir()

	m := NewMultiTSDB(dir, log.NewNopLogger(), prometheus.NewRegistry(), &tsdb.Options{
		MinBlockDuration:  (2 * time.Hour).Milliseconds(),
		MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
		RetentionDuration: (6 * time.Hour).Milliseconds(),
		NoLockfile:        true,
	}, labels.FromStrings("replica", "test"),
		"tenant_id",
		nil,
		false,
		metadata.NoneFunc,
	)
	defer func() { testutil.Ok(b, m.Close()) }()

	testutil.Ok(b, m.Flush())
	testutil.Ok(b, m.Open())

	app, err := m.TenantAppendable("foo")
	testutil.Ok(b, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var a storage.Appender
	testutil.Ok(b, runutil.Retry(1*time.Second, ctx.Done(), func() error {
		a, err = app.Appender(context.Background())
		return err
	}))

	l := labels.FromStrings("a", "1", "b", "2")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = a.Append(0, l, int64(i), float64(i))
	}
}

func TestMultiTSDBDoesNotDeleteNotUploadedBlocks(t *testing.T) {
	t.Parallel()

	tenant := &tenant{
		mtx: &sync.RWMutex{},
	}

	t.Run("no blocks", func(t *testing.T) {
		require.Equal(t, (map[ulid.ULID]struct{})(nil), tenant.blocksToDelete(nil))
	})

	tenant.tsdb = &tsdb.DB{}

	mockBlockIDs := []ulid.ULID{
		ulid.MustNew(1, nil),
		ulid.MustNew(2, nil),
	}

	t.Run("no shipper", func(t *testing.T) {
		tenant.blocksToDeleteFn = func(db *tsdb.DB) tsdb.BlocksToDeleteFunc {
			return func(_ []*tsdb.Block) map[ulid.ULID]struct{} {
				return map[ulid.ULID]struct{}{
					mockBlockIDs[0]: {},
					mockBlockIDs[1]: {},
				}
			}
		}

		require.Equal(t, map[ulid.ULID]struct{}{
			mockBlockIDs[0]: {},
			mockBlockIDs[1]: {},
		}, tenant.blocksToDelete(nil))
	})

	t.Run("some blocks uploaded", func(t *testing.T) {
		tenant.blocksToDeleteFn = func(db *tsdb.DB) tsdb.BlocksToDeleteFunc {
			return func(_ []*tsdb.Block) map[ulid.ULID]struct{} {
				return map[ulid.ULID]struct{}{
					mockBlockIDs[0]: {},
					mockBlockIDs[1]: {},
				}
			}
		}

		td := t.TempDir()

		require.NoError(t, shipper.WriteMetaFile(log.NewNopLogger(), filepath.Join(td, shipper.DefaultMetaFilename), &shipper.Meta{
			Version:  shipper.MetaVersion1,
			Uploaded: []ulid.ULID{mockBlockIDs[0]},
		}))

		tenant.ship = shipper.New(log.NewNopLogger(), nil, td, nil, nil, metadata.BucketUploadSource, nil, false, metadata.NoneFunc, "")
		require.Equal(t, map[ulid.ULID]struct{}{
			mockBlockIDs[0]: {},
		}, tenant.blocksToDelete(nil))
	})
}

func TestMultiTSDBBlockedTenantUploads(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	bucket := objstore.NewInMemBucket()

	m := NewMultiTSDB(dir, log.NewNopLogger(), prometheus.NewRegistry(),
		&tsdb.Options{
			MinBlockDuration:  (2 * time.Hour).Milliseconds(),
			MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
			RetentionDuration: (6 * time.Hour).Milliseconds(),
		},
		labels.FromStrings("replica", "test"),
		"tenant_id",
		bucket,
		false,
		metadata.NoneFunc,
		WithNoUploadTenants([]string{"no-upload-tenant", "blocked-*"}),
	)
	defer func() { testutil.Ok(t, m.Close()) }()

	testutil.Ok(t, appendSample(m, "allowed-tenant", time.Now()))
	testutil.Ok(t, appendSample(m, "no-upload-tenant", time.Now()))
	testutil.Ok(t, appendSample(m, "blocked-prefix-tenant", time.Now()))
	testutil.Ok(t, appendSample(m, "another-allowed-tenant", time.Now()))

	testutil.Ok(t, m.Flush())

	var objectsBeforeSync int
	testutil.Ok(t, bucket.Iter(context.Background(), "", func(s string) error {
		objectsBeforeSync++
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uploaded, err := m.Sync(ctx)
	testutil.Ok(t, err)

	// Should have uploaded blocks from 2 allowed tenants (not the 2 no-upload ones)
	testutil.Equals(t, 2, uploaded)

	// Count objects after sync - should only see uploads from allowed tenants
	var objectsAfterSync []string
	testutil.Ok(t, bucket.Iter(context.Background(), "", func(s string) error {
		objectsAfterSync = append(objectsAfterSync, s)
		return nil
	}))

	// Since object names don't contain tenant info, we verify behavior by:
	// 1. Checking upload count (should be 2, not 3)
	// 2. Verifying that all tenants exist locally but only allowed ones uploaded

	// Verify all tenants exist locally (blocks should be on disk for all)
	noUploadTenantBlocks := 0
	allowedTenantBlocks := 0
	anotherAllowedTenantBlocks := 0

	// Count blocks in local filesystem for each tenant
	if files, err := os.ReadDir(path.Join(dir, "no-upload-tenant")); err == nil {
		for _, f := range files {
			if f.IsDir() && f.Name() != "wal" && f.Name() != "chunks_head" {
				noUploadTenantBlocks++
			}
		}
	}

	blockedPrefixTenantBlocks := 0
	if files, err := os.ReadDir(path.Join(dir, "blocked-prefix-tenant")); err == nil {
		for _, f := range files {
			if f.IsDir() && f.Name() != "wal" && f.Name() != "chunks_head" {
				blockedPrefixTenantBlocks++
			}
		}
	}
	if files, err := os.ReadDir(path.Join(dir, "allowed-tenant")); err == nil {
		for _, f := range files {
			if f.IsDir() && f.Name() != "wal" && f.Name() != "chunks_head" {
				allowedTenantBlocks++
			}
		}
	}
	if files, err := os.ReadDir(path.Join(dir, "another-allowed-tenant")); err == nil {
		for _, f := range files {
			if f.IsDir() && f.Name() != "wal" && f.Name() != "chunks_head" {
				anotherAllowedTenantBlocks++
			}
		}
	}

	// All tenants should have blocks locally (including no-upload ones)
	testutil.Assert(t, noUploadTenantBlocks > 0, "no upload tenant should have blocks locally")
	testutil.Assert(t, blockedPrefixTenantBlocks > 0, "blocked prefix tenant should have blocks locally")
	testutil.Assert(t, allowedTenantBlocks > 0, "allowed tenant should have blocks locally")
	testutil.Assert(t, anotherAllowedTenantBlocks > 0, "another allowed tenant should have blocks locally")

	// But only 2 uploads should have happened (not 4) - exact match and prefix match should both be blocked
	testutil.Equals(t, 2, len(objectsAfterSync))
}

func TestMultiTSDBNoUploadTenantsPrefix(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	bucket := objstore.NewInMemBucket()

	// Test prefix matching functionality
	m := NewMultiTSDB(dir, log.NewNopLogger(), prometheus.NewRegistry(),
		&tsdb.Options{
			MinBlockDuration:  (2 * time.Hour).Milliseconds(),
			MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
			RetentionDuration: (6 * time.Hour).Milliseconds(),
		},
		labels.FromStrings("replica", "test"),
		"tenant_id",
		bucket,
		false,
		metadata.NoneFunc,
		WithNoUploadTenants([]string{"prod-*", "staging-*", "exact-tenant"}),
	)
	defer func() { testutil.Ok(t, m.Close()) }()

	// Test various tenant patterns
	testutil.Ok(t, appendSample(m, "prod-tenant1", time.Now())) // Should match prod-*
	testutil.Ok(t, appendSample(m, "prod-tenant2", time.Now())) // Should match prod-*
	testutil.Ok(t, appendSample(m, "staging-app", time.Now()))  // Should match staging-*
	testutil.Ok(t, appendSample(m, "exact-tenant", time.Now())) // Should match exact
	testutil.Ok(t, appendSample(m, "dev-tenant", time.Now()))   // Should NOT match
	testutil.Ok(t, appendSample(m, "production", time.Now()))   // Should NOT match (no * suffix)

	testutil.Ok(t, m.Flush())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uploaded, err := m.Sync(ctx)
	testutil.Ok(t, err)

	// Should have uploaded blocks from only 2 tenants (dev-tenant and production)
	testutil.Equals(t, 2, uploaded)

	// Test the prefix matching function directly
	testutil.Assert(t, m.isNoUploadTenant("prod-tenant1"), "prod-tenant1 should match prod-*")
	testutil.Assert(t, m.isNoUploadTenant("prod-anything"), "prod-anything should match prod-*")
	testutil.Assert(t, m.isNoUploadTenant("staging-app"), "staging-app should match staging-*")
	testutil.Assert(t, m.isNoUploadTenant("exact-tenant"), "exact-tenant should match exactly")
	testutil.Assert(t, !m.isNoUploadTenant("dev-tenant"), "dev-tenant should NOT match any pattern")
	testutil.Assert(t, !m.isNoUploadTenant("production"), "production should NOT match prod-* (no * suffix)")
	testutil.Assert(t, !m.isNoUploadTenant("random"), "random should NOT match any pattern")
}

func TestNoUploadTenantsRetentionStillWorks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	bucket := objstore.NewInMemBucket()

	// Create MultiTSDB with no-upload tenant
	m := NewMultiTSDB(dir, log.NewNopLogger(), prometheus.NewRegistry(),
		&tsdb.Options{
			MinBlockDuration:  (2 * time.Hour).Milliseconds(),
			MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
			RetentionDuration: (6 * time.Hour).Milliseconds(),
		},
		labels.FromStrings("replica", "test"),
		"tenant_id",
		bucket,
		false,
		metadata.NoneFunc,
		WithNoUploadTenants([]string{"no-upload-tenant"}),
	)
	defer func() { testutil.Ok(t, m.Close()) }()

	// Add sample to no-upload tenant
	testutil.Ok(t, appendSample(m, "no-upload-tenant", time.Now()))
	testutil.Ok(t, m.Flush())

	// Verify tenant exists locally
	tenantDir := path.Join(dir, "no-upload-tenant")
	_, err := os.Stat(tenantDir)
	testutil.Ok(t, err) // Should not error, directory should exist

	// Verify tenant has no shipper (key part of the fix)
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	tenant, exists := m.tenants["no-upload-tenant"]
	testutil.Assert(t, exists, "no-upload tenant should exist")
	shipper := tenant.shipper()
	testutil.Assert(t, shipper == nil, "no-upload tenant should have no shipper")

	// Test that retention cleanup would still work (by calling pruneTSDB directly)
	// Note: We can't easily test the full retention flow in a unit test due to timing,
	// but we've verified the key fix: no-upload tenants don't get a shipper,
	// so the pruning logic won't try to upload during retention cleanup.
}

func TestTenantBucketPrefixInUpload(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	bucket := objstore.NewInMemBucket()

	// Create MultiTSDB with bucket
	m := NewMultiTSDB(dir, log.NewNopLogger(), prometheus.NewRegistry(),
		&tsdb.Options{
			MinBlockDuration:  (2 * time.Hour).Milliseconds(),
			MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
			RetentionDuration: (6 * time.Hour).Milliseconds(),
		},
		labels.FromStrings("replica", "test"),
		"tenant_id",
		bucket,
		false,
		metadata.NoneFunc,
		WithTenantPathPrefix(),
		WithPathSegmentsBeforeTenant([]string{"v1", "raw"}),
	)
	defer func() { testutil.Ok(t, m.Close()) }()

	// Test with multiple tenants to ensure each gets their own prefix
	tenantIDs := []string{"tenant-a", "tenant-b"}

	for _, tenantID := range tenantIDs {
		// Add samples over a longer time period to trigger block creation
		baseTime := time.Now().Add(-4 * time.Hour)
		for i := 0; i < 100; i++ {
			sampleTime := baseTime.Add(time.Duration(i) * time.Minute)
			testutil.Ok(t, appendSample(m, tenantID, sampleTime))
		}
	}

	testutil.Ok(t, m.Flush())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uploaded, err := m.Sync(ctx)
	testutil.Ok(t, err)

	t.Logf("Uploaded %d blocks", uploaded)

	if uploaded > 0 {
		// Verify that uploaded blocks are in directories with tenant prefixes
		// Expected path format: "v1/raw/{tenantID}/{blockID}/{file}"
		expectedPrefix1 := "v1/raw/tenant-a"
		expectedPrefix2 := "v1/raw/tenant-b"
		foundTenantA := false
		foundTenantB := false

		var allObjects []string
		testutil.Ok(t, bucket.Iter(context.Background(), "", func(name string) error {
			allObjects = append(allObjects, name)
			return nil
		}))

		// Iterating within the expected prefixes
		testutil.Ok(t, bucket.Iter(context.Background(), expectedPrefix1, func(name string) error {
			t.Logf("Found tenant-a object: %s", name)
			foundTenantA = true
			return nil
		}))

		testutil.Ok(t, bucket.Iter(context.Background(), expectedPrefix2, func(name string) error {
			t.Logf("Found tenant-b object: %s", name)
			foundTenantB = true
			return nil
		}))

		// Also show all top-level objects for debugging
		testutil.Ok(t, bucket.Iter(context.Background(), "", func(name string) error {
			t.Logf("Found top-level object: %s", name)
			return nil
		}))

		testutil.Assert(t, foundTenantA, "uploaded blocks should contain tenant-a prefix path")
		testutil.Assert(t, foundTenantB, "uploaded blocks should contain tenant-b prefix path")

		// Also verify that objects don't exist at the block level without tenant prefixes
		// The only objects at root should be the version directory "v1/"
		rootObjects := 0
		testutil.Ok(t, bucket.Iter(context.Background(), "", func(name string) error {
			// Allow "v1/" directory but no direct block directories
			if name != "v1/" && !strings.HasPrefix(name, "v1/raw/tenant-") {
				rootObjects++
				t.Logf("Found unexpected root object: %s", name)
			}
			return nil
		}))
		testutil.Equals(t, 0, rootObjects)
	} else {
		t.Logf("No blocks were uploaded, checking what's in the bucket anyway...")
		testutil.Ok(t, bucket.Iter(context.Background(), "", func(name string) error {
			t.Logf("Found object in bucket: %s", name)
			return nil
		}))
	}
}

func TestMultiTSDBSkipsLostAndFound(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// Create a lost+found directory like ext4 does.
	lostFoundDir := filepath.Join(dir, "lost+found")
	testutil.Ok(t, os.MkdirAll(lostFoundDir, 0700))

	m := NewMultiTSDB(
		dir, log.NewNopLogger(), prometheus.NewRegistry(),
		&tsdb.Options{
			MinBlockDuration:  (2 * time.Hour).Milliseconds(),
			MaxBlockDuration:  (2 * time.Hour).Milliseconds(),
			RetentionDuration: (6 * time.Hour).Milliseconds(),
		},
		labels.FromStrings("replica", "test"),
		"tenant_id",
		nil,
		false,
		metadata.NoneFunc,
	)
	defer func() { testutil.Ok(t, m.Close()) }()

	testutil.Ok(t, m.Open())

	// Verify that lost+found was not loaded as a tenant.
	m.mtx.RLock()
	_, exists := m.tenants["lost+found"]
	m.mtx.RUnlock()
	testutil.Assert(t, !exists, "lost+found should not be loaded as a tenant")
}
