// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"io"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestMultiTSDB(t *testing.T) {
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

		tenant := m.tenants[testTenant]
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

			for i := 0; i < 100; i++ {
				testutil.Ok(t, appendSample(m, "deleted-tenant", time.UnixMilli(int64(10+i))))
				testutil.Ok(t, appendSample(m, "compacted-tenant", time.Now().Add(-4*time.Hour)))
				testutil.Ok(t, appendSample(m, "active-tenant", time.Now().Add(time.Duration(i)*time.Second)))
			}
			testutil.Equals(t, 3, len(m.TSDBLocalClients()))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if test.bucket != nil {
				go func() {
					testutil.Ok(t, syncTSDBs(ctx, m, 10*time.Millisecond))
				}()
			}

			testutil.Ok(t, m.Prune(ctx))
			testutil.Equals(t, test.expectedTenants, len(m.TSDBLocalClients()))
			var shippedBlocks int
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

func TestMultiTSDBStats(t *testing.T) {
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
		clients := m.TSDBLocalClients()
		if len(clients) > 0 {
			clients[0] = &slowClient{clients[0]}
		}
		return clients
	}, component.Store, nil, 1*time.Minute, store.LazyRetrieval)

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
