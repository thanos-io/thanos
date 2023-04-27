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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
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

		testutil.Ok(t, initTenantTSDBs(m, "foo"))
		testutil.Ok(t, initTenantTSDBs(m, "bar"))

		// Test samples.
		testutil.Ok(t, appendSample(m, "foo", labels.FromStrings("a", "1", "b", "2"), prompb.Sample{Timestamp: 1, Value: 2.41241}))
		testutil.Ok(t, appendSample(m, "foo", labels.FromStrings("a", "1", "b", "2"), prompb.Sample{Timestamp: 2, Value: 3.41241}))
		testutil.Ok(t, appendSample(m, "foo", labels.FromStrings("a", "1", "b", "2"), prompb.Sample{Timestamp: 3, Value: 4.41241}))

		// Test exemplars.
		testutil.Ok(t, appendExemplar(m, "foo", labels.FromStrings("a", "1", "b", "2"), prompb.Exemplar{Value: 1, Timestamp: 1}))
		testutil.Ok(t, appendExemplar(m, "foo", labels.FromStrings("a", "1", "b", "2"), prompb.Exemplar{Value: 2.1212, Timestamp: 2}))
		testutil.Ok(t, appendExemplar(m, "foo", labels.FromStrings("a", "1", "b", "2"), prompb.Exemplar{Value: 3.1313, Timestamp: 3}))

		// Check if not leaking.
		_, err := m.TenantAppendables("foo")
		testutil.Ok(t, err)
		_, err = m.TenantAppendables("foo")
		testutil.Ok(t, err)
		_, err = m.TenantAppendables("foo")
		testutil.Ok(t, err)

		// Test samples.
		testutil.Ok(t, appendSample(m, "bar", labels.FromStrings("a", "1", "b", "2"), prompb.Sample{Timestamp: 1, Value: 20.41241}))
		testutil.Ok(t, appendSample(m, "bar", labels.FromStrings("a", "1", "b", "2"), prompb.Sample{Timestamp: 2, Value: 30.41241}))
		testutil.Ok(t, appendSample(m, "bar", labels.FromStrings("a", "1", "b", "2"), prompb.Sample{Timestamp: 3, Value: 40.41241}))

		// Test exemplars.
		testutil.Ok(t, appendExemplar(m, "bar", labels.FromStrings("a", "1", "b", "2"), prompb.Exemplar{Value: 11, Timestamp: 1, Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("traceID", "abc"))}))
		testutil.Ok(t, appendExemplar(m, "bar", labels.FromStrings("a", "1", "b", "2"), prompb.Exemplar{Value: 22.1212, Timestamp: 2, Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("traceID", "def"))}))
		testutil.Ok(t, appendExemplar(m, "bar", labels.FromStrings("a", "1", "b", "2"), prompb.Exemplar{Value: 33.1313, Timestamp: 3, Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("traceID", "ghi"))}))

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
		apps, err := m.TenantAppendables("foo")
		testutil.Ok(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		testutil.Ok(t, runutil.Retry(1*time.Second, ctx.Done(), func() error {
			for _, app := range apps {
				_, err := app.Appender(context.Background())
				return err
			}
			return nil
		}))

		// Check if not leaking.
		_, err = m.TenantAppendables("foo")
		testutil.Ok(t, err)
		_, err = m.TenantAppendables("foo")
		testutil.Ok(t, err)
		_, err = m.TenantAppendables("foo")
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
		testutil.Ok(t, initTenantTSDBs(m, testTenant))
		testutil.Ok(t, appendSample(m, testTenant, labels.FromStrings("foo", "bar"), prompb.Sample{Timestamp: time.Now().UnixMilli()}))

		tenant := m.tenants[testTenant]
		testutil.Assert(t, func() bool {
			blocks := 0
			for _, shard := range tenant.shards {
				db := shard.readyStorage().Get()
				blocks += len(db.Blocks())
			}
			return blocks == 0
		}())
		testutil.Ok(t, m.Flush())
		testutil.Assert(t, func() bool {
			blocks := 0
			for _, shard := range tenant.shards {
				db := shard.readyStorage().Get()
				blocks += len(db.Blocks())
			}
			return blocks == 1
		}())
	})
}

var (
	expectedFooResp = &storepb.Series{
		Labels: []labelpb.ZLabel{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "replica", Value: "01"}, {Name: "shard_id", Value: "5"}, {Name: "tenant_id", Value: "foo"}},
		Chunks: []storepb.AggrChunk{{MinTime: 1, MaxTime: 3, Raw: &storepb.Chunk{Data: []byte("\000\003\002@\003L\235\2354X\315\001\330\r\257Mui\251\327:U"), Hash: 9768694233508509040}}},
	}
	expectedBarResp = &storepb.Series{
		Labels: []labelpb.ZLabel{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "replica", Value: "01"}, {Name: "shard_id", Value: "5"}, {Name: "tenant_id", Value: "bar"}},
		Chunks: []storepb.AggrChunk{{MinTime: 1, MaxTime: 3, Raw: &storepb.Chunk{Data: []byte("\000\003\002@4i\223\263\246\213\032\001\330\035i\337\322\352\323S\256t\270"), Hash: 2304287992246504442}}},
	}
)

func testMulitTSDBSeries(t *testing.T, m *MultiTSDB) {
	g := &errgroup.Group{}
	respFoo := make(chan *storepb.Series)
	respBar := make(chan *storepb.Series)

	expectedClients := 0
	for _, tenant := range m.tenants {
		expectedClients += len(tenant.shards)
	}
	for i := 0; i < 100; i++ {
		ss := m.TSDBLocalClients()
		testutil.Assert(t, len(ss) == expectedClients)

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
			SeriesLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "replica", Value: "01"}, {Name: "shard_id", Value: "5"}, {Name: "tenant_id", Value: "foo"}}},
			Exemplars: []*exemplarspb.Exemplar{
				{Value: 1, Ts: 1},
				{Value: 2.1212, Ts: 2},
				{Value: 3.1313, Ts: 3},
			},
		},
	}
	expectedBarRespExemplars = []exemplarspb.ExemplarData{
		{
			SeriesLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "replica", Value: "01"}, {Name: "shard_id", Value: "5"}, {Name: "tenant_id", Value: "bar"}}},
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
			for _, exemplar := range s["foo"] {
				srv := newExemplarsServer(context.Background())
				if err := exemplar.Exemplars(
					[][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "a", "1")}},
					0,
					10,
					srv,
				); err != nil {
					return err
				}
				if len(srv.Data) > 0 {
					respFoo <- srv.Data
				}
			}
			return nil
		})
		g.Go(func() error {
			for _, exemplar := range s["foo"] {
				srv := newExemplarsServer(context.Background())
				if err := exemplar.Exemplars(
					[][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "a", "1")}},
					0,
					10,
					srv,
				); err != nil {
					return err
				}
				if len(srv.Data) > 0 {
					respFoo <- srv.Data
				}
			}
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

			testutil.Ok(t, initTenantTSDBs(m, "deleted-tenant"))
			testutil.Ok(t, initTenantTSDBs(m, "compacted-tenant"))
			testutil.Ok(t, initTenantTSDBs(m, "active-tenant"))

			lbls := labels.FromStrings("foo", "bar")
			for i := 0; i < 100; i++ {
				testutil.Ok(t, appendSample(m, "deleted-tenant", lbls, prompb.Sample{Timestamp: int64(10 + i)}))
				testutil.Ok(t, appendSample(m, "compacted-tenant", lbls, prompb.Sample{Timestamp: time.Now().Add(-4 * time.Hour).UnixMilli()}))
				testutil.Ok(t, appendSample(m, "active-tenant", lbls, prompb.Sample{Timestamp: time.Now().Add(time.Duration(i) * time.Second).UnixMilli()}))
			}
			testutil.Equals(t, 3, len(m.tenants))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if test.bucket != nil {
				go func() {
					testutil.Ok(t, syncTSDBs(ctx, m, 10*time.Millisecond))
				}()
			}

			testutil.Ok(t, m.Prune(ctx))
			testutil.Equals(t, test.expectedTenants, len(m.tenants))
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

/*
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
*/

/*
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

			stats := m.TenantStats(labels.MetricName, test.tenants...)
			testutil.Equals(t, test.expectedStats, len(stats))
		})
	}
}
*/

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
	testutil.Ok(t, initTenantTSDBs(m, tenantID))

	// Get LabelSets of newly created TSDB.
	clients := m.TSDBLocalClients()
	for _, client := range clients {
		testutil.Ok(t, testutil.FaultOrPanicToErr(func() { client.LabelSets() }))
	}

	// Wait for tenant to become ready before terminating the test.
	// This allows the tear down procedure to cleanup properly.
	testutil.Ok(t, appendSample(m, tenantID, labels.FromStrings("foo", "bar"), prompb.Sample{Timestamp: time.Now().UnixMilli(), Value: 0}))
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
	testutil.Ok(t, initTenantTSDBs(m, "tenant-a"))
	testutil.Ok(t, initTenantTSDBs(m, "tenant-b"))

	// Append several samples to a TSDB outside the retention period.
	testutil.Ok(t, appendSample(m, "tenant-a", labels.FromStrings(labels.MetricName, "metric-a"), prompb.Sample{Timestamp: time.Now().Add(-5 * time.Hour).UnixMilli(), Value: 0}))
	testutil.Ok(t, appendSample(m, "tenant-a", labels.FromStrings(labels.MetricName, "metric-b"), prompb.Sample{Timestamp: time.Now().Add(-3 * time.Hour).UnixMilli(), Value: 0}))
	testutil.Ok(t, appendSample(m, "tenant-b", labels.FromStrings(labels.MetricName, "metric-c"), prompb.Sample{Timestamp: time.Now().Add(-1 * time.Hour).UnixMilli(), Value: 0}))

	// Append a sample within the retention period and flush all tenants.
	// This will lead deletion of blocks that fall out of the retention period.
	testutil.Ok(t, appendSample(m, "tenant-b", labels.FromStrings(labels.MetricName, "metric-d"), prompb.Sample{Timestamp: time.Now().UnixMilli(), Value: 0}))
	testutil.Ok(t, m.Flush())
}

func appendSample(m *MultiTSDB, tenant string, lbls labels.Labels, sample prompb.Sample) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	w := NewWriter(m.logger, m, &WriterOptions{})
	return w.Write(ctx, tenant, &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels:  labelpb.ZLabelsFromPromLabels(lbls),
		Samples: []prompb.Sample{sample},
	}}})
}

func appendExemplar(m *MultiTSDB, tenant string, lbls labels.Labels, exemplar prompb.Exemplar) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	w := NewWriter(m.logger, m, &WriterOptions{})
	return w.Write(ctx, tenant, &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels:    labelpb.ZLabelsFromPromLabels(lbls),
		Exemplars: []prompb.Exemplar{exemplar},
	}}})
}

func initTenantTSDBs(m *MultiTSDB, tenant string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	apps, err := m.TenantAppendables(tenant)
	if err != nil {
		return err
	}

	return runutil.Retry(1*time.Second, ctx.Done(), func() error {
		for _, app := range apps {
			_, err = app.Appender(context.Background())
			return err
		}
		return nil
	})
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
	testutil.Ok(b, initTenantTSDBs(m, "foo"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	l := labels.FromStrings("a", "1", "b", "2")

	b.ReportAllocs()
	b.ResetTimer()

	w := NewWriter(m.logger, m, &WriterOptions{})

	for i := 0; i < b.N; i++ {
		w.Write(ctx, "foo", &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
			Labels:  labelpb.ZLabelsFromPromLabels(l),
			Samples: []prompb.Sample{{Timestamp: int64(i), Value: float64(i)}},
		}}})
	}
}
