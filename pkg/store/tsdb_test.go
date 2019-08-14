package store

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestTSDBStore_Info(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := testutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	tsdbStore := NewTSDBStore(nil, nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

	resp, err := tsdbStore.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)

	testutil.Equals(t, []storepb.Label{{Name: "region", Value: "eu-west"}}, resp.Labels)
	testutil.Equals(t, storepb.StoreType_RULE, resp.StoreType)
	testutil.Equals(t, int64(0), resp.MinTime)
	testutil.Equals(t, int64(math.MaxInt64), resp.MaxTime)
}

// Regression test for https://github.com/thanos-io/thanos/issues/1038.
func TestTSDBStore_Series_SplitSamplesIntoChunksWithMaxSizeOfUint16_e2e(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	db, err := testutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	testSeries_SplitSamplesIntoChunksWithMaxSizeOfUint16_e2e(t, db.Appender(), func() storepb.StoreServer {
		tsdbStore := NewTSDBStore(nil, nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

		return tsdbStore
	})
}
