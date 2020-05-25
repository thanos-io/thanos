// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestFlushableStorage(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)

	{
		// Ensure that flushing storage does not cause data loss.
		// This test:
		// * opens a flushable storage;
		// * appends values;
		// * flushes the storage; and
		// * queries the storage to ensure the samples are present.

		dbDir, err := ioutil.TempDir("", "test")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, os.RemoveAll(dbDir)) }()

		tsdbCfg := &tsdb.Options{
			RetentionDuration: int64(time.Hour * 24 * 15 / time.Millisecond),
			NoLockfile:        true,
			MinBlockDuration:  int64(time.Hour * 2 / time.Millisecond),
			MaxBlockDuration:  int64(time.Hour * 2 / time.Millisecond),
			WALCompression:    true,
		}

		db := NewFlushableStorage(
			dbDir,
			log.NewNopLogger(),
			prometheus.NewRegistry(),
			tsdbCfg,
		)

		testutil.Ok(t, db.Open())
		defer func() { testutil.Ok(t, db.Close()) }()

		// Append data to the WAL.
		app := db.Appender()
		maxt := 1000
		for i := 0; i < maxt; i++ {
			_, err := app.Add(labels.FromStrings("thanos", "flush"), int64(i), 1.0)
			testutil.Ok(t, err)
		}
		testutil.Ok(t, app.Commit())

		// Flush the WAL.
		testutil.Ok(t, db.Flush())

		// Open again the db because Flush() leaves the db in a closed state by design.
		testutil.Ok(t, db.Open())

		querier, err := db.Querier(context.Background(), 0, int64(maxt)-1)
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, querier.Close()) }()

		// Sum the values.
		seriesSet, _, err := querier.Select(false, nil, &labels.Matcher{Type: labels.MatchEqual, Name: "thanos", Value: "flush"})
		testutil.Ok(t, err)
		sum := 0.0
		for seriesSet.Next() {
			series := seriesSet.At().Iterator()
			for series.Next() {
				_, v := series.At()
				sum += v
			}
			testutil.Ok(t, series.Err())
		}
		testutil.Ok(t, seriesSet.Err())
		testutil.Equals(t, 1000.0, sum)
	}
}
