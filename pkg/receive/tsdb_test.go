// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/testutil"
)

// TestHeadFlushIntoBlock ensures that the head flush
// creates a block that is ready for shipping and
// that flushing the storage does not cause data loss.
// This test:
// * opens a storage;
// * appends values;
// * flushes the storage wal/head; and
// * queries the storage to ensure the samples are present from the flushed block.
func TestHeadFlushIntoBlock(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)

	dbDir, err := ioutil.TempDir("", "testFlush")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dbDir)) }()

	// Open a DB and append data to the WAL.
	tsdbCfg := &tsdb.Options{
		RetentionDuration: int64(time.Hour * 24 * 15 / time.Millisecond),
		NoLockfile:        true,
		MinBlockDuration:  int64(time.Hour * 2 / time.Millisecond),
		MaxBlockDuration:  int64(time.Hour * 2 / time.Millisecond),
		WALCompression:    true,
	}

	db, err := tsdb.Open(dbDir, log.NewNopLogger(), prometheus.NewRegistry(), tsdbCfg)
	testutil.Ok(t, err)
	app := db.Appender()
	maxt := 1000
	for i := 0; i < maxt; i++ {
		_, err := app.Add(labels.FromStrings("thanos", "flush"), int64(i), 1.0)
		testutil.Ok(t, err)
	}
	testutil.Ok(t, app.Commit())

	// Flush the WAL/Head.
	testutil.Ok(t, db.CompactHead(tsdb.NewRangeHead(db.Head(), 0, int64(maxt)-1)))
	testutil.Ok(t, db.Close())
	testutil.Ok(t, deleteNonBlocks(db.Dir()))

	// Reopen the db to check the new block contains all data.
	db, err = tsdb.Open(dbDir, log.NewNopLogger(), prometheus.NewRegistry(), tsdbCfg)
	testutil.Ok(t, err)
	testutil.Equals(t, len(db.Blocks()), 1)
	defer func() { testutil.Ok(t, db.Close()) }()
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

func deleteNonBlocks(dbDir string) error {
	dirs, err := ioutil.ReadDir(dbDir)
	if err != nil {
		return err
	}
	for _, dir := range dirs {
		if _, ok := block.IsBlockDir(dir.Name()); !ok {
			if err := os.RemoveAll(filepath.Join(dbDir, dir.Name())); err != nil {
				return err
			}
		}
	}
	dirs, err = ioutil.ReadDir(dbDir)
	if err != nil {
		return err
	}
	for _, dir := range dirs {
		if _, ok := block.IsBlockDir(dir.Name()); !ok {
			return errors.Errorf("root folder:%v still hase non block directory:%v", dbDir, dir.Name())
		}
	}

	return nil
}
