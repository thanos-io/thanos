// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// This is copied from https://github.com/grafana/agent/blob/a23bd5cf27c2ac99695b7449d38fb12444941a1c/pkg/prom/wal/wal_test.go
// TODO(idoqo): Migrate to prometheus package when https://github.com/prometheus/prometheus/pull/8785 is ready.
package remotewrite

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
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
