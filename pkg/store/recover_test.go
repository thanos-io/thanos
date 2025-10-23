// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"io"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestRecoverableServer(t *testing.T) {
	t.Parallel()

	logger := log.NewNopLogger()
	store := NewRecoverableStoreServer(logger, &panicStoreServer{})

	ctx := t.Context()
	client := storepb.ServerAsClient(store)
	seriesClient, err := client.Series(ctx, &storepb.SeriesRequest{})
	testutil.Ok(t, err)

	for {
		_, err := seriesClient.Recv()
		if err == io.EOF {
			break
		}
		testutil.Ok(t, err)
	}
}

type panicStoreServer struct {
	storepb.StoreServer
}

func (m *panicStoreServer) Series(_ *storepb.SeriesRequest, _ storepb.Store_SeriesServer) error {
	panic("something went wrong.")
}
