// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestRecoverableServer(t *testing.T) {
	logger := log.NewNopLogger()
	store := NewRecoverableStoreServer(logger, &panicStoreServer{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := storepb.NewInProcessStream(ctx, 1)

	testutil.Ok(t, store.Series(&storepb.SeriesRequest{}, srv))
}

type panicStoreServer struct {
	storepb.StoreServer
}

func (m *panicStoreServer) Series(_ *storepb.SeriesRequest, _ storepb.Store_SeriesServer) error {
	panic("something went wrong.")
}
