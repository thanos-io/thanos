// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"fmt"
	"runtime"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type recoverableStoreServer struct {
	logger log.Logger
	storepb.StoreServer
}

func NewRecoverableStoreServer(logger log.Logger, storeServer storepb.StoreServer) *recoverableStoreServer {
	return &recoverableStoreServer{logger: logger, StoreServer: storeServer}
}

func (r *recoverableStoreServer) Series(request *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	defer r.recover(srv)
	return r.StoreServer.Series(request, srv)
}

func (r *recoverableStoreServer) recover(srv storepb.Store_SeriesServer) {
	e := recover()
	if e == nil {
		return
	}

	switch err := e.(type) {
	case runtime.Error:
		// Print the stack trace but do not inhibit the running application.
		buf := make([]byte, 64<<10)
		buf = buf[:runtime.Stack(buf, false)]

		level.Error(r.logger).Log("msg", "runtime panic in TSDB Series server", "err", err.Error(), "stacktrace", string(buf))
		if err := srv.Send(storepb.NewWarnSeriesResponse(err)); err != nil {
			level.Error(r.logger).Log("err", err)
		}
	default:
		if err := srv.Send(storepb.NewWarnSeriesResponse(errors.New(fmt.Sprintf("unknown error while processing Series: %v", e)))); err != nil {
			level.Error(r.logger).Log("err", err)
		}
	}
}
