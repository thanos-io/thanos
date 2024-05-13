// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"fmt"
	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/shipper"
	"net/http"
	"net/url"
	"os"

	"github.com/thanos-io/thanos/pkg/api"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
)

// SidecarAPI is a very simple API used by Thanos Sidecar.
type SidecarAPI struct {
	baseAPI     *api.BaseAPI
	client      *promclient.Client
	shipper     *shipper.Shipper
	promURL     *url.URL
	dataDir     string
	logger      log.Logger
	reg         prometheus.Registerer
	disableCORS bool
}

// NewSidecarAPI creates an Thanos Sidecar API.
func NewSidecarAPI(
	logger log.Logger,
	reg prometheus.Registerer,
	disableCORS bool,
	client *promclient.Client,
	shipper *shipper.Shipper,
	dataDir string,
	promURL *url.URL,
	flagsMap map[string]string,
) *SidecarAPI {
	return &SidecarAPI{
		baseAPI:     api.NewBaseAPI(logger, disableCORS, flagsMap),
		logger:      logger,
		client:      client,
		reg:         reg,
		shipper:     shipper,
		dataDir:     dataDir,
		promURL:     promURL,
		disableCORS: disableCORS,
	}
}

func (s *SidecarAPI) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware, logMiddleware *logging.HTTPServerMiddleware) {
	s.baseAPI.Register(r, tracer, logger, ins, logMiddleware)

	instr := api.GetInstr(tracer, logger, ins, logMiddleware, s.disableCORS)
	r.Post("/flush", instr("flush", s.flush))
}

type flushResponse struct {
	BlocksUploaded int `json:"blocksUploaded"`
}

func (s *SidecarAPI) flush(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	dir, err := s.client.Snapshot(r.Context(), s.promURL, false)

	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: fmt.Errorf("failed to snapshot: %w", err)}, func() {}
	}

	snapshotDir := s.dataDir + "/" + dir

	s.shipper.SetDirectoryToSync(snapshotDir)
	uploaded, err := s.shipper.Sync(r.Context())
	if err := os.RemoveAll(snapshotDir); err != nil {
		s.logger.Log("failed to remove snapshot directory", err.Error())
	}
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: fmt.Errorf("failed to upload head block: %w", err)}, func() {}
	}
	return &flushResponse{BlocksUploaded: uploaded}, nil, nil, func() {}
}
