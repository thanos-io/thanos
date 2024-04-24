// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"fmt"
	"net/http"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"

	"github.com/thanos-io/thanos/pkg/api"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
)

// SidecarAPI is a very simple API used by Thanos Sidecar.
type SidecarAPI struct {
	baseAPI     *api.BaseAPI
	logger      log.Logger
	reg         prometheus.Registerer
	disableCORS bool
}

// NewSidecarAPI creates an Thanos Sidecar API.
func NewSidecarAPI(
	logger log.Logger,
	reg prometheus.Registerer,
	disableCORS bool,
	flagsMap map[string]string,
) *SidecarAPI {
	return &SidecarAPI{
		baseAPI:     api.NewBaseAPI(logger, disableCORS, flagsMap),
		logger:      logger,
		reg:         reg,
		disableCORS: disableCORS,
	}
}

func (s *SidecarAPI) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware, logMiddleware *logging.HTTPServerMiddleware) {
	s.baseAPI.Register(r, tracer, logger, ins, logMiddleware)

	instr := api.GetInstr(tracer, logger, ins, logMiddleware, s.disableCORS)
	r.Post("/flush", instr("flush", s.flush))
}

func (s *SidecarAPI) flush(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	return fmt.Sprintf("successfully uploaded blocks"), nil, nil, func() {}
}
