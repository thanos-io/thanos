// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	qapi "github.com/thanos-io/thanos/pkg/query/api"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// API is a very simple API used by Thanos Compactor.
type API struct {
	logger     log.Logger
	blocksInfo *BlocksInfo
}

type BlocksInfo struct {
	Label       string          `json:"label"`
	Blocks      []metadata.Meta `json:"blocks"`
	RefreshedAt time.Time       `json:"refreshedAt"`
	Err         error           `json:"err"`
}

// NewAPI creates an Thanos Compactor API.
func NewAPI(logger log.Logger, label string) *API {
	return &API{
		logger: logger,
		blocksInfo: &BlocksInfo{
			Blocks: []metadata.Meta{},
			Label:  label,
		},
	}
}

func (api *API) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware) {
	instr := func(name string, f qapi.ApiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			qapi.SetCORS(w)
			if data, warnings, err := f(r); err != nil {
				qapi.RespondError(w, err, data)
			} else if data != nil {
				qapi.Respond(w, data, warnings)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		})
		return ins.NewHandler(name, tracing.HTTPMiddleware(tracer, name, logger, gziphandler.GzipHandler(hf)))
	}

	r.Get("/blocks", instr("blocks", api.blocks))
}

func (api *API) blocks(r *http.Request) (interface{}, []error, *qapi.ApiError) {
	return api.blocksInfo, nil, nil
}

// Set updates the blocks metadata in API
func (api *API) Set(blocks []metadata.Meta, err error) {
	if err != nil {
		// Last view is maintained.
		api.blocksInfo.RefreshedAt = time.Now()
		api.blocksInfo.Err = err
		return
	}

	api.blocksInfo.RefreshedAt = time.Now()
	api.blocksInfo.Blocks = blocks
	api.blocksInfo.Err = err
}
