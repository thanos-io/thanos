// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
)

// CompactAPI is a very simple API used by Thanos Compactor.
type CompactAPI struct {
	baseAPI    *api.BaseAPI
	logger     log.Logger
	blocksInfo *BlocksInfo
}

type BlocksInfo struct {
	Label       string          `json:"label"`
	Blocks      []metadata.Meta `json:"blocks"`
	RefreshedAt time.Time       `json:"refreshedAt"`
	Err         error           `json:"err"`
}

// NewAPI creates a simple API to be used by Thanos Compactor.
func NewCompactAPI(logger log.Logger, label string, flagsMap map[string]string) *CompactAPI {
	return &CompactAPI{
		baseAPI: api.NewBaseAPI(logger, flagsMap),
		logger:  logger,
		blocksInfo: &BlocksInfo{
			Blocks: []metadata.Meta{},
			Label:  label,
		},
	}
}

func (capi *CompactAPI) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware) {
	capi.baseAPI.Register(r, tracer, logger, ins)

	instr := api.GetInstr(tracer, logger, ins)

	r.Get("/blocks", instr("blocks", capi.blocks))
}

func (capi *CompactAPI) blocks(r *http.Request) (interface{}, []error, *api.ApiError) {
	return capi.blocksInfo, nil, nil
}

// Set updates the blocks metadata in API
func (capi *CompactAPI) Set(blocks []metadata.Meta, err error) {
	if err != nil {
		// Last view is maintained.
		capi.blocksInfo.RefreshedAt = time.Now()
		capi.blocksInfo.Err = err
		return
	}

	capi.blocksInfo.RefreshedAt = time.Now()
	capi.blocksInfo.Blocks = blocks
	capi.blocksInfo.Err = err
}
