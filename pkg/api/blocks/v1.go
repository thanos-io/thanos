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
	"github.com/thanos-io/thanos/pkg/logging"
)

// BlocksAPI is a very simple API used by Thanos Block Viewer.
type BlocksAPI struct {
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

// NewBlocksAPI creates a simple API to be used by Thanos Block Viewer.
func NewBlocksAPI(logger log.Logger, label string, flagsMap map[string]string) *BlocksAPI {
	return &BlocksAPI{
		baseAPI: api.NewBaseAPI(logger, flagsMap),
		logger:  logger,
		blocksInfo: &BlocksInfo{
			Blocks: []metadata.Meta{},
			Label:  label,
		},
	}
}

func (bapi *BlocksAPI) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware, logMiddleware *logging.HTTPServerMiddleware) {
	bapi.baseAPI.Register(r, tracer, logger, ins, logMiddleware)

	instr := api.GetInstr(tracer, logger, ins, logMiddleware)

	r.Get("/blocks", instr("blocks", bapi.blocks))
}

func (bapi *BlocksAPI) blocks(r *http.Request) (interface{}, []error, *api.ApiError) {
	return bapi.blocksInfo, nil, nil
}

// Set updates the blocks' metadata in the API.
func (bapi *BlocksAPI) Set(blocks []metadata.Meta, err error) {
	if err != nil {
		// Last view is maintained.
		bapi.blocksInfo.RefreshedAt = time.Now()
		bapi.blocksInfo.Err = err
		return
	}

	bapi.blocksInfo.RefreshedAt = time.Now()
	bapi.blocksInfo.Blocks = blocks
	bapi.blocksInfo.Err = err
}
