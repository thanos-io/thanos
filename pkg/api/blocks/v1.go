// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
)

// BlocksAPI is a very simple API used by Thanos Block Viewer.
type BlocksAPI struct {
	baseAPI          *api.BaseAPI
	logger           log.Logger
	globalBlocksInfo *BlocksInfo
	loadedBlocksInfo *BlocksInfo
	plannedBlocksInfo *BlocksInfo

	globalLock, loadedLock, plannedLock sync.Mutex// Question: whether is plannedLock needed?
	disableCORS            bool
	bkt                    objstore.Bucket
	disableAdminOperations bool
}

type BlocksInfo struct {
	Label       string          `json:"label"`
	Blocks      []metadata.Meta `json:"blocks"`
	RefreshedAt time.Time       `json:"refreshedAt"`
	Err         error           `json:"err"`
}

type ActionType int32

const (
	Deletion ActionType = iota
	NoCompaction
	Unknown
)

func parse(s string) ActionType {
	switch s {
	case "DELETION":
		return Deletion
	case "NO_COMPACTION":
		return NoCompaction
	default:
		return Unknown
	}
}

// NewBlocksAPI creates a simple API to be used by Thanos Block Viewer.
func NewBlocksAPI(logger log.Logger, disableCORS bool, label string, flagsMap map[string]string, bkt objstore.Bucket) *BlocksAPI {
	disableAdminOperations := flagsMap["disable-admin-operations"] == "true"
	return &BlocksAPI{
		baseAPI: api.NewBaseAPI(logger, disableCORS, flagsMap),
		logger:  logger,
		globalBlocksInfo: &BlocksInfo{
			Blocks: []metadata.Meta{},
			Label:  label,
		},
		loadedBlocksInfo: &BlocksInfo{
			Blocks: []metadata.Meta{},
			Label:  label,
		},
		disableCORS:            disableCORS,
		bkt:                    bkt,
		disableAdminOperations: disableAdminOperations,
	}
}

func (bapi *BlocksAPI) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware, logMiddleware *logging.HTTPServerMiddleware) {
	bapi.baseAPI.Register(r, tracer, logger, ins, logMiddleware)

	instr := api.GetInstr(tracer, logger, ins, logMiddleware, bapi.disableCORS)

	r.Get("/blocks", instr("blocks", bapi.blocks))
	r.Post("/blocks/mark", instr("blocks_mark", bapi.markBlock))
	r.Get("/blocks/plan", instr("blocks_plan", bapi.plannedBlocks))
}

func (bapi *BlocksAPI) markBlock(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	if bapi.disableAdminOperations {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.New("Admin operations are disabled")}, func() {}
	}
	idParam := r.FormValue("id")
	actionParam := r.FormValue("action")
	detailParam := r.FormValue("detail")

	if idParam == "" {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.New("ID cannot be empty")}, func() {}
	}

	if actionParam == "" {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.New("Action cannot be empty")}, func() {}
	}

	id, err := ulid.Parse(idParam)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Errorf("ULID %q is not valid: %v", idParam, err)}, func() {}
	}

	actionType := parse(actionParam)
	switch actionType {
	case Deletion:
		err := block.MarkForDeletion(r.Context(), bapi.logger, bapi.bkt, id, detailParam, promauto.With(nil).NewCounter(prometheus.CounterOpts{}))
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
		}
	case NoCompaction:
		err := block.MarkForNoCompact(r.Context(), bapi.logger, bapi.bkt, id, metadata.ManualNoCompactReason, detailParam, promauto.With(nil).NewCounter(prometheus.CounterOpts{}))
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
		}
	default:
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Errorf("not supported marker %v", actionParam)}, func() {}
	}
	return nil, nil, nil, func() {}
}

func (bapi *BlocksAPI) blocks(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	viewParam := r.URL.Query().Get("view")
	if viewParam == "loaded" {
		bapi.loadedLock.Lock()
		defer bapi.loadedLock.Unlock()

		return bapi.loadedBlocksInfo, nil, nil, func() {}
	}

	bapi.globalLock.Lock()
	defer bapi.globalLock.Unlock()

	return bapi.globalBlocksInfo, nil, nil, func() {}
}

func (bapi *BlocksAPI) plannedBlocks(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	// TODO: fetch from planner.plan then mock data
	// Mock data
	mockBlocks := []metadata.Meta{
		{
			BlockMeta: tsdb.BlockMeta{
				ULID:    ulid.MustNew(ulid.Now(), nil),
				MinTime: time.Now().Add(-1*time.Hour).Unix() * 1000,
				MaxTime: time.Now().Unix() * 1000,
				Stats: tsdb.BlockStats{
					NumSamples: 1000,
					NumSeries:  100,
				},
			},
			Thanos: metadata.Thanos{},
		},
		{
			BlockMeta: tsdb.BlockMeta{
				ULID:    ulid.MustNew(ulid.Now(), nil),
				MinTime: time.Now().Add(-2*time.Hour).Unix() * 1000,
				MaxTime: time.Now().Add(-1*time.Hour).Unix() * 1000,
				Stats: tsdb.BlockStats{
					NumSamples: 2000,
					NumSeries:  200,
				},
			},
			Thanos: metadata.Thanos{},
		},
	}

	return &BlocksInfo{
		Blocks:      mockBlocks,
		RefreshedAt: time.Now(),
		Label:       "Planned Blocks",
	}, nil, nil, func() {}
}

func (b *BlocksInfo) set(blocks []metadata.Meta, err error) {
	if err != nil {
		// Last view is maintained.
		b.RefreshedAt = time.Now()
		b.Err = err
		return
	}

	b.RefreshedAt = time.Now()
	b.Blocks = blocks
	b.Err = err
}

// SetGlobal updates the global blocks' metadata in the API.
func (bapi *BlocksAPI) SetGlobal(blocks []metadata.Meta, err error) {
	bapi.globalLock.Lock()
	defer bapi.globalLock.Unlock()

	bapi.globalBlocksInfo.set(blocks, err)
}

// SetLoaded updates the local blocks' metadata in the API.
func (bapi *BlocksAPI) SetLoaded(blocks []metadata.Meta, err error) {
	bapi.loadedLock.Lock()
	defer bapi.loadedLock.Unlock()

	bapi.loadedBlocksInfo.set(blocks, err)
}
// TODO: call setPlanned when needed
func (bapi *BlocksAPI) SetPlanned(blocks []metadata.Meta, err error) {

	bapi.plannedBlocksInfo.set(blocks, err)
}
