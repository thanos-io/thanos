// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/thanos-io/thanos/pkg/compact"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/route"
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
	disableCORS      bool
	bkt              objstore.Bucket
	relabelConfig    []*relabel.Config
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

const (
	defaultConsistencyDelay     = time.Minute * 30
	defaultBlockSyncConcurrency = 20
	defaultDeleteDelay          = time.Hour * 48
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
		disableCORS: disableCORS,
		bkt:         bkt,
	}
}

func (bapi *BlocksAPI) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware, logMiddleware *logging.HTTPServerMiddleware) {
	bapi.baseAPI.Register(r, tracer, logger, ins, logMiddleware)

	instr := api.GetInstr(tracer, logger, ins, logMiddleware, bapi.disableCORS)

	r.Get("/blocks", instr("blocks", bapi.blocks))
	r.Post("/blocks/mark", instr("blocks_mark", bapi.markBlock))
	r.Post("/blocks/cleanup", instr("blocks_cleanup", bapi.cleanupBlocks))
}

func (bapi *BlocksAPI) SetRelabelConfig(r []*relabel.Config) {
	bapi.relabelConfig = r
}

func (bapi *BlocksAPI) markBlock(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
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
		return bapi.loadedBlocksInfo, nil, nil, func() {}
	}
	return bapi.globalBlocksInfo, nil, nil, func() {}
}

func (bapi *BlocksAPI) cleanupBlocks(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	consistencyDelay := defaultConsistencyDelay
	consistencyDelayStr := r.FormValue("consistencyDelay")
	if consistencyDelayStr != "" {
		var err error
		consistencyDelay, err = time.ParseDuration(consistencyDelayStr)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
		}
	}

	blockSyncConcurrency := defaultBlockSyncConcurrency
	blockSyncConcurrencyStr := r.FormValue("blockSyncConcurrency")
	if blockSyncConcurrencyStr != "" {
		var err error
		blockSyncConcurrency, err = strconv.Atoi(blockSyncConcurrencyStr)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
		}
	}

	deleteDelay := defaultDeleteDelay
	deleteDelayStr := r.FormValue("deleteDelay")
	if consistencyDelayStr != "" {
		var err error
		deleteDelay, err = time.ParseDuration(deleteDelayStr)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
		}
	}

	sync := false
	syncStr := r.FormValue("sync")
	if syncStr != "" {
		var err error
		sync, err = strconv.ParseBool(syncStr)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
		}
	}

	cleanup := func() error {
		stubCounter := promauto.With(nil).NewCounter(prometheus.CounterOpts{})

		bkt := bapi.bkt.(objstore.InstrumentedBucketReader)

		// While fetching blocks, we filter out blocks that were marked for deletion by using IgnoreDeletionMarkFilter.
		// The delay of deleteDelay/2 is added to ensure we fetch blocks that are meant to be deleted but do not have a replacement yet.
		// This is to make sure compactor will not accidentally perform compactions with gap instead.
		ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(bapi.logger, bkt, deleteDelay/2, blockSyncConcurrency)
		duplicateBlocksFilter := block.NewDeduplicateFilter(blockSyncConcurrency)
		blocksCleaner := compact.NewBlocksCleaner(bapi.logger, bapi.bkt, ignoreDeletionMarkFilter, deleteDelay, stubCounter, stubCounter)

		ctx := context.Background()

		var sy *compact.Syncer
		{
			baseMetaFetcher, err := block.NewBaseFetcher(bapi.logger, blockSyncConcurrency, bkt, "", nil)
			if err != nil {
				return errors.Wrap(err, "create meta fetcher")
			}
			cf := baseMetaFetcher.NewMetaFetcher(
				nil, []block.MetadataFilter{
					block.NewLabelShardedMetaFilter(bapi.relabelConfig),
					block.NewConsistencyDelayMetaFilter(bapi.logger, consistencyDelay, nil),
					ignoreDeletionMarkFilter,
					duplicateBlocksFilter,
				},
			)

			sy, err = compact.NewMetaSyncer(
				bapi.logger,
				nil,
				bapi.bkt,
				cf,
				duplicateBlocksFilter,
				ignoreDeletionMarkFilter,
				stubCounter,
				stubCounter,
			)
			if err != nil {
				return errors.Wrap(err, "create syncer")
			}
		}

		level.Info(bapi.logger).Log("msg", "syncing blocks metadata")
		if err := sy.SyncMetas(ctx); err != nil {
			return errors.Wrap(err, "sync blocks")
		}

		level.Info(bapi.logger).Log("msg", "synced blocks done")

		compact.BestEffortCleanAbortedPartialUploads(ctx, bapi.logger, sy.Partial(), bapi.bkt, stubCounter, stubCounter, stubCounter)
		if err := blocksCleaner.DeleteMarkedBlocks(ctx); err != nil {
			return errors.Wrap(err, "error cleaning blocks")
		}

		level.Info(bapi.logger).Log("msg", "cleanup done")

		return nil
	}

	if sync {
		if err := cleanup(); err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrap(err, "error cleaning blocks")}, func() {}
		}

		return "cleanup done", nil, nil, func() {}
	} else {
		go func() {
			if err := cleanup(); err != nil {
				level.Error(bapi.logger).Log("msg", errors.Wrap(err, "error cleaning blocks").Error())
			}
		}()

		return "execute cleanup", nil, nil, func() {}
	}
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
	bapi.globalBlocksInfo.set(blocks, err)
}

// SetLoaded updates the local blocks' metadata in the API.
func (bapi *BlocksAPI) SetLoaded(blocks []metadata.Meta, err error) {
	bapi.loadedBlocksInfo.set(blocks, err)
}
