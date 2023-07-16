// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objmeta

import (
	"context"
	"encoding/json"
	"io"
	"path"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/objmeta/objmetapb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Server meta server.
type Server struct {
	bucket                    objstore.InstrumentedBucket
	backend                   Backend
	logger                    log.Logger
	blockMetaFetchConcurrency int
}

// NewServer create a meta server.
func NewServer(
	logger log.Logger,
	reg *prometheus.Registry,
	objStoreConfContentYaml []byte,
	objMetaConfContentYaml []byte,
	blockMetaFetchConcurrency int,
) (*Server, error) {
	bucket, err := client.NewBucket(logger, objStoreConfContentYaml, reg, component.ObjMeta.String())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create bucket")
	}
	backend, err := NewBackend(logger, reg, objMetaConfContentYaml)
	if err != nil {
		return nil, err
	}
	return &Server{
		bucket:                    bucket,
		backend:                   backend,
		logger:                    log.With(logger, "component", "objmeta"),
		blockMetaFetchConcurrency: blockMetaFetchConcurrency,
	}, nil
}

func (m *Server) SetBlockMeta(ctx context.Context, req *objmetapb.SetBlockMetaRequest) (*objmetapb.SetBlockMetaResponse, error) {
	level.Info(m.logger).Log("msg", "SetBlockMeta", "blockID", req.BlockMeta.BlockId,
		"metaType", req.BlockMeta.Type)
	if err := m.backend.SetBlockMeta(ctx, req.BlockMeta); err != nil {
		return nil, err
	}
	return &objmetapb.SetBlockMetaResponse{}, nil
}

func (m *Server) DelBlockMeta(ctx context.Context, req *objmetapb.DelBlockMetaRequest) (*objmetapb.DelBlockMetaResponse, error) {
	level.Info(m.logger).Log("msg", "DelBlockMeta", "blockID", req.BlockId, "metaType", req.Type)
	exist, err := m.backend.DelBlockMeta(ctx, req.BlockId, req.Type)
	if err != nil {
		return nil, err
	}
	return &objmetapb.DelBlockMetaResponse{Exist: exist}, nil
}

func (m *Server) GetBlockMeta(ctx context.Context, req *objmetapb.GetBlockMetaRequest) (*objmetapb.GetBlockMetaResponse, error) {
	level.Debug(m.logger).Log("msg", "GetBlockMeta", "blockID", req.BlockId, "metaType", req.Type)
	blockMeta, err := m.backend.GetBlockMeta(ctx, req.BlockId, req.Type)
	if err != nil {
		return nil, err
	}
	return &objmetapb.GetBlockMetaResponse{
		BlockMeta: blockMeta,
	}, nil
}

func (m *Server) ExistsBlockMeta(ctx context.Context, req *objmetapb.ExistsBlockMetaRequest) (*objmetapb.ExistsBlockMetaResponse, error) {
	level.Debug(m.logger).Log("msg", "ExistsBlockMeta", "blockID", req.BlockId, "metaType", req.Type)
	ret, err := m.backend.ExistsBlockMeta(ctx, req.BlockId, req.Type)
	if err != nil {
		return nil, err
	}
	return &objmetapb.ExistsBlockMetaResponse{Exist: ret}, nil
}

func (m *Server) ListBlocks(_ *objmetapb.ListBlocksRequest, s objmetapb.ObjMeta_ListBlocksServer) error {
	level.Info(m.logger).Log("msg", "ListBlocks")
	return m.backend.ListBlocks(s.Context(), func(blocks []string) error {
		err := s.Send(&objmetapb.ListBlocksResponse{BlockId: blocks})
		if err == io.EOF {
			return io.EOF
		}
		if err != nil {
			return errors.Wrapf(err, "Send msg")
		}
		return nil
	})
}

// Sync sync meta from object store backend to object meta backend.
func (m *Server) Sync() error {
	logger := log.With(m.logger, "module", "objmeta syncer")
	ctx := context.Background()
	ok, releaseFunc, err := m.backend.AcquireSyncLock(ctx, time.Hour)
	if err != nil {
		return err
	}
	if !ok {
		level.Info(logger).Log("msg", "not acquire lock, skip sync")
		return nil
	}
	defer func() {
		if err := releaseFunc(); err != nil {
			level.Error(logger).Log("msg", "release lock error", "error", err)
		}
	}()
	start := time.Now()
	// 1. sync add
	var (
		g                  errgroup.Group
		ch                 = make(chan string, m.blockMetaFetchConcurrency)
		blocksFromObjStore = make(map[string]struct{})
	)
	level.Info(logger).Log("msg", "start sync", "concurrency", m.blockMetaFetchConcurrency)
	for i := 0; i < m.blockMetaFetchConcurrency; i++ {
		g.Go(func() error {
			for blockID := range ch {
				if err := m.syncAdd(ctx, blockID, logger); err != nil {
					return err
				}
			}
			return nil
		})
	}
	g.Go(func() error {
		defer close(ch)
		return m.bucket.Iter(ctx, "", func(name string) error {
			level.Debug(logger).Log("msg", "iter", "name", name)
			id, ok := block.IsBlockDir(name)
			if !ok {
				return nil
			}
			blockID := id.String()
			blocksFromObjStore[blockID] = struct{}{}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- blockID:
			}
			return nil
		})
	})
	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "iter bucket")
	}
	level.Info(logger).Log("msg", "sync add to objMeta",
		"blocksFromObjStore", len(blocksFromObjStore),
		"duration", time.Since(start).Truncate(time.Second))
	// 2. sync del
	err = m.backend.ListBlocks(ctx, func(blocks []string) error {
		for _, blockID := range blocks {
			if _, ok := blocksFromObjStore[blockID]; !ok {
				blockMeta, err := m.backend.GetBlockMeta(ctx, blockID, objmetapb.Type_TYPE_META)
				if err != nil {
					return err
				}
				if err = m.backend.DelBlockAllMeta(ctx, blockID); err != nil {
					return err
				}
				if blockMeta == nil {
					level.Info(logger).Log("msg", "sync del to Meta, but not exist TYPE_META",
						"blockID", blockID,
					)
					continue
				}
				blockMetaJSON, err := parseBlockMetaJSON(blockMeta.Data)
				if err != nil {
					return errors.Wrapf(err, "blockID:%s", blockID)
				}
				// is this block is upload recently? if yes, we can not delete it.
				blockCreateTime := ulid.Time(blockMetaJSON.ULID.Time())
				const safeToDel = time.Hour
				if start.Sub(blockCreateTime) > safeToDel {
					minTime := ulid.Time(uint64(blockMetaJSON.MinTime))
					maxTime := ulid.Time(uint64(blockMetaJSON.MaxTime))
					level.Info(logger).Log("msg", "sync del to Meta",
						"blockID", blockID,
						"blockCreateTime", blockCreateTime,
						"blockMinTime", minTime,
						"blockMaxTime", maxTime,
						"blockDuration", maxTime.Sub(minTime).String(),
						"externalLabels", blockMetaJSON.Thanos.Labels,
						"compactionLevel", blockMetaJSON.Compaction.Level,
						"numSeries", blockMetaJSON.Stats.NumSeries,
					)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *Server) syncAdd(ctx context.Context, blockID string, logger log.Logger) error {
	exist, err := m.backend.ExistsBlockMeta(ctx, blockID, objmetapb.Type_TYPE_META)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}
	for _, metaType := range objMetaTypes {
		rc, err := m.bucket.Get(ctx, path.Join(blockID, objMetaFiles[metaType]))
		switch {
		case m.bucket.IsObjNotFoundErr(err):
			if metaType == objmetapb.Type_TYPE_META {
				// block not exist, skip it
				return nil
			}
			// check next markers
			continue
		case err != nil:
			return errors.Wrapf(err, "bucket get error, blockID:%s, metaType:%s", blockID, metaType)
		}
		blockData, err := io.ReadAll(rc)
		if err != nil {
			return errors.Wrapf(err, "read object error, blockID:%s, metaType:%s", blockID, metaType)
		}
		if metaType == objmetapb.Type_TYPE_META {
			metaJSON, err := parseBlockMetaJSON(blockData)
			if err != nil {
				return errors.Wrapf(err, "blockID:%s, metaType:%s", blockID, metaType)
			}
			minTime := ulid.Time(uint64(metaJSON.MinTime))
			maxTime := ulid.Time(uint64(metaJSON.MaxTime))
			level.Info(logger).Log("msg", "sync add to Meta",
				"blockID", blockID,
				"blockCreateTime", ulid.Time(metaJSON.ULID.Time()),
				"blockMinTime", minTime,
				"blockMaxTime", maxTime,
				"blockDuration", maxTime.Sub(minTime).String(),
				"externalLabels", metaJSON.Thanos.Labels,
				"compactionLevel", metaJSON.Compaction.Level,
				"numSeries", metaJSON.Stats.NumSeries,
			)
		}
		err = m.backend.SetBlockMeta(ctx, &objmetapb.BlockMeta{
			BlockId: blockID,
			Type:    metaType,
			Source:  objmetapb.Source_SOURCE_SYNCER,
			Data:    blockData,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// RegisterObjMetaServer registers the meta server.
func RegisterObjMetaServer(metaSrv objmetapb.ObjMetaServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		objmetapb.RegisterObjMetaServer(s, metaSrv)
	}
}

func parseBlockMetaJSON(data []byte) (*metadata.Meta, error) {
	var meta metadata.Meta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, errors.Wrapf(err, "parseBlockMetaJSON data:%s", data)
	}
	return &meta, nil
}

var (
	objMetaTypes = []objmetapb.Type{
		objmetapb.Type_TYPE_META,
		objmetapb.Type_TYPE_DELETE_MARK,
		objmetapb.Type_TYPE_NO_COMPACT_MARK,
	}
	objMetaFiles = map[objmetapb.Type]string{
		objmetapb.Type_TYPE_META:            block.MetaFilename,
		objmetapb.Type_TYPE_DELETE_MARK:     metadata.DeletionMarkFilename,
		objmetapb.Type_TYPE_NO_COMPACT_MARK: metadata.NoCompactMarkFilename,
	}
)
