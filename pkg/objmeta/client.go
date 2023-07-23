package objmeta

import (
	"bytes"
	"context"
	"io"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/objmeta/objmetapb"
	"google.golang.org/grpc"
)

// NewClient create a meta client.
func NewClient(
	logger log.Logger,
	reg prometheus.Registerer,
	metaServerEndpoint string,
) (objmetapb.ObjMetaClient, error) {
	if metaServerEndpoint == "" {
		return nil, nil
	}
	// TODO support grpc ssl.
	dialOpts, err := extgrpc.StoreClientGRPCOpts(logger, nil, nil, false, false, "", "", "", "")
	if err != nil {
		return nil, errors.Wrap(err, "building gRPC client")
	}
	cc, err := grpc.Dial(metaServerEndpoint, dialOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "grpc Dial")
	}
	return objmetapb.NewObjMetaClient(cc), nil
}

// BucketWithObjMetaClient wrapped the client.
type BucketWithObjMetaClient struct {
	client objmetapb.ObjMetaClient
	objstore.InstrumentedBucket
	logger log.Logger
}

// NewBucketWithObjMetaClient create a InstrumentedBucket with meta client
func NewBucketWithObjMetaClient(
	client objmetapb.ObjMetaClient,
	instrumentedBucket objstore.InstrumentedBucket,
	logger log.Logger,
) objstore.InstrumentedBucket {
	if client == nil {
		return instrumentedBucket
	}
	return &BucketWithObjMetaClient{
		client:             client,
		InstrumentedBucket: instrumentedBucket,
		logger:             log.With(logger, "module", "objmeta"),
	}
}

// Upload upload meta from objmeta backend and objstore if name is meta, otherwise access to objstore.
func (b *BucketWithObjMetaClient) Upload(ctx context.Context, name string, r io.Reader) error {
	level.Info(b.logger).Log("msg", "upload", "name", name)
	if isMeta, metaType, blockID := isBlockMeta(name); isMeta {
		fileData, err := io.ReadAll(r)
		if err != nil {
			return errors.Wrap(err, "read meta.json")
		}
		_, err = b.client.SetBlockMeta(ctx, &objmetapb.SetBlockMetaRequest{
			BlockMeta: &objmetapb.BlockMeta{
				BlockId: blockID,
				Type:    metaType,
				Source:  objmetapb.Source_SOURCE_CLIENT,
				Data:    fileData,
			},
		})
		if err != nil {
			return errors.Wrap(err, "SetBlockMeta")
		}
		r = bytes.NewReader(fileData)
	}
	return b.InstrumentedBucket.Upload(ctx, name, r)
}

// Delete delete meta from objmeta backend and objstore if name is meta, otherwise access to objstore.
func (b *BucketWithObjMetaClient) Delete(ctx context.Context, name string) error {
	level.Info(b.logger).Log("msg", "Delete", "name", name)
	if isMeta, metaType, blockID := isBlockMeta(name); isMeta {
		_, err := b.client.DelBlockMeta(ctx, &objmetapb.DelBlockMetaRequest{
			BlockId: blockID,
			Type:    metaType,
		})
		if err != nil {
			return errors.Wrap(err, "DelBlockMeta")
		}
	}
	return b.InstrumentedBucket.Delete(ctx, name)
}

// Iter iter meta to objmeta backend if dir is empty, otherwise access to objstore.
func (b *BucketWithObjMetaClient) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	level.Info(b.logger).Log("msg", "Iter", "dir", dir)
	if dir == "" && !objstore.ApplyIterOptions(options...).Recursive {
		// iter block list
		c, err := b.client.ListBlocks(ctx, &objmetapb.ListBlocksRequest{})
		if err != nil {
			level.Warn(b.logger).Log("msg", "ListBlocks error", "dir", dir, "error", err)
			return b.InstrumentedBucket.Iter(ctx, dir, f, options...)
		}
		for {
			rsp, err := c.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return errors.Wrap(err, "ListBlocks Recv")
			}
			for _, v := range rsp.BlockId {
				if err := f(v + "/"); err != nil {
					return err
				}
			}
		}
	}
	return b.InstrumentedBucket.Iter(ctx, dir, f, options...)
}

// Get get meta from objmeta backend if name is meta, otherwise access to objstore.
func (b *BucketWithObjMetaClient) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	level.Debug(b.logger).Log("msg", "Get", "name", name)
	if isMeta, metaType, blockID := isBlockMeta(name); isMeta {
		rsp, err := b.client.GetBlockMeta(ctx, &objmetapb.GetBlockMetaRequest{
			BlockId: blockID,
			Type:    metaType,
		})
		if err != nil {
			return nil, errors.Wrap(err, "GetBlockMeta")
		}
		if rsp.BlockMeta == nil {
			return nil, errObjNotFound
		}
		return io.NopCloser(bytes.NewReader(rsp.BlockMeta.Data)), nil
	}
	return b.InstrumentedBucket.Get(ctx, name)
}

// Exists judge meta from objmeta backend if name is meta, otherwise access to objstore.
func (b *BucketWithObjMetaClient) Exists(ctx context.Context, name string) (bool, error) {
	if isMeta, metaType, blockID := isBlockMeta(name); isMeta {
		rsp, err := b.client.ExistsBlockMeta(ctx, &objmetapb.ExistsBlockMetaRequest{
			BlockId: blockID,
			Type:    metaType,
		})
		if err != nil {
			return false, errors.Wrap(err, "ExistsBlockMeta")
		}
		return rsp.Exist, nil
	}
	return b.InstrumentedBucket.Exists(ctx, name)
}

var (
	errObjNotFound = errors.Errorf("object not found")
)

// IsObjNotFoundErr return true if err is not found error.
func (b *BucketWithObjMetaClient) IsObjNotFoundErr(err error) bool {
	return err == errObjNotFound || b.InstrumentedBucket.IsObjNotFoundErr(err)
}

// isBlockMeta return true if object name is meta file type.
func isBlockMeta(objectName string) (bool, objmetapb.Type, string) {
	s := strings.Split(objectName, "/")
	if len(s) < 2 {
		return false, 0, ""
	}
	switch s[len(s)-1] {
	case block.MetaFilename:
		return true, objmetapb.Type_TYPE_META, s[0]
	case metadata.DeletionMarkFilename:
		return true, objmetapb.Type_TYPE_DELETE_MARK, s[0]
	case metadata.NoCompactMarkFilename:
		return true, objmetapb.Type_TYPE_NO_COMPACT_MARK, s[0]
	default:
		return false, 0, ""
	}
}
