package compact

import (
	"math/rand"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/runutil"
)

type ShardedCompactor struct {
	chunks    chunkenc.Pool
	logger    log.Logger
	compactor Compactor
	numShards uint64
}

func NewShardedCompactor(chunks chunkenc.Pool, logger log.Logger, compactor Compactor, numShards uint64) *ShardedCompactor {
	return &ShardedCompactor{chunks: chunks, logger: logger, compactor: compactor, numShards: numShards}
}

func (sc *ShardedCompactor) Compact(dest string, dirs []string, open []*tsdb.Block) ([]ulid.ULID, error) {
	compID, err := sc.compactor.Compact(dest, dirs, open)
	if err != nil {
		return nil, errors.Wrapf(err, "compact blocks")
	}
	if compID == (ulid.ULID{}) {
		return nil, nil
	}

	newBlock, err := tsdb.OpenBlock(sc.logger, filepath.Join(dest, compID.String()), sc.chunks)
	if err != nil {
		return nil, errors.Wrapf(err, "open compacted block")
	}

	indexr, err := newBlock.Index()
	if err != nil {
		return nil, errors.Wrapf(err, "get index reader")
	}
	defer runutil.CloseWithErrCapture(&err, indexr, "sharded compactor index reader")

	chunkr, err := newBlock.Chunks()
	if err != nil {
		return nil, errors.Wrap(err, "open chunk reader")
	}
	defer runutil.CloseWithErrCapture(&err, chunkr, "sharded compactor chunk reader")

	postings, err := indexr.Postings(index.AllPostingsKey())
	if err != nil {
		return nil, errors.Wrapf(err, "get postings")
	}
	var (
		chks []chunks.Meta
		lset labels.Labels

		compIDs = make([]ulid.ULID, sc.numShards)
		blocks  = make([]*downsample.StreamedBlockWriter, sc.numShards)
	)
	for i := range blocks {
		uid := ulid.MustNew(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano())))
		compIDs[i] = uid

		blockDir := filepath.Join(dest, uid.String())
		meta := metadata.Meta{BlockMeta: newBlock.Meta()}
		meta.ULID = uid

		writer, err := downsample.NewStreamedBlockWriter(blockDir, indexr, sc.logger, meta)
		if err != nil {
			return nil, err
		}
		defer runutil.CloseWithLogOnErr(sc.logger, writer, "sharded block writer")
		blocks[i] = writer
	}

	for postings.Next() {
		lset = lset[:0]
		chks = chks[:0]

		// Get series labels and chunks. Downsampled data is sensitive to chunk boundaries
		// and we need to preserve them to properly downsample previously downsampled data.
		if err := indexr.Series(postings.At(), &lset, &chks); err != nil {
			return nil, errors.Wrapf(err, "get series %d", postings.At())
		}

		for i, c := range chks {
			chk, err := chunkr.Chunk(c)
			if err != nil {
				return nil, errors.Wrapf(err, "get chunk %d, series %d", c.Ref, postings.At())
			}
			chks[i].Chunk = chk
		}

		shardID := lset.Hash() % sc.numShards
		if err := blocks[shardID].WriteSeries(lset, chks); err != nil {
			return nil, err
		}
	}

	return compIDs, nil
}
