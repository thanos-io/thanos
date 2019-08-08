package block

import (
	"context"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type MetadataFetcher struct {
	cachedBlockMDs map[ulid.ULID]*metadata.Meta
	mu             *sync.RWMutex

	concurrencyLimit int
	logger           log.Logger
	bkt              objstore.BucketReader
}

func NewMetadataFetcher(logger log.Logger, concurrencyLimit int, bkt objstore.BucketReader) *MetadataFetcher {
	return &MetadataFetcher{
		logger:           logger,
		concurrencyLimit: concurrencyLimit,
		mu:               &sync.RWMutex{},
		cachedBlockMDs:   make(map[ulid.ULID]*metadata.Meta),
		bkt:              bkt,
	}
}

// Fetch wil return the mapping from block ID to its metadata that are currently stored in the bucket passed in.
// If the corresponding value for a block ID key is nil, no valid metadata is retrieved successfully.
func (f *MetadataFetcher) Fetch(ctx context.Context) (map[ulid.ULID]*metadata.Meta, error) {
	blockIDs := make(chan ulid.ULID, f.concurrencyLimit)
	defer close(blockIDs)
	remoteBlockMDs := make(map[ulid.ULID]*metadata.Meta, len(f.cachedBlockMDs))
	wg := &sync.WaitGroup{}

	for i := 0; i < f.concurrencyLimit; i++ {
		go func() {
			for blockID := range blockIDs {
				md, err := f.fetchBlockMDIfNotCached(ctx, wg, blockID)
				if err != nil {
					level.Debug(f.logger).Log("msg", "failed to fetch block metadata", "block", blockID, "error", err)
				}

				f.mu.Lock()
				remoteBlockMDs[blockID] = md
				f.mu.Unlock()
			}
		}()
	}

	if err := f.bkt.Iter(ctx, "", func(name string) error {
		wg.Add(1)

		id, isBlock := IsBlockDir(name)
		if !isBlock {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case blockIDs <- id:
		}

		return nil
	}); err != nil {
		return nil, err
	}
	wg.Wait()

	f.mu.Lock()
	f.cachedBlockMDs = make(map[ulid.ULID]*metadata.Meta, len(remoteBlockMDs))
	for k, v := range remoteBlockMDs {
		f.cachedBlockMDs[k] = v
	}
	f.mu.Unlock()

	return remoteBlockMDs, nil
}

func (f *MetadataFetcher) fetchBlockMDIfNotCached(ctx context.Context, wg *sync.WaitGroup, blockID ulid.ULID) (*metadata.Meta, error) {
	defer wg.Done()

	f.mu.RLock()
	existingMD, exist := f.cachedBlockMDs[blockID]
	f.mu.RUnlock()

	if exist {
		return existingMD, nil
	}

	md, err := DownloadMeta(ctx, f.logger, f.bkt, blockID)
	if err != nil {
		return nil, err
	}

	return &md, nil
}
