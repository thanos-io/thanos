// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"regexp"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos/pkg/cache"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
)

type CachingBucketConfig struct {
	// Basic unit used to cache chunks.
	ChunkBlockSize int64 `yaml:"chunk_block_size"`

	// Maximum number of GetRange requests issued by this bucket for single GetRange call. Zero or negative value = unlimited.
	MaxChunksGetRangeRequests int `yaml:"max_chunks_get_range_requests"`

	// TTLs for various cache items.
	ChunkObjectSizeTTL time.Duration `yaml:"chunk_object_size_ttl"`
	ChunkBlockTTL      time.Duration `yaml:"chunk_block_ttl"`
}

func DefaultCachingBucketConfig() CachingBucketConfig {
	return CachingBucketConfig{
		ChunkBlockSize:            16000, // Equal to max chunk size.
		ChunkObjectSizeTTL:        24 * time.Hour,
		ChunkBlockTTL:             24 * time.Hour,
		MaxChunksGetRangeRequests: 3,
	}
}

// Bucket implementation that provides some caching features, using knowledge about how Thanos accesses data.
type CachingBucket struct {
	bucket objstore.Bucket
	cache  cache.Cache

	config CachingBucketConfig

	logger log.Logger

	requestedChunkBytes prometheus.Counter
	cachedChunkBytes    prometheus.Counter
	fetchedChunkBytes   prometheus.Counter
	refetchedChunkBytes prometheus.Counter

	objectSizeRequests prometheus.Counter
	objectSizeHits     prometheus.Counter
}

func NewCachingBucket(b objstore.Bucket, c cache.Cache, chunks CachingBucketConfig, logger log.Logger, reg prometheus.Registerer) (*CachingBucket, error) {
	if b == nil {
		return nil, errors.New("bucket is nil")
	}
	if c == nil {
		return nil, errors.New("cache is nil")
	}

	return &CachingBucket{
		config: chunks,
		bucket: b,
		cache:  c,
		logger: logger,

		requestedChunkBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_requested_chunk_bytes_total",
			Help: "Total number of requested bytes for chunk data ",
		}),
		cachedChunkBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_cached_chunk_bytes_total",
			Help: "Total number of chunk bytes used from cache",
		}),
		fetchedChunkBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_fetched_chunk_bytes_total",
			Help: "Total number of chunk bytes fetched from storage",
		}),
		refetchedChunkBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_refetched_chunk_bytes_total",
			Help: "Total number of chunk bytes re-fetched from storage, despite being in cache already.",
		}),
		objectSizeRequests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_objectsize_requests_total",
			Help: "Number of object size requests for objects",
		}),
		objectSizeHits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_objectsize_hits_total",
			Help: "Number of object size hits for objects",
		}),
	}, nil
}

func (cb *CachingBucket) Close() error {
	return cb.bucket.Close()
}

func (cb *CachingBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	return cb.bucket.Upload(ctx, name, r)
}

func (cb *CachingBucket) Delete(ctx context.Context, name string) error {
	return cb.bucket.Delete(ctx, name)
}

func (cb *CachingBucket) Name() string {
	return "caching: " + cb.bucket.Name()
}

func (cb *CachingBucket) WithExpectedErrs(expectedFunc objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	if ib, ok := cb.bucket.(objstore.InstrumentedBucket); ok {
		// Make a copy, but replace bucket with instrumented one.
		res := &CachingBucket{}
		*res = *cb
		res.bucket = ib.WithExpectedErrs(expectedFunc)
		return res
	}

	return cb
}

func (cb *CachingBucket) ReaderWithExpectedErrs(expectedFunc objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return cb.WithExpectedErrs(expectedFunc)
}

func (cb *CachingBucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	return cb.bucket.Iter(ctx, dir, f)
}

func (cb *CachingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return cb.bucket.Get(ctx, name)
}

var chunksMatcher = regexp.MustCompile(`^.*/chunks/\d+$`)

func isTSDBChunkFile(name string) bool {
	return chunksMatcher.MatchString(name)
}

func (cb *CachingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if isTSDBChunkFile(name) && off >= 0 && length > 0 {
		return cb.getRangeChunkFile(ctx, name, off, length)
	}

	return cb.bucket.GetRange(ctx, name, off, length)
}

func (cb *CachingBucket) Exists(ctx context.Context, name string) (bool, error) {
	return cb.bucket.Exists(ctx, name)
}

func (cb *CachingBucket) IsObjNotFoundErr(err error) bool {
	return cb.bucket.IsObjNotFoundErr(err)
}

func (cb *CachingBucket) ObjectSize(ctx context.Context, name string) (uint64, error) {
	return cb.bucket.ObjectSize(ctx, name)
}

func (cb *CachingBucket) cachedObjectSize(ctx context.Context, name string, ttl time.Duration) (uint64, error) {
	key := cachingKeyObjectSize(name)

	cb.objectSizeRequests.Add(1)

	hits := cb.cache.Fetch(ctx, []string{key})
	if s := hits[key]; len(s) == 8 {
		cb.objectSizeHits.Add(1)
		return binary.BigEndian.Uint64(s), nil
	}

	size, err := cb.bucket.ObjectSize(ctx, name)
	if err != nil {
		return 0, err
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], size)
	cb.cache.Store(ctx, map[string][]byte{key: buf[:]}, ttl)

	return size, nil
}

func (cb *CachingBucket) getRangeChunkFile(ctx context.Context, name string, offset, length int64) (io.ReadCloser, error) {
	cb.requestedChunkBytes.Add(float64(length))

	size, err := cb.cachedObjectSize(ctx, name, cb.config.ChunkObjectSizeTTL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get size of chunk file: %s", name)
	}

	// If length goes over object size, adjust length. We use it later to limit number of read bytes.
	if uint64(offset+length) > size {
		length = int64(size - uint64(offset))
	}

	// Start and end range are block-aligned offsets into object, that we're going to read.
	startRange := (offset / cb.config.ChunkBlockSize) * cb.config.ChunkBlockSize
	endRange := ((offset + length) / cb.config.ChunkBlockSize) * cb.config.ChunkBlockSize
	if (offset+length)%cb.config.ChunkBlockSize > 0 {
		endRange += cb.config.ChunkBlockSize
	}

	// The very last block in the object may have length that is not divisible by block size.
	lastBlockOffset := int64(-1)
	lastBlockLength := int(cb.config.ChunkBlockSize)
	if uint64(endRange) > size {
		lastBlockOffset = (int64(size) / cb.config.ChunkBlockSize) * cb.config.ChunkBlockSize
		lastBlockLength = int(int64(size) - lastBlockOffset)
	}

	numBlocks := (endRange - startRange) / cb.config.ChunkBlockSize

	offsetKeys := make(map[int64]string, numBlocks)
	keys := make([]string, 0, numBlocks)

	for off := startRange; off < endRange; off += cb.config.ChunkBlockSize {
		end := off + cb.config.ChunkBlockSize
		if end > int64(size) {
			end = int64(size)
		}

		k := cachingKeyObjectBlock(name, off, end)
		keys = append(keys, k)
		offsetKeys[off] = k
	}

	// Try to get all blocks from the cache.
	hits := cb.cache.Fetch(ctx, keys)
	for _, b := range hits {
		cb.cachedChunkBytes.Add(float64(len(b)))
	}

	if len(hits) < len(keys) {
		if hits == nil {
			hits = map[string][]byte{}
		}

		err := cb.fetchMissingChunkBlocks(ctx, name, startRange, endRange, offsetKeys, hits, lastBlockOffset, lastBlockLength)
		if err != nil {
			return nil, err
		}
	}

	return newBlocksReader(cb.config.ChunkBlockSize, offsetKeys, hits, offset, length), nil
}

type rng struct {
	start, end int64
}

// Fetches missing blocks and stores them into "hits" map.
func (cb *CachingBucket) fetchMissingChunkBlocks(ctx context.Context, name string, startRange, endRange int64, cacheKeys map[int64]string, hits map[string][]byte, lastBlockOffset int64, lastBlockLength int) error {
	// Ordered list of missing sub-ranges.
	var missing []rng

	for off := startRange; off < endRange; off += cb.config.ChunkBlockSize {
		if hits[cacheKeys[off]] == nil {
			missing = append(missing, rng{start: off, end: off + cb.config.ChunkBlockSize})
		}
	}

	missing = mergeRanges(missing, 0) // Merge adjacent ranges.
	// Keep merging until we have only max number of ranges (= requests).
	for limit := cb.config.ChunkBlockSize; cb.config.MaxChunksGetRangeRequests > 0 && len(missing) > cb.config.MaxChunksGetRangeRequests; limit = limit * 2 {
		missing = mergeRanges(missing, limit)
	}

	var hitsMutex sync.Mutex

	// Run parallel queries for each missing range. Fetched data is stored into 'hits' map, protected by hitsMutex.
	g, gctx := errgroup.WithContext(ctx)
	for _, m := range missing {
		m := m
		g.Go(func() error {
			r, err := cb.bucket.GetRange(gctx, name, m.start, m.end-m.start)
			if err != nil {
				return errors.Wrapf(err, "fetching range [%d, %d]", m.start, m.end)
			}
			defer runutil.CloseWithLogOnErr(cb.logger, r, "fetching range [%d, %d]", m.start, m.end)

			for off := m.start; off < m.end && gctx.Err() == nil; off += cb.config.ChunkBlockSize {
				key := cacheKeys[off]
				if key == "" {
					return errors.Errorf("fetching range [%d, %d]: caching key for offset %d not found", m.start, m.end, off)
				}

				// We need a new buffer for each block, both for storing into hits, and also for caching.
				var blockData []byte
				if off == lastBlockOffset {
					// The very last block in the object may have different length,
					// if object length isn't divisible by block size.
					blockData = make([]byte, lastBlockLength)
				} else {
					blockData = make([]byte, cb.config.ChunkBlockSize)
				}
				_, err := io.ReadFull(r, blockData)
				if err != nil {
					return errors.Wrapf(err, "fetching range [%d, %d]", m.start, m.end)
				}

				storeToCache := false
				hitsMutex.Lock()
				if _, ok := hits[key]; !ok {
					storeToCache = true
					hits[key] = blockData
				}
				hitsMutex.Unlock()

				if storeToCache {
					cb.fetchedChunkBytes.Add(float64(len(blockData)))
					cb.cache.Store(gctx, map[string][]byte{key: blockData}, cb.config.ChunkBlockTTL)
				} else {
					cb.refetchedChunkBytes.Add(float64(len(blockData)))
				}
			}

			return gctx.Err()
		})
	}

	return g.Wait()
}

// Merges ranges that are close to each other. Modifies input.
func mergeRanges(input []rng, limit int64) []rng {
	if len(input) == 0 {
		return input
	}

	last := 0
	for ix := 1; ix < len(input); ix++ {
		if (input[ix].start - input[last].end) <= limit {
			input[last].end = input[ix].end
		} else {
			last++
			input[last] = input[ix]
		}
	}
	return input[:last+1]
}

func cachingKeyObjectSize(name string) string {
	return fmt.Sprintf("size:%s", name)
}

func cachingKeyObjectBlock(name string, start int64, end int64) string {
	return fmt.Sprintf("block:%s:%d:%d", name, start, end)
}

// io.ReadCloser implementation that uses in-memory blocks.
type blocksReader struct {
	blockSize int64

	// Mapping of blockSize-aligned offsets to keys in hits.
	offsetsKeys map[int64]string
	blocks      map[string][]byte

	// Offset for next read, used to find correct block to return data from.
	readOffset int64

	// Remaining data to return from this reader. Once zero, this reader reports EOF.
	remaining int64
}

func newBlocksReader(blockSize int64, offsetsKeys map[int64]string, blocks map[string][]byte, readOffset, remaining int64) *blocksReader {
	return &blocksReader{
		blockSize:   blockSize,
		offsetsKeys: offsetsKeys,
		blocks:      blocks,

		readOffset: readOffset,
		remaining:  remaining,
	}
}

func (c *blocksReader) Close() error {
	return nil
}

func (c *blocksReader) Read(p []byte) (n int, err error) {
	if c.remaining <= 0 {
		return 0, io.EOF
	}

	currentBlockOffset := (c.readOffset / c.blockSize) * c.blockSize
	currentBlock, err := c.blockAt(currentBlockOffset)
	if err != nil {
		return 0, errors.Wrapf(err, "read position: %d", c.readOffset)
	}

	offsetInBlock := int(c.readOffset - currentBlockOffset)
	toCopy := len(currentBlock) - offsetInBlock
	if toCopy <= 0 {
		// This can only happen if block's length is not blockSize, and reader is told to read more data.
		return 0, errors.Errorf("no more data left in block at position %d, block length %d, reading position %d", currentBlockOffset, len(currentBlock), c.readOffset)
	}

	if len(p) < toCopy {
		toCopy = len(p)
	}
	if c.remaining < int64(toCopy) {
		toCopy = int(c.remaining) // Conversion is safe, c.remaining is small enough.
	}

	copy(p, currentBlock[offsetInBlock:offsetInBlock+toCopy])
	c.readOffset += int64(toCopy)
	c.remaining -= int64(toCopy)

	return toCopy, nil
}

func (c *blocksReader) blockAt(offset int64) ([]byte, error) {
	b := c.blocks[c.offsetsKeys[offset]]
	if b == nil {
		return nil, errors.Errorf("block for offset %d not found", offset)
	}
	return b, nil
}
