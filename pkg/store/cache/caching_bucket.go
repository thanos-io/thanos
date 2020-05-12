// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
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
	"github.com/thanos-io/thanos/pkg/tracing"
)

const (
	originCache  = "cache"
	originBucket = "bucket"
)

type CachingBucketConfig struct {
	// Basic unit used to cache chunks.
	ChunkSubrangeSize int64 `yaml:"chunk_subrange_size"`

	// Maximum number of GetRange requests issued by this bucket for single GetRange call. Zero or negative value = unlimited.
	MaxChunksGetRangeRequests int `yaml:"max_chunks_get_range_requests"`

	// TTLs for various cache items.
	ChunkObjectSizeTTL time.Duration `yaml:"chunk_object_size_ttl"`
	ChunkSubrangeTTL   time.Duration `yaml:"chunk_subrange_ttl"`
}

func DefaultCachingBucketConfig() CachingBucketConfig {
	return CachingBucketConfig{
		ChunkSubrangeSize:         16000, // Equal to max chunk size.
		ChunkObjectSizeTTL:        24 * time.Hour,
		ChunkSubrangeTTL:          24 * time.Hour,
		MaxChunksGetRangeRequests: 3,
	}
}

// Bucket implementation that provides some caching features, using knowledge about how Thanos accesses data.
type CachingBucket struct {
	objstore.Bucket

	cache cache.Cache

	config CachingBucketConfig

	logger log.Logger

	requestedChunkBytes prometheus.Counter
	fetchedChunkBytes   *prometheus.CounterVec
	refetchedChunkBytes *prometheus.CounterVec

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

	cb := &CachingBucket{
		Bucket: b,
		config: chunks,
		cache:  c,
		logger: logger,

		requestedChunkBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_requested_chunk_bytes_total",
			Help: "Total number of requested bytes for chunk data.",
		}),
		fetchedChunkBytes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_fetched_chunk_bytes_total",
			Help: "Total number of fetched chunk bytes. Data from bucket is then stored to cache.",
		}, []string{"origin"}),
		refetchedChunkBytes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_refetched_chunk_bytes_total",
			Help: "Total number of chunk bytes re-fetched from storage, despite being in cache already.",
		}, []string{"origin"}),
		objectSizeRequests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_objectsize_requests_total",
			Help: "Number of object size requests for objects.",
		}),
		objectSizeHits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_objectsize_hits_total",
			Help: "Number of object size hits for objects.",
		}),
	}

	cb.fetchedChunkBytes.WithLabelValues(originBucket)
	cb.fetchedChunkBytes.WithLabelValues(originCache)
	cb.refetchedChunkBytes.WithLabelValues(originCache)

	return cb, nil
}

func (cb *CachingBucket) Name() string {
	return "caching: " + cb.Bucket.Name()
}

func (cb *CachingBucket) WithExpectedErrs(expectedFunc objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	if ib, ok := cb.Bucket.(objstore.InstrumentedBucket); ok {
		// Make a copy, but replace bucket with instrumented one.
		res := &CachingBucket{}
		*res = *cb
		res.Bucket = ib.WithExpectedErrs(expectedFunc)
		return res
	}

	return cb
}

func (cb *CachingBucket) ReaderWithExpectedErrs(expectedFunc objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return cb.WithExpectedErrs(expectedFunc)
}

var chunksMatcher = regexp.MustCompile(`^.*/chunks/\d+$`)

func isTSDBChunkFile(name string) bool {
	return chunksMatcher.MatchString(name)
}

func (cb *CachingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if isTSDBChunkFile(name) && off >= 0 && length > 0 {
		var (
			r   io.ReadCloser
			err error
		)
		tracing.DoInSpan(ctx, "cachingbucket_getrange_chunkfile", func(ctx context.Context) {
			r, err = cb.getRangeChunkFile(ctx, name, off, length)
		})
		return r, err
	}

	return cb.Bucket.GetRange(ctx, name, off, length)
}

func (cb *CachingBucket) cachedObjectSize(ctx context.Context, name string, ttl time.Duration) (uint64, error) {
	key := cachingKeyObjectSize(name)

	cb.objectSizeRequests.Add(1)

	hits := cb.cache.Fetch(ctx, []string{key})
	if s := hits[key]; len(s) == 8 {
		cb.objectSizeHits.Add(1)
		return binary.BigEndian.Uint64(s), nil
	}

	size, err := cb.Bucket.ObjectSize(ctx, name)
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

	// Start and end range are subrange-aligned offsets into object, that we're going to read.
	startRange := (offset / cb.config.ChunkSubrangeSize) * cb.config.ChunkSubrangeSize
	endRange := ((offset + length) / cb.config.ChunkSubrangeSize) * cb.config.ChunkSubrangeSize
	if (offset+length)%cb.config.ChunkSubrangeSize > 0 {
		endRange += cb.config.ChunkSubrangeSize
	}

	// The very last subrange in the object may have length that is not divisible by subrange size.
	lastSubrangeOffset := endRange - cb.config.ChunkSubrangeSize
	lastSubrangeLength := int(cb.config.ChunkSubrangeSize)
	if uint64(endRange) > size {
		lastSubrangeOffset = (int64(size) / cb.config.ChunkSubrangeSize) * cb.config.ChunkSubrangeSize
		lastSubrangeLength = int(int64(size) - lastSubrangeOffset)
	}

	numSubranges := (endRange - startRange) / cb.config.ChunkSubrangeSize

	offsetKeys := make(map[int64]string, numSubranges)
	keys := make([]string, 0, numSubranges)

	for off := startRange; off < endRange; off += cb.config.ChunkSubrangeSize {
		end := off + cb.config.ChunkSubrangeSize
		if end > int64(size) {
			end = int64(size)
		}

		k := cachingKeyObjectSubrange(name, off, end)
		keys = append(keys, k)
		offsetKeys[off] = k
	}

	// Try to get all subranges from the cache.
	hits := cb.cache.Fetch(ctx, keys)
	for _, b := range hits {
		cb.fetchedChunkBytes.WithLabelValues(originCache).Add(float64(len(b)))
	}

	if len(hits) < len(keys) {
		if hits == nil {
			hits = map[string][]byte{}
		}

		err := cb.fetchMissingChunkSubranges(ctx, name, startRange, endRange, offsetKeys, hits, lastSubrangeOffset, lastSubrangeLength)
		if err != nil {
			return nil, err
		}
	}

	return ioutil.NopCloser(newSubrangesReader(cb.config.ChunkSubrangeSize, offsetKeys, hits, offset, length)), nil
}

type rng struct {
	start, end int64
}

// fetchMissingChunkSubranges fetches missing subranges, stores them into "hits" map
// and into cache as well (using provided cacheKeys).
func (cb *CachingBucket) fetchMissingChunkSubranges(ctx context.Context, name string, startRange, endRange int64, cacheKeys map[int64]string, hits map[string][]byte, lastSubrangeOffset int64, lastSubrangeLength int) error {
	// Ordered list of missing sub-ranges.
	var missing []rng

	for off := startRange; off < endRange; off += cb.config.ChunkSubrangeSize {
		if hits[cacheKeys[off]] == nil {
			missing = append(missing, rng{start: off, end: off + cb.config.ChunkSubrangeSize})
		}
	}

	missing = mergeRanges(missing, 0) // Merge adjacent ranges.
	// Keep merging until we have only max number of ranges (= requests).
	for limit := cb.config.ChunkSubrangeSize; cb.config.MaxChunksGetRangeRequests > 0 && len(missing) > cb.config.MaxChunksGetRangeRequests; limit = limit * 2 {
		missing = mergeRanges(missing, limit)
	}

	var hitsMutex sync.Mutex

	// Run parallel queries for each missing range. Fetched data is stored into 'hits' map, protected by hitsMutex.
	g, gctx := errgroup.WithContext(ctx)
	for _, m := range missing {
		m := m
		g.Go(func() error {
			r, err := cb.Bucket.GetRange(gctx, name, m.start, m.end-m.start)
			if err != nil {
				return errors.Wrapf(err, "fetching range [%d, %d]", m.start, m.end)
			}
			defer runutil.CloseWithLogOnErr(cb.logger, r, "fetching range [%d, %d]", m.start, m.end)

			for off := m.start; off < m.end && gctx.Err() == nil; off += cb.config.ChunkSubrangeSize {
				key := cacheKeys[off]
				if key == "" {
					return errors.Errorf("fetching range [%d, %d]: caching key for offset %d not found", m.start, m.end, off)
				}

				// We need a new buffer for each subrange, both for storing into hits, and also for caching.
				var subrangeData []byte
				if off == lastSubrangeOffset {
					// The very last subrange in the object may have different length,
					// if object length isn't divisible by subrange size.
					subrangeData = make([]byte, lastSubrangeLength)
				} else {
					subrangeData = make([]byte, cb.config.ChunkSubrangeSize)
				}
				_, err := io.ReadFull(r, subrangeData)
				if err != nil {
					return errors.Wrapf(err, "fetching range [%d, %d]", m.start, m.end)
				}

				storeToCache := false
				hitsMutex.Lock()
				if _, ok := hits[key]; !ok {
					storeToCache = true
					hits[key] = subrangeData
				}
				hitsMutex.Unlock()

				if storeToCache {
					cb.fetchedChunkBytes.WithLabelValues(originBucket).Add(float64(len(subrangeData)))
					cb.cache.Store(gctx, map[string][]byte{key: subrangeData}, cb.config.ChunkSubrangeTTL)
				} else {
					cb.refetchedChunkBytes.WithLabelValues(originCache).Add(float64(len(subrangeData)))
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

func cachingKeyObjectSubrange(name string, start int64, end int64) string {
	return fmt.Sprintf("subrange:%s:%d:%d", name, start, end)
}

// Reader implementation that uses in-memory subranges.
type subrangesReader struct {
	subrangeSize int64

	// Mapping of subrangeSize-aligned offsets to keys in hits.
	offsetsKeys map[int64]string
	subranges   map[string][]byte

	// Offset for next read, used to find correct subrange to return data from.
	readOffset int64

	// Remaining data to return from this reader. Once zero, this reader reports EOF.
	remaining int64
}

func newSubrangesReader(subrangeSize int64, offsetsKeys map[int64]string, subranges map[string][]byte, readOffset, remaining int64) *subrangesReader {
	return &subrangesReader{
		subrangeSize: subrangeSize,
		offsetsKeys:  offsetsKeys,
		subranges:    subranges,

		readOffset: readOffset,
		remaining:  remaining,
	}
}

func (c *subrangesReader) Read(p []byte) (n int, err error) {
	if c.remaining <= 0 {
		return 0, io.EOF
	}

	currentSubrangeOffset := (c.readOffset / c.subrangeSize) * c.subrangeSize
	currentSubrange, err := c.subrangeAt(currentSubrangeOffset)
	if err != nil {
		return 0, errors.Wrapf(err, "read position: %d", c.readOffset)
	}

	offsetInSubrange := int(c.readOffset - currentSubrangeOffset)
	toCopy := len(currentSubrange) - offsetInSubrange
	if toCopy <= 0 {
		// This can only happen if subrange's length is not subrangeSize, and reader is told to read more data.
		return 0, errors.Errorf("no more data left in subrange at position %d, subrange length %d, reading position %d", currentSubrangeOffset, len(currentSubrange), c.readOffset)
	}

	if len(p) < toCopy {
		toCopy = len(p)
	}
	if c.remaining < int64(toCopy) {
		toCopy = int(c.remaining) // Conversion is safe, c.remaining is small enough.
	}

	copy(p, currentSubrange[offsetInSubrange:offsetInSubrange+toCopy])
	c.readOffset += int64(toCopy)
	c.remaining -= int64(toCopy)

	return toCopy, nil
}

func (c *subrangesReader) subrangeAt(offset int64) ([]byte, error) {
	b := c.subranges[c.offsetsKeys[offset]]
	if b == nil {
		return nil, errors.Errorf("subrange for offset %d not found", offset)
	}
	return b, nil
}
