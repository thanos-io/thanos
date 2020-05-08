// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/snappy"
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
	config = "config"

	originCache  = "cache"
	originBucket = "bucket"

	existsTrue  = "true"
	existsFalse = "false"
)

var errObjNotFound = errors.Errorf("object not found")

type CachingBucketConfig struct {
	// Basic unit used to cache chunks.
	SubrangeSize int64 `yaml:"chunk_subrange_size"`

	// Maximum number of GetRange requests issued by this bucket for single GetRange call. Zero or negative value = unlimited.
	MaxGetRangeRequests int           `yaml:"max_chunks_get_range_requests"`
	SubrangeTTL         time.Duration `yaml:"subrange_ttl"`

	// TTLs for various cache items.
	ObjectSizeTTL time.Duration `yaml:"object_size_ttl"`

	// How long to cache result of Iter call.
	IterTTL time.Duration `yaml:"iter_ttl"`

	// Config for Exists and Get opertions.
	ExistsTTL      time.Duration `yaml:"exists_ttl"`
	DoesntExistTTL time.Duration `yaml:"doesnt_exist_ttl"`
	ContentTTL     time.Duration `yaml:"content_ttl"`
}

func DefaultCachingBucketConfig() CachingBucketConfig {
	return CachingBucketConfig{
		SubrangeSize:        16000, // Equal to max chunk size.
		ObjectSizeTTL:       24 * time.Hour,
		SubrangeTTL:         24 * time.Hour,
		MaxGetRangeRequests: 3,

		IterTTL: 5 * time.Minute,

		ExistsTTL:      10 * time.Minute,
		DoesntExistTTL: 3 * time.Minute,
		ContentTTL:     1 * time.Hour,
	}
}

// Bucket implementation that provides some caching features, using knowledge about how Thanos accesses data.
type CachingBucket struct {
	objstore.Bucket

	logger log.Logger

	getRangeConfigs        map[string]*cacheConfig
	requestedGetRangeBytes *prometheus.CounterVec
	fetchedGetRangeBytes   *prometheus.CounterVec
	refetchedGetRangeBytes *prometheus.CounterVec

	objectSizeRequests *prometheus.CounterVec
	objectSizeHits     *prometheus.CounterVec

	iterConfigs   map[string]*cacheConfig
	iterRequests  *prometheus.CounterVec
	iterCacheHits *prometheus.CounterVec

	existsConfigs   map[string]*cacheConfig
	existsRequests  *prometheus.CounterVec
	existsCacheHits *prometheus.CounterVec

	getConfigs   map[string]*cacheConfig
	getRequests  *prometheus.CounterVec
	getCacheHits *prometheus.CounterVec
}

type cacheConfig struct {
	CachingBucketConfig

	matcher func(name string) bool
	cache   cache.Cache
}

// NewCachingBucket creates caching bucket with no configuration. Various "Cache*" methods configure
// this bucket to cache results of individual bucket methods. Configuration must be set before
// caching bucket is used by other objects.
func NewCachingBucket(b objstore.Bucket, logger log.Logger, reg prometheus.Registerer) (*CachingBucket, error) {
	if b == nil {
		return nil, errors.New("bucket is nil")
	}

	cb := &CachingBucket{
		Bucket:          b,
		logger:          logger,
		getConfigs:      map[string]*cacheConfig{},
		getRangeConfigs: map[string]*cacheConfig{},
		existsConfigs:   map[string]*cacheConfig{},
		iterConfigs:     map[string]*cacheConfig{},

		requestedGetRangeBytes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_getrange_requested_bytes_total",
			Help: "Total number of bytes requested via GetRange.",
		}, []string{config}),
		fetchedGetRangeBytes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_getrange_fetched_bytes_total",
			Help: "Total number of bytes fetched because of GetRange operation. Data from bucket is then stored to cache.",
		}, []string{"origin", config}),
		refetchedGetRangeBytes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_refetched_chunk_bytes_total",
			Help: "Total number of bytes re-fetched from storage because of GetRange operation, despite being in cache already.",
		}, []string{"origin", config}),

		objectSizeRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_objectsize_requests_total",
			Help: "Number of object size requests for objects.",
		}, []string{config}),
		objectSizeHits: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_objectsize_hits_total",
			Help: "Number of object size hits for objects.",
		}, []string{config}),

		iterRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_iter_requests_total",
			Help: "Number of Iter requests for directories.",
		}, []string{config}),
		iterCacheHits: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_iter_cache_hits_total",
			Help: "Number of Iter requests served from cache.",
		}, []string{config}),

		existsRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_exists_requests_total",
			Help: "Number of Exists requests.",
		}, []string{config}),
		existsCacheHits: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_exists_cache_hits_total",
			Help: "Number of Exists requests served from cache.",
		}, []string{config}),

		getRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_get_requests_total",
			Help: "Number of Get requests.",
		}, []string{config}),
		getCacheHits: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_get_hits_total",
			Help: "Number of Get requests served from cache with data.",
		}, []string{config}),
	}

	return cb, nil
}

func newCacheConfig(cfg CachingBucketConfig, matcher func(string) bool, cache cache.Cache) *cacheConfig {
	if matcher == nil {
		panic("matcher")
	}
	if cache == nil {
		panic("cache")
	}

	return &cacheConfig{
		CachingBucketConfig: cfg,
		matcher:             matcher,
		cache:               cache,
	}
}

func (cb *CachingBucket) CacheIter(labelName string, cache cache.Cache, matcher func(string) bool, cfg CachingBucketConfig) {
	cb.iterConfigs[labelName] = newCacheConfig(cfg, matcher, cache)
	cb.iterRequests.WithLabelValues(labelName)
	cb.iterCacheHits.WithLabelValues(labelName)
}

func (cb *CachingBucket) CacheExists(labelName string, cache cache.Cache, matcher func(string) bool, cfg CachingBucketConfig) {
	cb.existsConfigs[labelName] = newCacheConfig(cfg, matcher, cache)
	cb.existsRequests.WithLabelValues(labelName)
	cb.existsCacheHits.WithLabelValues(labelName)
}

func (cb *CachingBucket) CacheGet(labelName string, cache cache.Cache, matcher func(string) bool, cfg CachingBucketConfig) {
	cb.getConfigs[labelName] = newCacheConfig(cfg, matcher, cache)
	cb.getRequests.WithLabelValues(labelName)
	cb.getCacheHits.WithLabelValues(labelName)
}

func (cb *CachingBucket) CacheGetRange(labelName string, cache cache.Cache, matcher func(string) bool, cfg CachingBucketConfig) {
	cb.getRangeConfigs[labelName] = newCacheConfig(cfg, matcher, cache)
	cb.requestedGetRangeBytes.WithLabelValues(labelName)
	cb.fetchedGetRangeBytes.WithLabelValues(originCache, labelName)
	cb.fetchedGetRangeBytes.WithLabelValues(originBucket, labelName)
	cb.refetchedGetRangeBytes.WithLabelValues(originCache, labelName)
}

func (cb *CachingBucket) findCacheConfig(configs map[string]*cacheConfig, objectName string) (string, *cacheConfig) {
	for n, cfg := range configs {
		if cfg.matcher(objectName) {
			return n, cfg
		}
	}
	return "", nil
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

func (cb *CachingBucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	lname, cfg := cb.findCacheConfig(cb.iterConfigs, dir)
	if cfg == nil {
		return cb.Bucket.Iter(ctx, dir, f)
	}

	cb.iterRequests.WithLabelValues(lname).Inc()

	key := cachingKeyIter(dir)

	data := cfg.cache.Fetch(ctx, []string{key})
	if data[key] != nil {
		list, err := decodeIterResult(data[key])
		if err == nil {
			cb.iterCacheHits.WithLabelValues(lname).Inc()

			for _, n := range list {
				err = f(n)
				if err != nil {
					return err
				}
			}
			return nil
		} else {
			// This should not happen.
			level.Warn(cb.logger).Log("msg", "failed to decode cached Iter result", "err", err)
		}
	}

	// Iteration can take a while (esp. since it calls function), and iterTTL is generally low.
	// We will compute TTL based time when iteration started.
	iterTime := time.Now()
	var list []string
	err := cb.Bucket.Iter(ctx, dir, func(s string) error {
		list = append(list, s)
		return f(s)
	})

	remainingTTL := cfg.IterTTL - time.Since(iterTime)
	if err == nil && remainingTTL > 0 {
		data := encodeIterResult(list)
		if data != nil {
			cfg.cache.Store(ctx, map[string][]byte{key: data}, remainingTTL)
		}
	}
	return err
}

// Iter results should compress nicely, especially in subdirectories.
func encodeIterResult(files []string) []byte {
	data, err := json.Marshal(files)
	if err != nil {
		return nil
	}

	return snappy.Encode(nil, data)
}

func decodeIterResult(data []byte) ([]string, error) {
	decoded, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, err
	}

	var list []string
	err = json.Unmarshal(decoded, &list)
	return list, err
}

func (cb *CachingBucket) Exists(ctx context.Context, name string) (bool, error) {
	lname, cfg := cb.findCacheConfig(cb.existsConfigs, name)
	if cfg == nil {
		return cb.Bucket.Exists(ctx, name)
	}

	cb.existsRequests.WithLabelValues(lname).Inc()

	key := cachingKeyExists(name)
	hits := cfg.cache.Fetch(ctx, []string{key})

	if ex := hits[key]; ex != nil {
		switch string(ex) {
		case existsTrue:
			cb.existsCacheHits.WithLabelValues(lname).Inc()
			return true, nil
		case existsFalse:
			cb.existsCacheHits.WithLabelValues(lname).Inc()
			return false, nil
		default:
			level.Warn(cb.logger).Log("msg", "unexpected cached 'exists' value", "val", string(ex))
		}
	}

	existsTime := time.Now()
	ok, err := cb.Bucket.Exists(ctx, name)
	if err == nil {
		cb.storeExistsCacheEntry(ctx, key, ok, existsTime, cfg)
	}

	return ok, err
}

func (cb *CachingBucket) storeExistsCacheEntry(ctx context.Context, cachingKey string, exists bool, ts time.Time, cfg *cacheConfig) {
	var (
		data []byte
		ttl  time.Duration
	)
	if exists {
		ttl = cfg.ExistsTTL - time.Since(ts)
		data = []byte(existsTrue)
	} else {
		ttl = cfg.DoesntExistTTL - time.Since(ts)
		data = []byte(existsFalse)
	}

	if ttl > 0 {
		cfg.cache.Store(ctx, map[string][]byte{cachingKey: data}, ttl)
	}
}

func (cb *CachingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	lname, cfg := cb.findCacheConfig(cb.getConfigs, name)
	if cfg == nil {
		return cb.Bucket.Get(ctx, name)
	}

	cb.getRequests.WithLabelValues(lname).Inc()

	key := cachingKeyContent(name)
	existsKey := cachingKeyExists(name)

	hits := cfg.cache.Fetch(ctx, []string{key, existsKey})
	if hits[key] != nil {
		cb.getCacheHits.WithLabelValues(lname).Inc()
		return ioutil.NopCloser(bytes.NewReader(hits[key])), nil
	}

	// If we know that file doesn't exist, we can return that. Useful for deletion marks.
	if ex := hits[existsKey]; ex != nil && string(ex) == existsFalse {
		cb.getCacheHits.WithLabelValues(lname).Inc()
		return nil, errObjNotFound
	}

	getTime := time.Now()
	reader, err := cb.Bucket.Get(ctx, name)
	if err != nil {
		if cb.Bucket.IsObjNotFoundErr(err) {
			// Cache that object doesn't exist.
			cb.storeExistsCacheEntry(ctx, existsKey, false, getTime, cfg)
		}

		return nil, err
	}
	defer runutil.CloseWithLogOnErr(cb.logger, reader, "CachingBucket.Get(%q)", name)

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	ttl := cfg.ContentTTL - time.Since(getTime)
	if ttl > 0 {
		cfg.cache.Store(ctx, map[string][]byte{key: data}, ttl)
	}
	cb.storeExistsCacheEntry(ctx, existsKey, true, getTime, cfg)

	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

func (cb *CachingBucket) IsObjNotFoundErr(err error) bool {
	return err == errObjNotFound || cb.Bucket.IsObjNotFoundErr(err)
}

func (cb *CachingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	lname, cfg := cb.findCacheConfig(cb.getRangeConfigs, name)
	if off >= 0 && length > 0 && cfg != nil {
		var (
			r   io.ReadCloser
			err error
		)
		tracing.DoInSpan(ctx, "cachingbucket_getrange", func(ctx context.Context) {
			r, err = cb.cachedGetRange(ctx, name, off, length, lname, cfg)
		})
		return r, err
	}

	return cb.Bucket.GetRange(ctx, name, off, length)
}

func (cb *CachingBucket) cachedObjectSize(ctx context.Context, name string, labelName string, cache cache.Cache, ttl time.Duration) (uint64, error) {
	key := cachingKeyObjectSize(name)

	cb.objectSizeRequests.WithLabelValues(labelName).Add(1)

	hits := cache.Fetch(ctx, []string{key})
	if s := hits[key]; len(s) == 8 {
		cb.objectSizeHits.WithLabelValues(labelName).Add(1)
		return binary.BigEndian.Uint64(s), nil
	}

	size, err := cb.Bucket.ObjectSize(ctx, name)
	if err != nil {
		return 0, err
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], size)
	cache.Store(ctx, map[string][]byte{key: buf[:]}, ttl)

	return size, nil
}

func (cb *CachingBucket) cachedGetRange(ctx context.Context, name string, offset, length int64, lname string, cfg *cacheConfig) (io.ReadCloser, error) {
	cb.requestedGetRangeBytes.WithLabelValues(lname).Add(float64(length))

	size, err := cb.cachedObjectSize(ctx, name, lname, cfg.cache, cfg.ObjectSizeTTL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get size of chunk file: %s", name)
	}

	// If length goes over object size, adjust length. We use it later to limit number of read bytes.
	if uint64(offset+length) > size {
		length = int64(size - uint64(offset))
	}

	// Start and end range are subrange-aligned offsets into object, that we're going to read.
	startRange := (offset / cfg.SubrangeSize) * cfg.SubrangeSize
	endRange := ((offset + length) / cfg.SubrangeSize) * cfg.SubrangeSize
	if (offset+length)%cfg.SubrangeSize > 0 {
		endRange += cfg.SubrangeSize
	}

	// The very last subrange in the object may have length that is not divisible by subrange size.
	lastSubrangeOffset := endRange - cfg.SubrangeSize
	lastSubrangeLength := int(cfg.SubrangeSize)
	if uint64(endRange) > size {
		lastSubrangeOffset = (int64(size) / cfg.SubrangeSize) * cfg.SubrangeSize
		lastSubrangeLength = int(int64(size) - lastSubrangeOffset)
	}

	numSubranges := (endRange - startRange) / cfg.SubrangeSize

	offsetKeys := make(map[int64]string, numSubranges)
	keys := make([]string, 0, numSubranges)

	for off := startRange; off < endRange; off += cfg.SubrangeSize {
		end := off + cfg.SubrangeSize
		if end > int64(size) {
			end = int64(size)
		}

		k := cachingKeyObjectSubrange(name, off, end)
		keys = append(keys, k)
		offsetKeys[off] = k
	}

	// Try to get all subranges from the cache.
	hits := cfg.cache.Fetch(ctx, keys)
	for _, b := range hits {
		cb.fetchedGetRangeBytes.WithLabelValues(originCache, lname).Add(float64(len(b)))
	}

	if len(hits) < len(keys) {
		if hits == nil {
			hits = map[string][]byte{}
		}

		err := cb.fetchMissingChunkSubranges(ctx, name, startRange, endRange, offsetKeys, hits, lastSubrangeOffset, lastSubrangeLength, lname, cfg)
		if err != nil {
			return nil, err
		}
	}

	return ioutil.NopCloser(newSubrangesReader(cfg.SubrangeSize, offsetKeys, hits, offset, length)), nil
}

type rng struct {
	start, end int64
}

// fetchMissingChunkSubranges fetches missing subranges, stores them into "hits" map
// and into cache as well (using provided cacheKeys).
func (cb *CachingBucket) fetchMissingChunkSubranges(ctx context.Context, name string, startRange, endRange int64, cacheKeys map[int64]string, hits map[string][]byte, lastSubrangeOffset int64, lastSubrangeLength int, labelName string, cfg *cacheConfig) error {
	// Ordered list of missing sub-ranges.
	var missing []rng

	for off := startRange; off < endRange; off += cfg.SubrangeSize {
		if hits[cacheKeys[off]] == nil {
			missing = append(missing, rng{start: off, end: off + cfg.SubrangeSize})
		}
	}

	missing = mergeRanges(missing, 0) // Merge adjacent ranges.
	// Keep merging until we have only max number of ranges (= requests).
	for limit := cfg.SubrangeSize; cfg.MaxGetRangeRequests > 0 && len(missing) > cfg.MaxGetRangeRequests; limit = limit * 2 {
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

			for off := m.start; off < m.end && gctx.Err() == nil; off += cfg.SubrangeSize {
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
					subrangeData = make([]byte, cfg.SubrangeSize)
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
					cb.fetchedGetRangeBytes.WithLabelValues(originBucket, labelName).Add(float64(len(subrangeData)))
					cfg.cache.Store(gctx, map[string][]byte{key: subrangeData}, cfg.SubrangeTTL)
				} else {
					cb.refetchedGetRangeBytes.WithLabelValues(originCache, labelName).Add(float64(len(subrangeData)))
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

func cachingKeyIter(name string) string {
	return fmt.Sprintf("iter:%s", name)
}

func cachingKeyExists(name string) string {
	return fmt.Sprintf("exists:%s", name)
}

func cachingKeyContent(name string) string {
	return fmt.Sprintf("content:%s", name)
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
