// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"time"

	"github.com/thanos-io/objstore"
)

// Codec for encoding and decoding results of Iter call.
type IterCodec interface {
	Encode(files []string) ([]byte, error)
	Decode(cachedData []byte) ([]string, error)
}

// CachingBucketConfig contains low-level configuration for individual bucket operations.
// This is not exposed to the user, but it is expected that code sets up individual
// operations based on user-provided configuration.
type CachingBucketConfig struct {
	get        map[string]*GetConfig
	iter       map[string]*IterConfig
	exists     map[string]*ExistsConfig
	getRange   map[string]*GetRangeConfig
	attributes map[string]*AttributesConfig
}

func NewCachingBucketConfig() *CachingBucketConfig {
	return &CachingBucketConfig{
		get:        map[string]*GetConfig{},
		iter:       map[string]*IterConfig{},
		exists:     map[string]*ExistsConfig{},
		getRange:   map[string]*GetRangeConfig{},
		attributes: map[string]*AttributesConfig{},
	}
}

// SetCacheImplementation sets the value of Cache for all configurations.
func (cfg *CachingBucketConfig) SetCacheImplementation(c Cache) {
	if cfg.get != nil {
		for k := range cfg.get {
			cfg.get[k].Cache = c
		}
	}
	if cfg.iter != nil {
		for k := range cfg.iter {
			cfg.iter[k].Cache = c
		}
	}
	if cfg.exists != nil {
		for k := range cfg.exists {
			cfg.exists[k].Cache = c
		}
	}
	if cfg.getRange != nil {
		for k := range cfg.getRange {
			cfg.getRange[k].Cache = c
		}
	}
	if cfg.attributes != nil {
		for k := range cfg.attributes {
			cfg.attributes[k].Cache = c
		}
	}
}

// Generic config for single operation.
type OperationConfig struct {
	Matcher func(name string) bool
	Cache   Cache
}

// Operation-specific configs.
type IterConfig struct {
	OperationConfig
	TTL   time.Duration
	Codec IterCodec
}

type ExistsConfig struct {
	OperationConfig
	ExistsTTL      time.Duration
	DoesntExistTTL time.Duration
}

type GetConfig struct {
	ExistsConfig
	ContentTTL       time.Duration
	MaxCacheableSize int
}

type GetRangeConfig struct {
	OperationConfig
	SubrangeSize   int64
	MaxSubRequests int
	AttributesTTL  time.Duration
	SubrangeTTL    time.Duration
}

type AttributesConfig struct {
	OperationConfig
	TTL time.Duration
}

func newOperationConfig(cache Cache, matcher func(string) bool) OperationConfig {
	if matcher == nil {
		panic("matcher")
	}

	return OperationConfig{
		Matcher: matcher,
		Cache:   cache,
	}
}

// CacheIter configures caching of "Iter" operation for matching directories.
func (cfg *CachingBucketConfig) CacheIter(configName string, cache Cache, matcher func(string) bool, ttl time.Duration, codec IterCodec) {
	cfg.iter[configName] = &IterConfig{
		OperationConfig: newOperationConfig(cache, matcher),
		TTL:             ttl,
		Codec:           codec,
	}
}

// CacheGet configures caching of "Get" operation for matching files. Content of the object is cached, as well as whether object exists or not.
func (cfg *CachingBucketConfig) CacheGet(configName string, cache Cache, matcher func(string) bool, maxCacheableSize int, contentTTL, existsTTL, doesntExistTTL time.Duration) {
	cfg.get[configName] = &GetConfig{
		ExistsConfig: ExistsConfig{
			OperationConfig: newOperationConfig(cache, matcher),
			ExistsTTL:       existsTTL,
			DoesntExistTTL:  doesntExistTTL,
		},
		ContentTTL:       contentTTL,
		MaxCacheableSize: maxCacheableSize,
	}
}

// CacheExists configures caching of "Exists" operation for matching files. Negative values are cached as well.
func (cfg *CachingBucketConfig) CacheExists(configName string, cache Cache, matcher func(string) bool, existsTTL, doesntExistTTL time.Duration) {
	cfg.exists[configName] = &ExistsConfig{
		OperationConfig: newOperationConfig(cache, matcher),
		ExistsTTL:       existsTTL,
		DoesntExistTTL:  doesntExistTTL,
	}
}

// CacheGetRange configures caching of "GetRange" operation. Subranges (aligned on subrange size) are cached individually.
// Since caching operation needs to know the object size to compute correct subranges, object size is cached as well.
// Single "GetRange" requests can result in multiple smaller GetRange sub-requests issued on the underlying bucket.
// MaxSubRequests specifies how many such subrequests may be issued. Values <= 0 mean there is no limit (requests
// for adjacent missing subranges are still merged).
func (cfg *CachingBucketConfig) CacheGetRange(configName string, cache Cache, matcher func(string) bool, subrangeSize int64, attributesTTL, subrangeTTL time.Duration, maxSubRequests int) {
	cfg.getRange[configName] = &GetRangeConfig{
		OperationConfig: newOperationConfig(cache, matcher),
		SubrangeSize:    subrangeSize,
		AttributesTTL:   attributesTTL,
		SubrangeTTL:     subrangeTTL,
		MaxSubRequests:  maxSubRequests,
	}
}

// CacheAttributes configures caching of "Attributes" operation for matching files.
func (cfg *CachingBucketConfig) CacheAttributes(configName string, cache Cache, matcher func(name string) bool, ttl time.Duration) {
	cfg.attributes[configName] = &AttributesConfig{
		OperationConfig: newOperationConfig(cache, matcher),
		TTL:             ttl,
	}
}

func (cfg *CachingBucketConfig) AllConfigNames() map[string][]string {
	result := map[string][]string{}
	for n := range cfg.get {
		result[objstore.OpGet] = append(result[objstore.OpGet], n)
	}
	for n := range cfg.iter {
		result[objstore.OpIter] = append(result[objstore.OpIter], n)
	}
	for n := range cfg.exists {
		result[objstore.OpExists] = append(result[objstore.OpExists], n)
	}
	for n := range cfg.getRange {
		result[objstore.OpGetRange] = append(result[objstore.OpGetRange], n)
	}
	for n := range cfg.attributes {
		result[objstore.OpAttributes] = append(result[objstore.OpAttributes], n)
	}
	return result
}

func (cfg *CachingBucketConfig) FindIterConfig(dir string) (string, *IterConfig) {
	for n, cfg := range cfg.iter {
		if cfg.Matcher(dir) {
			return n, cfg
		}
	}
	return "", nil
}

func (cfg *CachingBucketConfig) FindExistConfig(name string) (string, *ExistsConfig) {
	for n, cfg := range cfg.exists {
		if cfg.Matcher(name) {
			return n, cfg
		}
	}
	return "", nil
}

func (cfg *CachingBucketConfig) FindGetConfig(name string) (string, *GetConfig) {
	for n, cfg := range cfg.get {
		if cfg.Matcher(name) {
			return n, cfg
		}
	}
	return "", nil
}

func (cfg *CachingBucketConfig) FindGetRangeConfig(name string) (string, *GetRangeConfig) {
	for n, cfg := range cfg.getRange {
		if cfg.Matcher(name) {
			return n, cfg
		}
	}
	return "", nil
}

func (cfg *CachingBucketConfig) FindAttributesConfig(name string) (string, *AttributesConfig) {
	for n, cfg := range cfg.attributes {
		if cfg.Matcher(name) {
			return n, cfg
		}
	}
	return "", nil
}
