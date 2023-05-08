// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

const CachedMatcherTTL = 5 * time.Minute

type NewItemFunc func(matcher LabelMatcher) (*labels.Matcher, error)

type itemExpiration struct {
	mu        sync.Mutex
	expiresAt time.Time
}

func (e *itemExpiration) setExpiration(t time.Time) {
	e.mu.Lock()
	e.expiresAt = t
	e.mu.Unlock()
}

func newExpiration(expiresAt time.Time) *itemExpiration {
	return &itemExpiration{
		expiresAt: expiresAt,
	}
}

type MatchersCache struct {
	mu       sync.RWMutex
	ttl      time.Duration
	now      func() time.Time
	itemTTLs map[LabelMatcher]*itemExpiration
	cache    map[LabelMatcher]*labels.Matcher
}

type MatcherCacheOption func(*MatchersCache)

func WithNowFunc(now func() time.Time) MatcherCacheOption {
	return func(c *MatchersCache) {
		c.now = now
	}
}

func NewMatchersCache(opts ...MatcherCacheOption) *MatchersCache {
	cache := &MatchersCache{
		now: time.Now,
		mu:  sync.RWMutex{},
		// This TTL should be sufficient to allow caching matchers for alerting queries.
		ttl:      CachedMatcherTTL,
		itemTTLs: make(map[LabelMatcher]*itemExpiration),
		cache:    make(map[LabelMatcher]*labels.Matcher),
	}

	for _, opt := range opts {
		opt(cache)
	}
	return cache
}

func (c *MatchersCache) GetOrSet(key LabelMatcher, newItem NewItemFunc) (*labels.Matcher, error) {
	expirationTime := c.now().Add(c.ttl)

	c.mu.RLock()
	if item, ok := c.cache[key]; ok {
		c.itemTTLs[key].setExpiration(expirationTime)
		c.mu.RUnlock()
		return item, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if item, ok := c.cache[key]; ok {
		c.itemTTLs[key].setExpiration(expirationTime)
		return item, nil
	}

	item, err := newItem(key)
	if err != nil {
		return nil, err
	}
	c.cache[key] = item
	c.itemTTLs[key] = newExpiration(expirationTime)
	return item, nil
}

func (c *MatchersCache) RemoveExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	for key, expiration := range c.itemTTLs {
		if expiration.expiresAt.Before(now) {
			delete(c.cache, key)
			delete(c.itemTTLs, key)
		}
	}
}
