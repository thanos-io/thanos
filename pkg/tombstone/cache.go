// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tombstone

import (
	"sync"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

// MemTombstonesCache is a cache for the MemTombstones associated with each global tombstone file.
type MemTombstonesCache struct {
	mtx sync.RWMutex
	// tombstone cache: global tombstone ID -> MemTombstones.
	tombstones map[ulid.ULID]*tombstones.MemTombstones
}

// NewMemTombstoneCache initializes the MemTombstonesCache.
func NewMemTombstoneCache() *MemTombstonesCache {
	return &MemTombstonesCache{tombstones: make(map[ulid.ULID]*tombstones.MemTombstones)}
}

// Get gets the MemTombstones from cache.
func (m *MemTombstonesCache) Get(id ulid.ULID) (*tombstones.MemTombstones, bool) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	t, exists := m.tombstones[id]
	return t, exists
}

// GetTombstoneIDs returns a list of tombstone IDs.
func (m *MemTombstonesCache) GetTombstoneIDs() []ulid.ULID {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	res := make([]ulid.ULID, 0, len(m.tombstones))
	for id := range m.tombstones {
		res = append(res, id)
	}
	return res
}

// Set sets tombstone by ID.
func (m *MemTombstonesCache) Set(id ulid.ULID, tombstone *tombstones.MemTombstones) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.tombstones[id] = tombstone
}

// Delete deletes the tombstone by ID.
func (m *MemTombstonesCache) Delete(id ulid.ULID) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	delete(m.tombstones, id)
}

// GetIntervalsByRef returns a list of merged tombstone intervals by given series ref.
func (m *MemTombstonesCache) GetIntervalsByRef(ref storage.SeriesRef) tombstones.Intervals {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	var intervals tombstones.Intervals
	for _, tombstone := range m.tombstones {
		// MemTombstone always return nil error.
		ivs, _ := tombstone.Get(ref)
		for _, iv := range ivs {
			intervals = intervals.Add(iv)
		}
	}
	return intervals
}

// MergeTombstones merges multiple in-memory tombstones into one in-memory tombstone.
func (m *MemTombstonesCache) MergeTombstones() *tombstones.MemTombstones {
	stones := tombstones.NewMemTombstones()
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	if len(m.tombstones) == 0 {
		return stones
	}
	if len(m.tombstones) == 1 {
		for _, t := range m.tombstones {
			return t
		}
	}
	for _, ts := range m.tombstones {
		ts.Iter(func(id storage.SeriesRef, ivs tombstones.Intervals) error {
			for _, iv := range ivs {
				stones.AddInterval(id, iv)
			}
			return nil
		})
	}
	return stones
}
