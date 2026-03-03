// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"encoding/binary"
	"fmt"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// rendezvousHashring distributes series across shards using rendezvous (highest random weight) hashing.
// For each series, it computes hash(seriesKey, shard) for every shard and picks the shard with the
// highest hash value. This avoids virtual nodes entirely while providing good distribution.
type rendezvousHashring struct {
	azEndpoints       [][]Endpoint // [azIndex][shardIndex] → Endpoint
	sortedAZs         []string
	numShards         int
	replicationFactor uint64
	flatEndpoints     []Endpoint
}

func newRendezvousHashring(endpoints []Endpoint, replicationFactor uint64) (*rendezvousHashring, error) {
	if replicationFactor == 0 {
		return nil, errors.New("replication factor cannot be zero")
	}

	groupedEndpoints, err := groupByAZ(endpoints)
	if err != nil {
		return nil, errors.Wrap(err, "failed to group endpoints by AZ")
	}

	numAZs := len(groupedEndpoints)
	if numAZs == 0 {
		return nil, errors.New("no endpoint groups found after grouping by AZ")
	}
	if uint64(numAZs) != replicationFactor {
		return nil, fmt.Errorf("number of AZs (%d) must equal replication factor (%d)", numAZs, replicationFactor)
	}

	numShards := len(groupedEndpoints[0])
	if numShards == 0 {
		return nil, errors.New("AZ groups are empty after grouping")
	}

	// Build sorted AZ names from groupedEndpoints (already sorted by groupByAZ).
	sortedAZs := make([]string, numAZs)
	for i := range groupedEndpoints {
		sortedAZs[i] = groupedEndpoints[i][0].AZ
	}

	// Build flat endpoint list: all AZ0 endpoints, then AZ1, etc.
	flatEndpoints := make([]Endpoint, 0, numAZs*numShards)
	for azIdx := 0; azIdx < numAZs; azIdx++ {
		flatEndpoints = append(flatEndpoints, groupedEndpoints[azIdx]...)
	}

	return &rendezvousHashring{
		azEndpoints:       groupedEndpoints,
		sortedAZs:         sortedAZs,
		numShards:         numShards,
		replicationFactor: replicationFactor,
		flatEndpoints:     flatEndpoints,
	}, nil
}

func (r *rendezvousHashring) GetN(tenant string, ts *prompb.TimeSeries, n uint64) (Endpoint, error) {
	if n >= r.replicationFactor {
		return Endpoint{}, &insufficientNodesError{have: r.replicationFactor, want: n + 1}
	}

	seriesKey := labelpb.HashWithPrefix(tenant, ts.Labels)

	// Rendezvous hashing: for each shard, compute hash(seriesKey, shard), pick the max.
	// buf is a fixed-size stack array: 8 bytes seriesKey + 2 bytes shard (little-endian).
	bestShard := 0
	bestHash := uint64(0)

	var buf [10]byte
	binary.LittleEndian.PutUint64(buf[:8], seriesKey)

	for shard := 0; shard < r.numShards; shard++ {
		binary.LittleEndian.PutUint16(buf[8:], uint16(shard))
		h := xxhash.Sum64(buf[:])
		if shard == 0 || h > bestHash {
			bestHash = h
			bestShard = shard
		}
	}

	// n selects the AZ (replica index).
	return r.azEndpoints[n][bestShard], nil
}

func (r *rendezvousHashring) Get(tenant string, ts *prompb.TimeSeries) (Endpoint, error) {
	return r.GetN(tenant, ts, 0)
}

func (r *rendezvousHashring) Nodes() []Endpoint {
	return r.flatEndpoints
}

func (r *rendezvousHashring) Close() {}
