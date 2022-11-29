// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"math"
	"strings"
)

type adjusterFn func(uint64) uint64

func maxStringsPerStore(storeCount uint64) uint64 {
	return math.MaxUint64 / uint64(storeCount)
}

func newReferenceAdjusterFactory(storeCount uint64) func(storeIndex uint64) adjusterFn {
	// Adjuster adjusts each incoming reference according to the number of stores.
	// Whole label space is stored in uint64 so that's how many
	// strings we are able to store.
	eachStore := maxStringsPerStore(storeCount)

	return func(storeIndex uint64) adjusterFn {
		startFrom := eachStore * storeIndex

		mappings := map[uint64]uint64{}
		currentMappingInd := startFrom

		return func(ref uint64) uint64 {
			if i, ok := mappings[ref]; ok {
				return i
			}
			mappings[ref] = currentMappingInd
			currentMappingInd++
			return currentMappingInd - 1
		}
	}
}

// symbolTableBuilder provides a way of building
// a symbol table for static strings to compress
// responses better. gRPC compression works on a
// message-by-message basis but we want to compress
// static strings as well.
// It's not safe to use this concurrently.
type symbolTableBuilder struct {
	maxElements uint64

	current      uint64
	table        map[uint64]string
	reverseTable map[string]uint64
}

func newSymbolTableBuilder(maxElements uint64) *symbolTableBuilder {
	return &symbolTableBuilder{maxElements: maxElements, table: make(map[uint64]string), reverseTable: make(map[string]uint64)}
}

func (b *symbolTableBuilder) getOrStoreString(s string) (uint64, bool) {
	if num, ok := b.reverseTable[s]; ok {
		return num, true
	}
	if b.current >= b.maxElements {
		return 0, false
	}

	s = strings.Clone(s)
	b.reverseTable[s] = b.current
	b.table[b.current] = s
	b.current++
	return b.current - 1, true
}

func (b *symbolTableBuilder) getTable() map[uint64]string {
	return b.table
}
