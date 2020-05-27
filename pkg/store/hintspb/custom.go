// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package hintspb

import "github.com/oklog/ulid"

func (m *SeriesRequestHints) ShouldQueryBlock(id ulid.ULID) bool {
	// If the include list is empty, always query any block.
	if len(m.IncludeBlocks) == 0 {
		return true
	}

	searchID := id.String()
	for _, b := range m.IncludeBlocks {
		if b.Id == searchID {
			return true
		}
	}
	return false
}

func (m *SeriesResponseHints) AddQueriedBlock(id ulid.ULID) {
	m.QueriedBlocks = append(m.QueriedBlocks, Block{
		Id: id.String(),
	})
}
