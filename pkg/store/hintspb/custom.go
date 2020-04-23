// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package hintspb

func (m *SeriesResponseHints) AddQueriedBlock(id string) {
	m.QueriedBlocks = append(m.QueriedBlocks, Block{
		Id: id,
	})
}
