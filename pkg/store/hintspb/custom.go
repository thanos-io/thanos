package hintspb

func (m *SeriesResponseHints) AddQueriedBlock(id string) {
	m.QueriedBlocks = append(m.QueriedBlocks, Block{
		Id: id,
	})
}
