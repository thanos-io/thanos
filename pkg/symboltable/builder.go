// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package symboltable

// Builder is a string interner. It eventually turns into a list of
// offsets and a contiguous bytes buffer.
type Builder struct {
	Table       map[string]Entry
	SymbolsSize uint32
}

func NewBuilder() *Builder {
	return &Builder{Table: make(map[string]Entry)}
}

func (s *Builder) Reset() {
	clear(s.Table)
	s.SymbolsSize = 0
}

// AddEntry interns item into the table and returns its index.
func (s *Builder) AddEntry(item string) uint32 {
	entry, ok := s.Table[item]
	if ok {
		return entry.Index
	}
	entry = Entry{
		Index: uint32(len(s.Table)),
		Start: s.SymbolsSize,
	}
	s.SymbolsSize += uint32(len(item))
	s.Table[item] = entry
	return entry.Index
}

// Len returns the number of distinct interned symbols.
func (s *Builder) Len() int32 {
	return int32(len(s.Table))
}

// Entry is the metadata Builder stores for each interned string.
type Entry struct {
	// Index is the sequential index assigned to the symbol.
	Index uint32
	// Start is the start byte offset in the contiguous symbols buffer.
	Start uint32
}
