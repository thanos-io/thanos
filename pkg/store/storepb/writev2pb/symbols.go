// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writev2pb

import (
	"github.com/prometheus/prometheus/model/labels"
)

// SymbolsTable implements table for easy symbol use.
type SymbolsTable struct {
	strings    []string
	symbolsMap map[string]uint32
}

// NewSymbolTable returns a symbol table.
// The first element of the symbols table is always an empty string so index will be 1-based.
func NewSymbolTable() *SymbolsTable {
	return &SymbolsTable{
		// Empty string is required as a first element.
		symbolsMap: map[string]uint32{"": 0},
		strings:    []string{""},
	}
}

// NewSymbolTableFromSymbols returns a symbol table from a list of symbols.
func NewSymbolTableFromSymbols(symbols []string) *SymbolsTable {
	st := NewSymbolTable()
	for _, symbol := range symbols {
		st.Symbolize(symbol)
	}
	return st
}

// Symbolize adds (if not added before) a string to the symbols table,
// while returning its reference number.
func (t *SymbolsTable) Symbolize(str string) uint32 {
	if ref, ok := t.symbolsMap[str]; ok {
		return ref
	}
	ref := uint32(len(t.strings))
	t.strings = append(t.strings, str)
	t.symbolsMap[str] = ref
	return ref
}

// SymbolizeLabels symbolize Prometheus labels.
func (t *SymbolsTable) SymbolizeLabels(lbls labels.Labels, buf []uint32) []uint32 {
	buf = buf[:0]
	if cap(buf) < lbls.Len()*2 {
		buf = make([]uint32, 0, lbls.Len()*2)
	}

	lbls.Range(func(l labels.Label) {
		buf = append(buf,
			t.Symbolize(l.Name),
			t.Symbolize(l.Value))
	})

	return buf
}

// SymbolizeMetadata symbolizes metadata help and unit text.
func (t *SymbolsTable) SymbolizeMetadata(help, unit string) (uint32, uint32) {
	return t.Symbolize(help), t.Symbolize(unit)
}

// Symbols returns computes symbols table to put in e.g. Request.Symbols.
// As per spec, order does not matter.
func (t *SymbolsTable) Symbols() []string {
	return t.strings
}

// Reset clears symbols table.
func (t *SymbolsTable) Reset() {
	// NOTE: Make sure to keep empty symbol.
	t.strings = t.strings[:1]
	for k := range t.symbolsMap {
		if k == "" {
			continue
		}
		delete(t.symbolsMap, k)
	}
}

// desymbolizeLabels decodes label references, with given symbols to labels.
func DesymbolizeLabels(b *labels.ScratchBuilder, labelRefs []uint32, symbols []string) labels.Labels {
	b.Reset()
	for i := 0; i < len(labelRefs); i += 2 {
		name := symbols[labelRefs[i]]
		value := symbols[labelRefs[i+1]]
		b.Add(name, value)
	}
	b.Sort()
	return b.Labels()
}
