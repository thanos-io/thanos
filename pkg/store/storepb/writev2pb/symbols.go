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
	result := buf[:0]

	for _, lbl := range lbls {
		off := t.Symbolize(lbl.Name)
		result = append(result, off)
		off = t.Symbolize(lbl.Value)
		result = append(result, off)
	}

	return result
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
		b.Add(symbols[labelRefs[i]], symbols[labelRefs[i+1]])
	}
	b.Sort()
	return b.Labels()
}
