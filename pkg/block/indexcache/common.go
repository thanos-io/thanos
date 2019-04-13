package indexcache

import (
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// IndexCache is a provider of an index cache with a different underlying implementation.
type IndexCache interface {
	WriteIndexCache(indexFn string, fn string) error
	ReadIndexCache(fn string) (version int,
		symbols map[uint32]string,
		lvals map[string][]string,
		postings map[labels.Label]index.Range,
		err error)
}

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}

func (b realByteSlice) Sub(start, end int) index.ByteSlice {
	return b[start:end]
}

func getSymbolTableBinary(b index.ByteSlice) (map[string]struct{}, error) {
	version := int(b.Range(4, 5)[0])

	if version != 1 && version != 2 {
		return nil, errors.Errorf("unknown index file version %d", version)
	}

	toc, err := index.NewTOCFromByteSlice(b)
	if err != nil {
		return nil, errors.Wrap(err, "read TOC")
	}

	symbolsV2, symbolsV1, err := index.ReadSymbols(b, version, int(toc.Symbols))
	if err != nil {
		return nil, errors.Wrap(err, "read symbols")
	}

	symbolsTable := make(map[string]struct{}, len(symbolsV1)+len(symbolsV2))
	for _, s := range symbolsV1 {
		symbolsTable[s] = struct{}{}
	}
	for _, s := range symbolsV2 {
		symbolsTable[s] = struct{}{}
	}

	return symbolsTable, nil
}

func getSymbolTableJSON(b index.ByteSlice) (map[uint32]string, error) {
	version := int(b.Range(4, 5)[0])

	if version != 1 && version != 2 {
		return nil, errors.Errorf("unknown index file version %d", version)
	}

	toc, err := index.NewTOCFromByteSlice(b)
	if err != nil {
		return nil, errors.Wrap(err, "read TOC")
	}

	symbolsV2, symbolsV1, err := index.ReadSymbols(b, version, int(toc.Symbols))
	if err != nil {
		return nil, errors.Wrap(err, "read symbols")
	}

	symbolsTable := make(map[uint32]string, len(symbolsV1)+len(symbolsV2))
	for o, s := range symbolsV1 {
		symbolsTable[o] = s
	}
	for o, s := range symbolsV2 {
		symbolsTable[uint32(o)] = s
	}

	return symbolsTable, nil
}
