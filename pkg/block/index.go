package block

import (
	"encoding/json"
	"os"

	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// IndexCacheFilename is the canonical name for index cache files.
const IndexCacheFilename = "index.cache.json"

type postingsRange struct {
	Name, Value string
	Start, End  int64
}

type indexCache struct {
	Symbols     map[uint32]string
	LabelValues map[string][]string
	Postings    []postingsRange
}

// WriteIndexCache writes a cache file containing the first lookup stages
// for an index file.
func WriteIndexCache(fn string, r *index.Reader) error {
	f, err := os.Create(fn)
	if err != nil {
		return errors.Wrap(err, "create file")
	}
	defer f.Close()

	v := indexCache{
		Symbols:     r.SymbolTable(),
		LabelValues: map[string][]string{},
	}

	// Extract label value indices.
	lnames, err := r.LabelIndices()
	if err != nil {
		return errors.Wrap(err, "read label indices")
	}
	for _, lns := range lnames {
		if len(lns) != 1 {
			continue
		}
		ln := lns[0]

		tpls, err := r.LabelValues(ln)
		if err != nil {
			return errors.Wrap(err, "get label values")
		}
		vals := make([]string, 0, tpls.Len())

		for i := 0; i < tpls.Len(); i++ {
			v, err := tpls.At(i)
			if err != nil {
				return errors.Wrap(err, "get label value")
			}
			if len(v) != 1 {
				return errors.Errorf("unexpected tuple length %d", len(v))
			}
			vals = append(vals, v[0])
		}
		v.LabelValues[ln] = vals
	}

	// Extract postings ranges.
	pranges, err := r.PostingsRanges()
	if err != nil {
		return errors.Wrap(err, "read postings ranges")
	}
	for l, rng := range pranges {
		v.Postings = append(v.Postings, postingsRange{
			Name:  l.Name,
			Value: l.Value,
			Start: rng.Start,
			End:   rng.End,
		})
	}

	if err := json.NewEncoder(f).Encode(&v); err != nil {
		return errors.Wrap(err, "encode file")
	}
	return nil
}

// ReadIndexCache reads an index cache file.
func ReadIndexCache(fn string) (
	symbols map[uint32]string,
	lvals map[string][]string,
	postings map[labels.Label]index.Range,
	err error,
) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "open file")
	}
	defer f.Close()

	var v indexCache
	if err := json.NewDecoder(f).Decode(&v); err != nil {
		return nil, nil, nil, errors.Wrap(err, "decode file")
	}
	strs := map[string]string{}
	lvals = make(map[string][]string, len(v.LabelValues))
	postings = make(map[labels.Label]index.Range, len(v.Postings))

	// Most strings we encounter are duplicates. Dedup string objects that we keep
	// around after the function returns to reduce total memory usage.
	// NOTE(fabxc): it could even make sense to deduplicate globally.
	getStr := func(s string) string {
		if cs, ok := strs[s]; ok {
			return cs
		}
		strs[s] = s
		return s
	}

	for o, s := range v.Symbols {
		v.Symbols[o] = getStr(s)
	}
	for ln, vals := range v.LabelValues {
		for i := range vals {
			vals[i] = getStr(vals[i])
		}
		lvals[getStr(ln)] = vals
	}
	for _, e := range v.Postings {
		l := labels.Label{
			Name:  getStr(e.Name),
			Value: getStr(e.Value),
		}
		postings[l] = index.Range{Start: e.Start, End: e.End}
	}
	return v.Symbols, lvals, postings, nil
}
