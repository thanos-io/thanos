package indexcache

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// JSONCache is a JSON index cache.
type JSONCache struct {
	IndexCache

	logger log.Logger
}

type postingsRange struct {
	Name, Value string
	Start, End  int64
}

type indexCache struct {
	Version     int
	Symbols     map[uint32]string
	LabelValues map[string][]string
	Postings    []postingsRange
}

// WriteIndexCache writes an index cache into the specified filename.
func (c *JSONCache) WriteIndexCache(indexFn string, fn string) error {
	indexFile, err := fileutil.OpenMmapFile(indexFn)
	if err != nil {
		return errors.Wrapf(err, "open mmap index file %s", indexFn)
	}
	defer runutil.CloseWithLogOnErr(c.logger, indexFile, "close index cache mmap file from %s", indexFn)

	b := realByteSlice(indexFile.Bytes())
	indexr, err := index.NewReader(b)
	if err != nil {
		return errors.Wrap(err, "open index reader")
	}
	defer runutil.CloseWithLogOnErr(c.logger, indexr, "load index cache reader")

	// We assume reader verified index already.
	symbols, err := getSymbolTableJSON(b)
	if err != nil {
		return err
	}

	f, err := os.Create(fn)
	if err != nil {
		return errors.Wrap(err, "create index cache file")
	}
	defer runutil.CloseWithLogOnErr(c.logger, f, "index cache writer")

	v := indexCache{
		Version:     indexr.Version(),
		Symbols:     symbols,
		LabelValues: map[string][]string{},
	}

	// Extract label value indices.
	lnames, err := indexr.LabelIndices()
	if err != nil {
		return errors.Wrap(err, "read label indices")
	}
	for _, lns := range lnames {
		fmt.Println(lns)
		if len(lns) != 1 {
			continue
		}
		ln := lns[0]

		tpls, err := indexr.LabelValues(ln)
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
	pranges, err := indexr.PostingsRanges()
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

// ReadIndexCache reads the index cache from the specified file.
func (c *JSONCache) ReadIndexCache(fn string) (version int,
	symbols map[uint32]string,
	lvals map[string][]string,
	postings map[labels.Label]index.Range,
	err error) {
	f, err := os.Open(fn)
	if err != nil {
		return 0, nil, nil, nil, errors.Wrap(err, "open file")
	}
	defer runutil.CloseWithLogOnErr(c.logger, f, "index reader")

	var v indexCache
	if err := json.NewDecoder(f).Decode(&v); err != nil {
		return 0, nil, nil, nil, errors.Wrap(err, "decode file")
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
	return v.Version, v.Symbols, lvals, postings, nil
}

// ToBCache converts the JSON cache into a BinaryCache one.
func (c *JSONCache) ToBCache(fnJSON string, fnB string) error {
	return nil
}
