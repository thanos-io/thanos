package indexcache

import (
	"io/ioutil"
	"os"

	"github.com/golang/snappy"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// BinaryCache is a binary index cache that uses protobufs + snappy compression.
type BinaryCache struct {
	IndexCache

	logger log.Logger
}

// WriteIndexCache writes an index cache into the specified filename.
func (c *BinaryCache) WriteIndexCache(indexFn string, fn string) error {
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

	// Extract label value indices.
	lnames, err := indexr.LabelNames()
	if err != nil {
		return errors.Wrap(err, "read label indices")
	}
	labelValues := map[string]*Values{}
	for _, lns := range lnames {
		if len(lns) != 1 {
			continue
		}
		ln := string(lns[0])

		tpls, err := indexr.LabelValues(ln)
		if err != nil {
			return errors.Wrap(err, "get label values")
		}
		vals := Values{}

		for i := 0; i < tpls.Len(); i++ {
			v, err := tpls.At(i)
			if err != nil {
				return errors.Wrap(err, "get label value")
			}
			if len(v) != 1 {
				return errors.Errorf("unexpected tuple length %d", len(v))
			}
			vals.Val = append(vals.Val, v[0])
		}

		labelValues[ln] = &vals
	}

	// Extract postings ranges.
	pranges, err := indexr.PostingsRanges()
	if err != nil {
		return errors.Wrap(err, "read postings ranges")
	}
	postings := []*PostingsRange{}
	for l, rng := range pranges {
		postings = append(postings, &PostingsRange{
			Name:  l.Name,
			Value: l.Value,
			Start: rng.Start,
			End:   rng.End,
		})
	}

	indexCache := Cache{
		Version:     int32(indexr.Version()),
		Symbols:     symbols,
		Labelvalues: labelValues,
		Postings:    postings,
	}

	snappyWriter := snappy.NewBufferedWriter(f)
	defer runutil.CloseWithLogOnErr(c.logger, f, "snappy index cache writer")
	snappyBuffer, err := indexCache.Marshal()
	if err != nil {
		return errors.Wrap(err, "index cache marshal")
	}
	if _, err := snappyWriter.Write(snappyBuffer); err != nil {
		return errors.Wrap(err, "snappy writer write index cache")
	}
	if err := snappyWriter.Flush(); err != nil {
		return errors.Wrap(err, "snappy writer flush")
	}

	return nil
}

// ReadIndexCache reads the index cache from the specified file.
func (c *BinaryCache) ReadIndexCache(fn string) (version int,
	symbols map[uint32]string,
	lvals map[string][]string,
	postings map[labels.Label]index.Range,
	err error) {
	indexFile, err := os.Open(fn)
	if err != nil {
		return 0, nil, nil, nil, errors.Wrapf(err, "open index file %s", fn)
	}
	defer runutil.CloseWithLogOnErr(c.logger, indexFile, "index cache reader")
	snappyReader := snappy.NewReader(indexFile)
	indexContent, err := ioutil.ReadAll(snappyReader)
	if err != nil {
		return 0, nil, nil, nil, errors.Wrap(err, "snappy read all index cache")
	}
	bCache := &Cache{}
	if err = bCache.Unmarshal(indexContent); err != nil {
		return 0, nil, nil, nil, errors.Wrap(err, "unmarshal index content")
	}

	strs := map[string]string{}
	lvals = make(map[string][]string, len(bCache.Labelvalues))
	postings = make(map[labels.Label]index.Range, len(bCache.Postings))

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

	for o, s := range bCache.Symbols {
		bCache.Symbols[o] = getStr(s)
	}
	for ln, vals := range bCache.Labelvalues {
		for i := range vals.Val {
			vals.Val[i] = getStr(vals.Val[i])
		}
		lvals[getStr(ln)] = vals.Val
	}
	for _, e := range bCache.Postings {
		l := labels.Label{
			Name:  getStr(e.Name),
			Value: getStr(e.Value),
		}
		postings[l] = index.Range{Start: e.Start, End: e.End}
	}
	return int(bCache.Version), bCache.Symbols, lvals, postings, nil
}
