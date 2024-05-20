// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestReaders(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	// Create block index version 2.
	id1, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
		labels.FromStrings("a", "4"),
		labels.FromStrings("a", "5"),
		labels.FromStrings("a", "6"),
		labels.FromStrings("a", "7"),
		labels.FromStrings("a", "8"),
		labels.FromStrings("a", "9"),
		// Missing 10 on purpose.
		labels.FromStrings("a", "11"),
		labels.FromStrings("a", "12"),
		labels.FromStrings("a", "13"),
		labels.FromStrings("a", "1", "longer-string", "1"),
		labels.FromStrings("a", "1", "longer-string", "2"),
		labels.FromStrings("cluster", "a-eu-west-1"),
		labels.FromStrings("cluster", "b-eu-west-1"),
		labels.FromStrings("cluster", "c-eu-west-1"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc)
	testutil.Ok(t, err)

	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, id1.String()), metadata.NoneFunc))

	// Copy block index version 1 for backward compatibility.
	/* The block here was produced at the commit
	    706602daed1487f7849990678b4ece4599745905 used in 2.0.0 with:
	   db, _ := Open("v1db", nil, nil, nil)
	   app := db.Appender()
	   app.Add(labels.FromStrings("foo", "bar"), 1, 2)
	   app.Add(labels.FromStrings("foo", "baz"), 3, 4)
	   app.Add(labels.FromStrings("foo", "meh"), 1000*3600*4, 4) // Not in the block.
	   // Make sure we've enough values for the lack of sorting of postings offsets to show up.
	   for i := 0; i < 100; i++ {
	     app.Add(labels.FromStrings("bar", strconv.FormatInt(int64(i), 10)), 0, 0)
	   }
	   app.Commit()
	   db.compact()
	   db.Close()
	*/

	m, err := metadata.ReadFromDir("./testdata/index_format_v1")
	testutil.Ok(t, err)
	e2eutil.Copy(t, "./testdata/index_format_v1", filepath.Join(tmpDir, m.ULID.String()))

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(tmpDir, m.ULID.String()), metadata.Thanos{
		Labels:     labels.FromStrings("ext1", "1").Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}, &m.BlockMeta)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, m.ULID.String()), metadata.NoneFunc))

	for _, id := range []ulid.ULID{id1, m.ULID} {
		t.Run(id.String(), func(t *testing.T) {
			indexFile, err := fileutil.OpenMmapFile(filepath.Join(tmpDir, id.String(), block.IndexFilename))
			testutil.Ok(t, err)
			defer func() { _ = indexFile.Close() }()

			b := realByteSlice(indexFile.Bytes())

			t.Run("binary reader", func(t *testing.T) {
				fn := filepath.Join(tmpDir, id.String(), block.IndexHeaderFilename)
				_, err := WriteBinary(ctx, bkt, id, fn)
				testutil.Ok(t, err)

				br, err := NewBinaryReader(ctx, log.NewNopLogger(), nil, tmpDir, id, 3, NewBinaryReaderMetrics(nil))
				testutil.Ok(t, err)

				defer func() { testutil.Ok(t, br.Close()) }()

				if id == id1 {
					testutil.Equals(t, 1, br.version)
					testutil.Equals(t, 2, br.indexVersion)
					testutil.Equals(t, &BinaryTOC{Symbols: headerLen, PostingsOffsetTable: 114}, br.toc)
					testutil.Equals(t, int64(905), br.indexLastPostingEnd)
					testutil.Equals(t, 8, br.symbols.Size())
					testutil.Equals(t, 0, len(br.postingsV1))
					testutil.Equals(t, 3, len(br.nameSymbols))
					testutil.Equals(t, map[string]*postingValueOffsets{
						"": {
							offsets:       []postingOffset{{value: "", tableOff: 4}},
							lastValOffset: 576,
						},
						"a": {
							offsets: []postingOffset{
								{value: "1", tableOff: 9},
								{value: "13", tableOff: 32},
								{value: "4", tableOff: 54},
								{value: "7", tableOff: 75},
								{value: "9", tableOff: 89},
							},
							lastValOffset: 776,
						},
						"cluster": {
							offsets: []postingOffset{
								{value: "a-eu-west-1", tableOff: 96},
								{value: "c-eu-west-1", tableOff: 142},
							},
							lastValOffset: 824,
						},
						"longer-string": {
							offsets: []postingOffset{
								{value: "1", tableOff: 165},
								{value: "2", tableOff: 184},
							},
							lastValOffset: 901,
						},
					}, br.postings)

					vals, err := br.LabelValues("not-existing")
					testutil.Ok(t, err)
					testutil.Equals(t, []string(nil), vals)

					// single value
					rngs, err := br.PostingsOffsets("a", "9")
					testutil.Ok(t, err)
					for _, rng := range rngs {
						testutil.Assert(t, rng.End > rng.Start)
					}

					rngs, err = br.PostingsOffsets("a", "2", "3", "4", "5", "6", "7", "8", "9")
					testutil.Ok(t, err)
					for _, rng := range rngs {
						testutil.Assert(t, rng.End > rng.Start)
					}

					rngs, err = br.PostingsOffsets("a", "0")
					testutil.Ok(t, err)
					testutil.Assert(t, len(rngs) == 1)
					testutil.Equals(t, NotFoundRange, rngs[0])

					rngs, err = br.PostingsOffsets("a", "0", "10", "99")
					testutil.Ok(t, err)
					testutil.Assert(t, len(rngs) == 3)
					for _, rng := range rngs {
						testutil.Equals(t, NotFoundRange, rng)
					}

					rngs, err = br.PostingsOffsets("a", "1", "10", "9")
					testutil.Ok(t, err)
					testutil.Assert(t, len(rngs) == 3)
					testutil.Assert(t, rngs[0].End > rngs[0].Start)
					testutil.Assert(t, rngs[2].End > rngs[2].Start)
					testutil.Equals(t, NotFoundRange, rngs[1])

					// 3 values exist and 3 values don't exist.
					rngs, err = br.PostingsOffsets("cluster", "a-eu-west-1", "a-us-west-2", "b-eu-west-1", "b-us-east-1", "c-eu-west-1", "c-us-east-2")
					testutil.Ok(t, err)
					for i := 0; i < len(rngs); i++ {
						if i%2 == 0 {
							testutil.Assert(t, rngs[i].End > rngs[i].Start)
						} else {
							testutil.Equals(t, NotFoundRange, rngs[i])
						}
					}

					// Regression tests for https://github.com/thanos-io/thanos/issues/2213.
					// Most of not existing value was working despite bug, except in certain unlucky cases
					// it was causing "invalid size" errors.
					_, err = br.PostingsOffset("not-existing", "1")
					testutil.Equals(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("a", "0")
					testutil.Equals(t, NotFoundRangeErr, err)
					// Unlucky case, because the bug was causing unnecessary read & decode requiring more bytes than
					// available. For rest cases read was noop wrong, but at least not failing.
					_, err = br.PostingsOffset("a", "10")
					testutil.Equals(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("a", "121")
					testutil.Equals(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("a", "131")
					testutil.Equals(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("a", "91")
					testutil.Equals(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("longer-string", "0")
					testutil.Equals(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("longer-string", "11")
					testutil.Equals(t, NotFoundRangeErr, err)
					_, err = br.PostingsOffset("longer-string", "21")
					testutil.Equals(t, NotFoundRangeErr, err)
				}

				compareIndexToHeader(t, b, br)
			})

			t.Run("lazy binary reader", func(t *testing.T) {
				fn := filepath.Join(tmpDir, id.String(), block.IndexHeaderFilename)
				_, err := WriteBinary(ctx, bkt, id, fn)
				testutil.Ok(t, err)

				br, err := NewLazyBinaryReader(ctx, log.NewNopLogger(), nil, tmpDir, id, 3, NewLazyBinaryReaderMetrics(nil), NewBinaryReaderMetrics(nil), nil, false)
				testutil.Ok(t, err)

				defer func() { testutil.Ok(t, br.Close()) }()

				compareIndexToHeader(t, b, br)
			})
		})
	}

}

func compareIndexToHeader(t *testing.T, indexByteSlice index.ByteSlice, headerReader Reader) {
	ctx := context.Background()

	indexReader, err := index.NewReader(indexByteSlice)
	testutil.Ok(t, err)
	defer func() { _ = indexReader.Close() }()

	actVersion, err := headerReader.IndexVersion()
	testutil.Ok(t, err)
	testutil.Equals(t, indexReader.Version(), actVersion)

	if indexReader.Version() == index.FormatV2 {
		// For v2 symbols ref sequential integers 0, 1, 2 etc.
		iter := indexReader.Symbols()
		i := 0
		for iter.Next() {
			r, err := headerReader.LookupSymbol(ctx, uint32(i))
			testutil.Ok(t, err)
			testutil.Equals(t, iter.At(), r)

			i++
		}
		testutil.Ok(t, iter.Err())
		_, err := headerReader.LookupSymbol(ctx, uint32(i))
		testutil.NotOk(t, err)

	} else {
		// For v1 symbols refs are actual offsets in the index.
		symbols, err := getSymbolTable(indexByteSlice)
		testutil.Ok(t, err)

		for refs, sym := range symbols {
			r1, err := headerReader.LookupSymbol(ctx, refs)
			testutil.Ok(t, err)
			testutil.Equals(t, sym, r1)

			r2, err := headerReader.LookupSymbol(ctx, refs)
			testutil.Ok(t, err)
			testutil.Equals(t, sym, r2)
		}
		_, err = headerReader.LookupSymbol(ctx, 200000)
		testutil.NotOk(t, err)
	}

	expLabelNames, err := indexReader.LabelNames(ctx)
	testutil.Ok(t, err)
	actualLabelNames, err := headerReader.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, expLabelNames, actualLabelNames)

	expRanges, err := indexReader.PostingsRanges()
	testutil.Ok(t, err)

	minStart := int64(math.MaxInt64)
	maxEnd := int64(math.MinInt64)
	for il, lname := range expLabelNames {
		expectedLabelVals, err := indexReader.SortedLabelValues(ctx, lname)
		testutil.Ok(t, err)

		vals, err := headerReader.LabelValues(lname)
		testutil.Ok(t, err)
		testutil.Equals(t, expectedLabelVals, vals)

		for iv, v := range vals {
			if minStart > expRanges[labels.Label{Name: lname, Value: v}].Start {
				minStart = expRanges[labels.Label{Name: lname, Value: v}].Start
			}
			if maxEnd < expRanges[labels.Label{Name: lname, Value: v}].End {
				maxEnd = expRanges[labels.Label{Name: lname, Value: v}].End
			}

			ptr, err := headerReader.PostingsOffset(lname, v)
			testutil.Ok(t, err)

			// For index-cache those values are exact.
			//
			// For binary they are exact except last item posting offset. It's good enough if the value is larger than exact posting ending.
			if indexReader.Version() == index.FormatV2 {
				if iv == len(vals)-1 && il == len(expLabelNames)-1 {
					testutil.Equals(t, expRanges[labels.Label{Name: lname, Value: v}].Start, ptr.Start)
					testutil.Assert(t, expRanges[labels.Label{Name: lname, Value: v}].End <= ptr.End, "got offset %v earlier than actual posting end %v ", ptr.End, expRanges[labels.Label{Name: lname, Value: v}].End)
					continue
				}
			} else {
				// For index formatV1 the last one does not mean literally last value, as postings were not sorted.
				// Account for that. We know it's 40 label value.
				if v == "40" {
					testutil.Equals(t, expRanges[labels.Label{Name: lname, Value: v}].Start, ptr.Start)
					testutil.Assert(t, expRanges[labels.Label{Name: lname, Value: v}].End <= ptr.End, "got offset %v earlier than actual posting end %v ", ptr.End, expRanges[labels.Label{Name: lname, Value: v}].End)
					continue
				}
			}
			testutil.Equals(t, expRanges[labels.Label{Name: lname, Value: v}], ptr)
		}
	}

	ptr, err := headerReader.PostingsOffset(index.AllPostingsKey())
	testutil.Ok(t, err)
	testutil.Equals(t, expRanges[labels.Label{Name: "", Value: ""}].Start, ptr.Start)
	testutil.Equals(t, expRanges[labels.Label{Name: "", Value: ""}].End, ptr.End)
}

func prepareIndexV2Block(t testing.TB, tmpDir string, bkt objstore.Bucket) *metadata.Meta {
	/* Copy index 6MB block index version 2. It was generated via thanosbench. Meta.json:
		{
		"ulid": "01DRBP4RNVZ94135ZA6B10EMRR",
		"minTime": 1570766415000,
		"maxTime": 1570939215001,
		"stats": {
			"numSamples": 115210000,
			"numSeries": 10000,
			"numChunks": 990000
		},
		"compaction": {
			"level": 1,
			"sources": [
				"01DRBP4RNVZ94135ZA6B10EMRR"
			]
		},
		"version": 1,
		"thanos": {
			"labels": {
				"cluster": "one",
				"dataset": "continuous"
			},
			"downsample": {
				"resolution": 0
			},
			"source": "blockgen"
		}
	}
	*/

	m, err := metadata.ReadFromDir("./testdata/index_format_v2")
	testutil.Ok(t, err)
	e2eutil.Copy(t, "./testdata/index_format_v2", filepath.Join(tmpDir, m.ULID.String()))

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(tmpDir, m.ULID.String()), metadata.Thanos{
		Labels:     labels.FromStrings("ext1", "1").Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}, &m.BlockMeta)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(context.Background(), log.NewNopLogger(), bkt, filepath.Join(tmpDir, m.ULID.String()), metadata.NoneFunc))

	return m
}

func BenchmarkBinaryWrite(t *testing.B) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	m := prepareIndexV2Block(t, tmpDir, bkt)
	fn := filepath.Join(tmpDir, m.ULID.String(), block.IndexHeaderFilename)

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		_, err := WriteBinary(ctx, bkt, m.ULID, fn)
		testutil.Ok(t, err)
	}
}

func BenchmarkBinaryReader(t *testing.B) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)

	m := prepareIndexV2Block(t, tmpDir, bkt)
	fn := filepath.Join(tmpDir, m.ULID.String(), block.IndexHeaderFilename)
	_, err = WriteBinary(ctx, bkt, m.ULID, fn)
	testutil.Ok(t, err)

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		br, err := newFileBinaryReader(fn, 32, NewBinaryReaderMetrics(nil))
		testutil.Ok(t, err)
		testutil.Ok(t, br.Close())
	}
}

func BenchmarkBinaryReader_LookupSymbol(b *testing.B) {
	for _, numSeries := range []int{valueSymbolsCacheSize, valueSymbolsCacheSize * 10} {
		b.Run(fmt.Sprintf("num series = %d", numSeries), func(b *testing.B) {
			benchmarkBinaryReaderLookupSymbol(b, numSeries)
		})
	}
}

func benchmarkBinaryReaderLookupSymbol(b *testing.B, numSeries int) {
	const postingOffsetsInMemSampling = 32

	ctx := context.Background()
	logger := log.NewNopLogger()

	tmpDir := b.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(b, err)
	defer func() { testutil.Ok(b, bkt.Close()) }()

	// Generate series labels.
	seriesLabels := make([]labels.Labels, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		seriesLabels = append(seriesLabels, labels.FromStrings("a", strconv.Itoa(i)))
	}

	// Create a block.
	id1, err := e2eutil.CreateBlock(ctx, tmpDir, seriesLabels, 100, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc)
	testutil.Ok(b, err)
	testutil.Ok(b, block.Upload(ctx, logger, bkt, filepath.Join(tmpDir, id1.String()), metadata.NoneFunc))

	// Create an index reader.
	reader, err := NewBinaryReader(ctx, logger, bkt, tmpDir, id1, postingOffsetsInMemSampling, NewBinaryReaderMetrics(nil))
	testutil.Ok(b, err)

	// Get the offset of each label value symbol.
	symbolsOffsets := make([]uint32, numSeries)
	for i := 0; i < numSeries; i++ {
		o, err := reader.symbols.ReverseLookup(strconv.Itoa(i))
		testutil.Ok(b, err)

		symbolsOffsets[i] = o
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		for i := 0; i < len(symbolsOffsets); i++ {
			if _, err := reader.LookupSymbol(ctx, symbolsOffsets[i]); err != nil {
				b.Fail()
			}
		}
	}
}

func getSymbolTable(b index.ByteSlice) (map[uint32]string, error) {
	version := int(b.Range(4, 5)[0])

	if version != 1 && version != 2 {
		return nil, errors.Errorf("unknown index file version %d", version)
	}

	toc, err := index.NewTOCFromByteSlice(b)
	if err != nil {
		return nil, errors.Wrap(err, "read TOC")
	}

	symbolsV2, symbolsV1, err := readSymbols(b, version, int(toc.Symbols))
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

// readSymbols reads the symbol table fully into memory and allocates proper strings for them.
// Strings backed by the mmap'd memory would cause memory faults if applications keep using them
// after the reader is closed.
func readSymbols(bs index.ByteSlice, version, off int) ([]string, map[uint32]string, error) {
	if off == 0 {
		return nil, nil, nil
	}
	d := encoding.NewDecbufAt(bs, off, castagnoliTable)

	var (
		origLen     = d.Len()
		cnt         = d.Be32int()
		basePos     = uint32(off) + 4
		nextPos     = basePos + uint32(origLen-d.Len())
		symbolSlice []string
		symbols     = map[uint32]string{}
	)
	if version == index.FormatV2 {
		symbolSlice = make([]string, 0, cnt)
	}

	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		s := d.UvarintStr()

		if version == index.FormatV2 {
			symbolSlice = append(symbolSlice, s)
		} else {
			symbols[nextPos] = s
			nextPos = basePos + uint32(origLen-d.Len())
		}
		cnt--
	}
	return symbolSlice, symbols, errors.Wrap(d.Err(), "read symbols")
}

// The idea of this test case is to make sure that reader.PostingsOffsets and
// reader.PostingsOffset get the same index ranges for required label values.
func TestReaderPostingsOffsets(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	possibleClusters := []string{"us-west-2", "us-east-1", "us-east-2", "eu-west-1", "eu-central-1", "ap-southeast-1", "ap-south-1"}
	possiblePrefixes := []string{"a", "b", "c", "d", "1", "2", "3", "4"}
	totalValues := []string{}
	for i := 0; i < len(possibleClusters); i++ {
		for j := 0; j < len(possiblePrefixes); j++ {
			totalValues = append(totalValues, fmt.Sprintf("%s-%s", possiblePrefixes[j], possibleClusters[i]))
		}
	}

	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	// Pick 5 label values to be used in the block.
	clusterLbls := make([]labels.Labels, 0)
	valueSet := map[int]struct{}{}
	for i := 0; i < 5; {
		idx := rnd.Intn(len(totalValues))
		if _, ok := valueSet[idx]; ok {
			continue
		}
		valueSet[idx] = struct{}{}
		clusterLbls = append(clusterLbls, labels.FromStrings("cluster", totalValues[idx]))
		i++
	}

	// Add additional labels.
	lbls := append([]labels.Labels{
		labels.FromStrings("job", "1"),
		labels.FromStrings("job", "2"),
		labels.FromStrings("job", "3"),
		labels.FromStrings("job", "4"),
		labels.FromStrings("job", "5"),
		labels.FromStrings("job", "6"),
		labels.FromStrings("job", "7"),
		labels.FromStrings("job", "8"),
		labels.FromStrings("job", "9")}, clusterLbls...)
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()
	id, err := e2eutil.CreateBlock(ctx, tmpDir, lbls, 100, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc)
	testutil.Ok(t, err)

	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, id.String()), metadata.NoneFunc))

	fn := filepath.Join(tmpDir, id.String(), block.IndexHeaderFilename)
	_, err = WriteBinary(ctx, bkt, id, fn)
	testutil.Ok(t, err)

	br, err := NewBinaryReader(ctx, log.NewNopLogger(), nil, tmpDir, id, 3, NewBinaryReaderMetrics(nil))
	testutil.Ok(t, err)

	defer func() { testutil.Ok(t, br.Close()) }()

	for i := 0; i < 100; i++ {
		vals := make([]string, 0, 15)
		for j := 0; j < 15; j++ {
			vals = append(vals, totalValues[rnd.Intn(len(totalValues))])
		}
		sort.Strings(vals)
		rngs, err := br.PostingsOffsets("cluster", vals...)
		require.NoError(t, err)
		rngs2 := make([]index.Range, 0)
		for _, val := range vals {
			rng2, err2 := br.PostingsOffset("cluster", val)
			if err2 == NotFoundRangeErr {
				rngs2 = append(rngs2, NotFoundRange)
			} else {
				rngs2 = append(rngs2, rng2)
			}
		}
		require.Equal(t, rngs2, rngs, "Got mismatched results from batched and non-batched API.\nInput cluster labels: %v.\nValues queried: %v", clusterLbls, vals)
	}
}
