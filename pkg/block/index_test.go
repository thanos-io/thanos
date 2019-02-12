package block

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/tsdbutil"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/tsdb/labels"
)

func TestWriteReadIndexCache(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test-compact-prepare")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	b, err := testutil.CreateBlock(tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "b", Value: "1"}},
	}, 100, 0, 1000, nil, 124)
	testutil.Ok(t, err)

	fn := filepath.Join(tmpDir, "index.cache.json")
	testutil.Ok(t, WriteIndexCache(log.NewNopLogger(), filepath.Join(tmpDir, b.String(), "index"), fn))

	version, symbols, lvals, postings, err := ReadIndexCache(log.NewNopLogger(), fn)
	testutil.Ok(t, err)

	testutil.Equals(t, 2, version)
	testutil.Equals(t, 6, len(symbols))
	testutil.Equals(t, 2, len(lvals))

	vals, ok := lvals["a"]
	testutil.Assert(t, ok, "")
	testutil.Equals(t, []string{"1", "2", "3", "4"}, vals)

	vals, ok = lvals["b"]
	testutil.Assert(t, ok, "")
	testutil.Equals(t, []string{"1"}, vals)
	testutil.Equals(t, 6, len(postings))
}

func TestGatherIndexIssueStats(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test-gather-stats")
	testutil.Ok(t, err)
	// Cleans up all temp files.
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	// Non existent file.
	_, err = GatherIndexIssueStats(log.NewNopLogger(), "", 0, 0)
	testutil.NotOk(t, err)

	// Real but empty file.
	tmpfile, err := ioutil.TempFile(tmpDir, "empty-file")
	testutil.Ok(t, err)
	_, err = GatherIndexIssueStats(log.NewNopLogger(), tmpfile.Name(), 0, 0)
	testutil.NotOk(t, err)

	// Working test cases.
	testCases := []struct {
		name       string
		lsets      []labels.Labels
		expected   Stats
		startTime  int64
		endTime    int64
		numSamples int
	}{
		{
			name: "empty case",
		},
		{
			name: "1 labelset name only",
			lsets: []labels.Labels{
				{
					{Name: "__name__", Value: "metric_name"},
				},
			},
			expected: Stats{TotalSeries: 1},
		},
		{
			name: "2 labelsets name only",
			lsets: []labels.Labels{
				{
					{Name: "__name__", Value: "metric_name"},
				}, {
					{Name: "__name__", Value: "metric_name_2"},
				},
			},
			expected: Stats{TotalSeries: 2},
		},
		{
			name: "2 labelsets with adtl labels",
			lsets: []labels.Labels{
				{
					{Name: "__name__", Value: "metric_name"},
					{Name: "label", Value: "value1"},
				}, {
					{Name: "__name__", Value: "metric_name"},
					{Name: "label", Value: "value2"},
				},
			},
			expected: Stats{TotalSeries: 2},
		},
		{
			name: "1 labelset with lots of labels",
			lsets: []labels.Labels{
				{
					{Name: "__name__", Value: "metric_name"},
					{Name: "a", Value: "1"},
					{Name: "b", Value: "2"},
					{Name: "c", Value: "3"},
					{Name: "d", Value: "4"},
					{Name: "e", Value: "5"},
				},
			},
			expected: Stats{TotalSeries: 1},
		},
		{
			// Name always comes first and prometheus doesn't sort it.
			// https://github.com/prometheus/prometheus/blob/9f903fb3f7841f55d1b634d5fb02986a8bfc5e0c/pkg/textparse/promparse.go#L230
			name: "1 labelset with capitalized label",
			lsets: []labels.Labels{
				{
					{Name: "__name__", Value: "metric_name"},
					{Name: "CapitalizedLabel", Value: "1"},
					{Name: "CapitalizedLabel2", Value: "2"},
					{Name: "more_common_label", Value: "3"},
				},
			},
			expected: Stats{TotalSeries: 1},
		},
	}

	for caseNum, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpfile, err := ioutil.TempFile(tmpDir, fmt.Sprintf("test-case-%d", caseNum))
			testutil.Ok(t, err)

			testutil.Ok(t, writeTestIndex(tmpfile.Name(), tc.startTime, tc.numSamples, tc.lsets...))

			stats, err := GatherIndexIssueStats(log.NewNopLogger(), tmpfile.Name(), tc.startTime, tc.endTime)

			testutil.Ok(t, err)
			// == works for now since the struct is only ints (no pointers or nested values)
			testutil.Assert(t, tc.expected == stats,
				"index stats did not have an expected value. \nExpected: %+v \nActual:   %+v", tc.expected, stats)
		})
	}
}

func TestGatherIndexIssueStatsBadLabelSet(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test-gather-stats")
	testutil.Ok(t, err)
	// Cleans up all temp files.
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	// All of these label sets should produce an error.
	testCases := []struct {
		name  string
		lsets []labels.Labels
	}{
		{
			name: "no __name__ label",
			lsets: []labels.Labels{
				{
					{Name: "no_name", Value: "we're missing __name__"},
				},
			},
		},
		{
			// Should be illegal according to https://github.com/prometheus/tsdb/issues/32#issuecomment-292771463
			name: "capital letter before name label",
			lsets: []labels.Labels{
				{
					{Name: "ABC", Value: "onetwothree"},
					{Name: "__name__", Value: "metric_name"},
				},
			},
		},
	}

	for caseNum, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpfile, err := ioutil.TempFile(tmpDir, fmt.Sprintf("test-case-%d", caseNum))
			testutil.Ok(t, err)

			testutil.Ok(t, writeTestIndex(tmpfile.Name(), 0, 0, tc.lsets...))

			_, err = GatherIndexIssueStats(log.NewNopLogger(), tmpfile.Name(), 0, 0)
			testutil.NotOk(t, err)
		})
	}
}

// writeTestIndex is a helper function that creates an index file with dummy data for testing
func writeTestIndex(filename string, startTime int64, numSamples int, lsets ...labels.Labels) error {
	chunks := tsdbutil.PopulatedChunk(numSamples, startTime)

	symbols := make(map[string]struct{})
	for _, lset := range lsets {
		for _, l := range lset {
			symbols[l.Name] = struct{}{}
			symbols[l.Value] = struct{}{}
		}
	}

	memPostings := index.NewMemPostings()
	for i, lset := range lsets {
		memPostings.Add(uint64(i), lset)
	}

	w, err := index.NewWriter(filename)
	if err != nil {
		return err
	}

	if err = w.AddSymbols(symbols); err != nil {
		return err
	}

	for i, lset := range lsets {
		if err = w.AddSeries(uint64(i), lset, chunks); err != nil {
			return err
		}
	}

	pKey, pVal := index.AllPostingsKey()
	if err = w.WritePostings(pKey, pVal, memPostings.All()); err != nil {
		return err
	}

	return w.Close()
}
