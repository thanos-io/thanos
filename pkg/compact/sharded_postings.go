package compact

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

type ShardedPostingsFunc struct {
	partitionCount uint64
	partitionID    uint64
}

func (p *ShardedPostingsFunc) GetPostings(originalPostings index.Postings, indexReader tsdb.IndexReader) index.Postings {
	return NewShardedPosting(originalPostings, p.partitionCount, p.partitionID, indexReader.Series)
}

type ShardedPostings struct {
	//postings Postings
	//labelsFn func(ref storage.SeriesRef, lset *labels.Labels, chks *[]chunks.Meta) error
	series      []storage.SeriesRef
	cur         storage.SeriesRef
	initialized bool

	//bufChks []chunks.Meta
	//bufLbls labels.Labels
}

func NewShardedPosting(postings index.Postings, partitionCount uint64, partitionId uint64, labelsFn func(ref storage.SeriesRef, lset *labels.Labels, chks *[]chunks.Meta) error) *ShardedPostings {
	bufChks := make([]chunks.Meta, 0)
	bufLbls := make(labels.Labels, 0)
	series := make([]storage.SeriesRef, 0)
	for postings.Next() {
		err := labelsFn(postings.At(), &bufLbls, &bufChks)
		if err != nil {
			fmt.Printf("err: %v", err)
		}
		if bufLbls.Hash()%partitionCount == partitionId {
			posting := postings.At()
			series = append(series, posting)
		}
	}
	return &ShardedPostings{series: series, initialized: false}
}

func (p *ShardedPostings) Next() bool {
	if len(p.series) > 0 {
		p.cur, p.series = p.series[0], p.series[1:]
		return true
	}
	return false
}

func (p *ShardedPostings) At() storage.SeriesRef {
	return p.cur
}

func (p *ShardedPostings) Seek(v storage.SeriesRef) bool {
	fmt.Println("ran seek")
	return false
}

func (p *ShardedPostings) Err() error {
	return nil
}
