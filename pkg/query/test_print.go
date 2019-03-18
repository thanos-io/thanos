package query

import (
	"fmt"

	"github.com/prometheus/prometheus/storage"
)

type printSeriesSet struct {
	set storage.SeriesSet
}

func newPrintSeriesSet(set storage.SeriesSet) storage.SeriesSet {
	return &printSeriesSet{set: set}
}

func (s *printSeriesSet) Next() bool {
	return s.set.Next()
}

func (s *printSeriesSet) At() storage.Series {
	at := s.set.At()
	fmt.Println("Series", at.Labels())

	i := at.Iterator()
	for i.Next() {
		fmt.Println(i.At())
	}
	return at
}

func (s *printSeriesSet) Err() error {
	return s.set.Err()
}
