package query

import (
	"net/http"

	"context"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

var _ promql.Queryable = (*Queryable)(nil)
var _ storage.Querier = (*querier)(nil)
var _ storage.SeriesSet = (*seriesSet)(nil)

type Queryable struct {
	client         *http.Client
	storeAddresses []string
}

// NewQueryable creates implementation of promql.Queryable that uses given HTTP client
// to talk to each store node.
func NewQueryable(client *http.Client, storeAddresses []string) *Queryable {
	return &Queryable{
		client:         client,
		storeAddresses: storeAddresses,
	}
}

func (q *Queryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return newQuerier(q.client, q.storeAddresses), nil
}

type querier struct {
	client         *http.Client
	storeAddresses []string
}

// newQuerier creates implementation of storage.Querier that uses given HTTP client
// to talk to each store node.
func newQuerier(client *http.Client, storeAddresses []string) *querier {
	return &querier{
		client:         client,
		storeAddresses: storeAddresses,
	}
}

func (*querier) Select(...*labels.Matcher) storage.SeriesSet {
	return newSeriesSet()
}

func (*querier) LabelValues(name string) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (*querier) Close() error {
	return nil
}

type seriesSet struct{}

func newSeriesSet() *seriesSet {
	return &seriesSet{}
}

func (*seriesSet) Next() bool {
	return false
}

func (*seriesSet) At() storage.Series {
	return nil
}

func (*seriesSet) Err() error {
	return errors.New("not implemented")
}
