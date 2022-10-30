// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos/internal/cortex/querier/series"
	"github.com/thanos-io/thanos/pkg/httpconfig"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type promClientsQueryable struct {
	httpMethod string
	step       time.Duration

	logger            log.Logger
	promClients       []*promclient.Client
	queryClients      []*httpconfig.Client
	ignoredLabelNames []string

	duplicatedQuery prometheus.Counter
}
type promClientsQuerier struct {
	ctx        context.Context
	mint, maxt int64
	step       int64
	httpMethod string

	logger              log.Logger
	promClients         []*promclient.Client
	queryClients        []*httpconfig.Client
	restoreIgnoreLabels []string

	// We use a dummy counter here because the duplicated
	// addresses are already tracked by rule evaluation part.
	duplicatedQuery prometheus.Counter
}

// NewPromClientsQueryable creates a queryable that queries queriers from Prometheus clients.
func NewPromClientsQueryable(logger log.Logger, queryClients []*httpconfig.Client, promClients []*promclient.Client,
	httpMethod string, step time.Duration, ignoredLabelNames []string) *promClientsQueryable {
	return &promClientsQueryable{
		logger:            logger,
		queryClients:      queryClients,
		promClients:       promClients,
		duplicatedQuery:   promauto.With(nil).NewCounter(prometheus.CounterOpts{}),
		httpMethod:        httpMethod,
		step:              step,
		ignoredLabelNames: ignoredLabelNames,
	}
}

// Querier returns a new Querier for the given time range.
func (q *promClientsQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &promClientsQuerier{
		ctx:                 ctx,
		mint:                mint,
		maxt:                maxt,
		step:                int64(q.step / time.Second),
		httpMethod:          q.httpMethod,
		logger:              q.logger,
		queryClients:        q.queryClients,
		promClients:         q.promClients,
		restoreIgnoreLabels: q.ignoredLabelNames,
	}, nil
}

// Select implements storage.Querier interface.
func (q *promClientsQuerier) Select(_ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	query := storepb.PromMatchersToString(matchers...)

	for _, i := range rand.Perm(len(q.queryClients)) {
		promClient := q.promClients[i]
		endpoints := RemoveDuplicateQueryEndpoints(q.logger, q.duplicatedQuery, q.queryClients[i].Endpoints())
		for _, i := range rand.Perm(len(endpoints)) {
			m, warns, err := promClient.QueryRange(q.ctx, endpoints[i], query, q.mint, q.maxt, q.step, promclient.QueryOptions{
				Deduplicate: true,
				Method:      q.httpMethod,
			})

			if err != nil {
				level.Error(q.logger).Log("err", err, "query", q)
				continue
			}
			if len(warns) > 0 {
				level.Warn(q.logger).Log("warnings", strings.Join(warns, ", "), "query", q)
			}
			matrix := make([]*model.SampleStream, 0, m.Len())
			for _, metric := range m {
				for _, label := range q.restoreIgnoreLabels {
					delete(metric.Metric, model.LabelName(label))
				}

				matrix = append(matrix, &model.SampleStream{
					Metric: metric.Metric,
					Values: metric.Values,
				})
			}

			return series.MatrixToSeriesSet(matrix)
		}
	}
	return storage.NoopSeriesSet()
}

// LabelValues implements storage.LabelQuerier interface.
func (q *promClientsQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

// LabelNames implements storage.LabelQuerier interface.
func (q *promClientsQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

// Close implements storage.LabelQuerier interface.
func (q *promClientsQuerier) Close() error {
	return nil
}

// RemoveDuplicateQueryEndpoints removes duplicate endpoints from the list of urls.
func RemoveDuplicateQueryEndpoints(logger log.Logger, duplicatedQueriers prometheus.Counter, urls []*url.URL) []*url.URL {
	set := make(map[string]struct{})
	deduplicated := make([]*url.URL, 0, len(urls))
	for _, u := range urls {
		if _, ok := set[u.String()]; ok {
			level.Warn(logger).Log("msg", "duplicate query address is provided", "addr", u.String())
			duplicatedQueriers.Inc()
			continue
		}
		deduplicated = append(deduplicated, u)
		set[u.String()] = struct{}{}
	}
	return deduplicated
}
