// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/thanos-io/thanos/pkg/httpconfig"
	"github.com/thanos-io/thanos/pkg/promclient"
)

// headSeriesLimiter encompasses active series limiting logic.
type headSeriesLimiter interface {
	QueryMetaMonitoring(context.Context, log.Logger) error
	isUnderLimit(string, log.Logger) (bool, error)
}

// headSeriesLimit implements activeSeriesLimiter interface.
type headSeriesLimit struct {
	mtx                    sync.RWMutex
	limit                  map[string]uint64
	tenantCurrentSeriesMap map[string]float64
	defaultLimit           uint64

	metaMonitoringURL    *url.URL
	metaMonitoringClient *http.Client
	metaMonitoringQuery  string

	configuredTenantLimit  *prometheus.GaugeVec
	configuredDefaultLimit prometheus.Gauge
	limitedRequests        *prometheus.CounterVec
	metaMonitoringErr      prometheus.Counter
}

func NewHeadSeriesLimit(w WriteLimitsConfig, registerer prometheus.Registerer, logger log.Logger) *headSeriesLimit {
	limit := &headSeriesLimit{
		metaMonitoringURL:   w.GlobalLimits.metaMonitoringURL,
		metaMonitoringQuery: w.GlobalLimits.MetaMonitoringLimitQuery,
		configuredTenantLimit: promauto.With(registerer).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "thanos_receive_tenant_head_series_limit",
				Help: "The configured limit for active (head) series of tenants.",
			}, []string{"tenant"},
		),
		configuredDefaultLimit: promauto.With(registerer).NewGauge(
			prometheus.GaugeOpts{
				Name: "thanos_receive_default_head_series_limit",
				Help: "The configured default limit for active (head) series of tenants.",
			},
		),
		limitedRequests: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "thanos_receive_head_series_limited_requests_total",
				Help: "The total number of remote write requests that have been dropped due to active series limiting.",
			}, []string{"tenant"},
		),
		metaMonitoringErr: promauto.With(registerer).NewCounter(
			prometheus.CounterOpts{
				Name: "thanos_receive_metamonitoring_failed_queries_total",
				Help: "The total number of meta-monitoring queries that failed while limiting.",
			},
		),
	}

	// Initialize map for configured limits of each tenant.
	limit.limit = map[string]uint64{}
	for t, w := range w.TenantsLimits {
		if w.HeadSeriesLimit != 0 {
			limit.limit[t] = w.HeadSeriesLimit
			limit.configuredTenantLimit.WithLabelValues(t).Set(float64(w.HeadSeriesLimit))
		}
	}

	if w.DefaultLimits.HeadSeriesLimit != 0 {
		limit.defaultLimit = w.DefaultLimits.HeadSeriesLimit
		limit.configuredDefaultLimit.Set(float64(w.DefaultLimits.HeadSeriesLimit))
	}

	// Initialize map for current head series of each tenant.
	limit.tenantCurrentSeriesMap = map[string]float64{}

	// Use specified HTTPConfig (if any) to make requests to meta-monitoring.
	c := httpconfig.ClientConfig{TransportConfig: httpconfig.DefaultTransportConfig}
	if w.GlobalLimits.MetaMonitoringHTTPClient != nil {
		c = *w.GlobalLimits.MetaMonitoringHTTPClient
	}

	var err error
	limit.metaMonitoringClient, err = httpconfig.NewHTTPClient(c, "meta-mon-for-limit")
	if err != nil {
		level.Error(logger).Log("msg", "improper http client config", "err", err.Error())
	}

	return limit
}

// QueryMetaMonitoring queries any Prometheus Query API compatible meta-monitoring
// solution with the configured query for getting current active (head) series of all tenants.
// It then populates tenantCurrentSeries map with result.
func (h *headSeriesLimit) QueryMetaMonitoring(ctx context.Context, logger log.Logger) error {
	c := promclient.NewWithTracingClient(logger, h.metaMonitoringClient, httpconfig.ThanosUserAgent)

	vectorRes, _, err := c.QueryInstant(ctx, h.metaMonitoringURL, h.metaMonitoringQuery, time.Now(), promclient.QueryOptions{})
	if err != nil {
		h.metaMonitoringErr.Inc()
		return err
	}

	level.Debug(logger).Log("msg", "successfully queried meta-monitoring", "vectors", len(vectorRes))

	h.mtx.Lock()
	defer h.mtx.Unlock()
	// Construct map of tenant name and current head series.
	for _, e := range vectorRes {
		for k, v := range e.Metric {
			if k == "tenant" {
				h.tenantCurrentSeriesMap[string(v)] = float64(e.Value)
				level.Debug(logger).Log("msg", "tenant value queried", "tenant", string(v), "value", e.Value)
			}
		}
	}

	return nil
}

// isUnderLimit ensures that the current number of active series for a tenant does not exceed given limit.
// It does so in a best-effort way, i.e, in case meta-monitoring is unreachable, it does not impose limits.
func (h *headSeriesLimit) isUnderLimit(tenant string, logger log.Logger) (bool, error) {
	h.mtx.RLock()
	defer h.mtx.RUnlock()
	if len(h.limit) == 0 && h.defaultLimit == 0 {
		return true, nil
	}

	// In such limiting flow, we ingest the first remote write request
	// and then check meta-monitoring metric to ascertain current active
	// series. As such metric is updated in intervals, it is possible
	// that Receive ingests more series than the limit, before detecting that
	// a tenant has exceeded the set limits.
	v, ok := h.tenantCurrentSeriesMap[tenant]
	if !ok {
		return true, errors.Newf("tenant not in current series map")
	}

	// Check if config has specified limits for this tenant. If not specified,
	// set to default limit.
	var limit uint64
	limit, ok = h.limit[tenant]
	if !ok {
		limit = h.defaultLimit
	}

	if v >= float64(limit) {
		level.Error(logger).Log("msg", "tenant above limit", "currentSeries", v, "limit", limit)
		h.limitedRequests.WithLabelValues(tenant).Inc()
		return false, nil
	}

	return true, nil
}

// nopSeriesLimit implements activeSeriesLimiter interface as no-op.
type nopSeriesLimit struct{}

func NewNopSeriesLimit() *nopSeriesLimit {
	return &nopSeriesLimit{}
}

func (a *nopSeriesLimit) QueryMetaMonitoring(_ context.Context, _ log.Logger) error {
	return nil
}

func (a *nopSeriesLimit) isUnderLimit(_ string, _ log.Logger) (bool, error) {
	return true, nil
}
