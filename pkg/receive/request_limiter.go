package receive

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	seriesLimitName    = "series"
	samplesLimitName   = "samples"
	sizeBytesLimitName = "body_size"
)

type configRequestLimiter struct {
	tenantLimits        map[string]*requestLimitsConfig
	cachedDefaultLimits *requestLimitsConfig
	limitsHit           *prometheus.SummaryVec
	configuredLimits    *prometheus.GaugeVec
}

func newConfigRequestLimiter(reg prometheus.Registerer, writeLimits *writeLimitsConfig) *configRequestLimiter {
	defaultRequestLimits := writeLimits.DefaultLimits.RequestLimits
	tenantLimits := writeLimits.TenantsLimits
	requestLimits := make(map[string]*requestLimitsConfig)
	for tenant, limitConfig := range tenantLimits {
		requestLimits[tenant] = limitConfig.RequestLimits.MergeWithDefaults(defaultRequestLimits)
	}
	limiter := configRequestLimiter{
		tenantLimits:        requestLimits,
		cachedDefaultLimits: newEmptyRequestLimitsConfig().MergeWithDefaults(defaultRequestLimits),
	}

	limiter.limitsHit = promauto.With(reg).NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:  "thanos",
			Subsystem:  "receive",
			Name:       "write_limits_hit",
			Help:       "The number of times a request was refused due to a remote write limit.",
			Objectives: map[float64]float64{0.50: 0.1, 0.95: 0.1, 0.99: 0.001},
		}, []string{"tenant", "limit"},
	)
	limiter.configuredLimits = promauto.With(reg).NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "thanos",
			Subsystem: "receive",
			Name:      "write_limits",
			Help:      "The configured write limits.",
		}, []string{"tenant", "limit"},
	)
	for tenant, limits := range requestLimits {
		limiter.configuredLimits.WithLabelValues(tenant, sizeBytesLimitName).Set(float64(*limits.SizeBytesLimit))
		limiter.configuredLimits.WithLabelValues(tenant, seriesLimitName).Set(float64(*limits.SeriesLimit))
		limiter.configuredLimits.WithLabelValues(tenant, samplesLimitName).Set(float64(*limits.SamplesLimit))
	}
	return &limiter
}

func (l *configRequestLimiter) AllowSizeBytes(tenant string, contentLengthBytes int64) bool {
	limits := l.limitsFor(tenant)

	if limits.SizeBytesLimit == nil || *limits.SizeBytesLimit <= 0 {
		return true
	}

	// This happens when the content length is unknown, then we allow it.
	if contentLengthBytes < 0 {
		return true
	}
	allowed := *limits.SizeBytesLimit >= contentLengthBytes
	if !allowed {
		l.limitsHit.
			WithLabelValues(tenant, sizeBytesLimitName).
			Observe(float64(contentLengthBytes - *limits.SizeBytesLimit))
	}
	return allowed
}

func (l *configRequestLimiter) AllowSeries(tenant string, amount int64) bool {
	limits := l.limitsFor(tenant)

	if limits.SeriesLimit == nil || *limits.SeriesLimit <= 0 {
		return true
	}
	allowed := *limits.SeriesLimit >= amount
	if !allowed {
		l.limitsHit.
			WithLabelValues(tenant, seriesLimitName).
			Observe(float64(amount - *limits.SeriesLimit))
	}
	return allowed
}

func (l *configRequestLimiter) AllowSamples(tenant string, amount int64) bool {
	limits := l.limitsFor(tenant)

	if limits.SamplesLimit == nil || *limits.SamplesLimit <= 0 {
		return true
	}
	allowed := *limits.SamplesLimit >= amount
	if !allowed {
		l.limitsHit.
			WithLabelValues(tenant, samplesLimitName).
			Observe(float64(amount - *limits.SamplesLimit))
	}
	return allowed
}

func (l *configRequestLimiter) limitsFor(tenant string) requestLimitsConfig {
	limits, ok := l.tenantLimits[tenant]
	if !ok {
		limits = l.cachedDefaultLimits
	}
	return *limits
}

type noopRequestLimiter struct{}

func (l *noopRequestLimiter) AllowSizeBytes(tenant string, contentLengthBytes int64) bool {
	return true
}

func (l *noopRequestLimiter) AllowSeries(tenant string, amount int64) bool {
	return true
}

func (l *noopRequestLimiter) AllowSamples(tenant string, amount int64) bool {
	return true
}
