// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package replicate

import (
	"context"
	"math/rand"
	"time"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	objstoretracing "github.com/thanos-io/objstore/tracing/opentracing"

	thanosblock "github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extprom"
	thanosmodel "github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/server/http"
)

const (
	// Labels for metrics.
	labelSuccess = "success"
	labelError   = "error"
)

// ParseFlagMatchers parse flag into matchers.
func ParseFlagMatchers(s string) ([]*labels.Matcher, error) {
	amMatchers, err := amlabels.ParseMatchers(s)
	if err != nil {
		return nil, err
	}
	matchers := make([]*labels.Matcher, 0, len(amMatchers))
	for _, a := range amMatchers {
		if !model.LabelName.IsValid(model.LabelName(a.Name)) {
			return nil, errors.Errorf("unsupported format for label %s", a.Name)
		}
		matchers = append(matchers, labels.MustNewMatcher(labels.MatchType(a.Type), a.Name, a.Value))
	}

	return matchers, nil
}

// RunReplicate replicate data based on config.
func RunReplicate(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	_ opentracing.Tracer,
	httpBindAddr string,
	httpTLSConfig string,
	httpGracePeriod time.Duration,
	labelSelector labels.Selector,
	resolutions []compact.ResolutionLevel,
	compactions []int,
	fromObjStoreConfig *extflag.PathOrContent,
	toObjStoreConfig *extflag.PathOrContent,
	singleRun bool,
	minTime, maxTime *thanosmodel.TimeOrDurationValue,
	blockIDs []ulid.ULID,
	ignoreMarkedForDeletion bool,
) error {
	logger = log.With(logger, "component", "replicate")

	level.Debug(logger).Log("msg", "setting up http listen-group")

	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		prober.NewInstrumentation(component.Replicate, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	s := http.New(logger, reg, component.Replicate, httpProbe,
		http.WithListen(httpBindAddr),
		http.WithGracePeriod(httpGracePeriod),
		http.WithTLSConfig(httpTLSConfig),
	)

	g.Add(func() error {
		level.Info(logger).Log("msg", "Listening for http service", "address", httpBindAddr)

		statusProber.Healthy()

		return s.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		s.Shutdown(err)
	})

	fromConfContentYaml, err := fromObjStoreConfig.Content()
	if err != nil {
		return err
	}

	if len(fromConfContentYaml) == 0 {
		return errors.New("No supported bucket was configured to replicate from")
	}

	bkt, err := client.NewBucket(logger, fromConfContentYaml, component.Replicate.String())
	if err != nil {
		return err
	}
	fromBkt := objstoretracing.WrapWithTraces(
		objstore.WrapWithMetrics(
			bkt,
			prometheus.WrapRegistererWithPrefix("thanos_", prometheus.WrapRegistererWith(prometheus.Labels{"replicate": "from"}, reg)),
			bkt.Name(),
		),
	)

	toConfContentYaml, err := toObjStoreConfig.Content()
	if err != nil {
		return err
	}

	if len(toConfContentYaml) == 0 {
		return errors.New("No supported bucket was configured to replicate to")
	}

	toBkt, err := client.NewBucket(logger, toConfContentYaml, component.Replicate.String())
	if err != nil {
		return err
	}
	toBkt = objstoretracing.WrapWithTraces(
		objstore.WrapWithMetrics(
			toBkt,
			prometheus.WrapRegistererWithPrefix("thanos_", prometheus.WrapRegistererWith(prometheus.Labels{"replicate": "to"}, reg)),
			toBkt.Name(),
		),
	)

	replicationRunCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_replicate_replication_runs_total",
		Help: "The number of replication runs split by success and error.",
	}, []string{"result"})
	replicationRunCounter.WithLabelValues(labelSuccess)
	replicationRunCounter.WithLabelValues(labelError)

	replicationRunDuration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "thanos_replicate_replication_run_duration_seconds",
		Help: "The Duration of replication runs split by success and error.",
	}, []string{"result"})
	replicationRunDuration.WithLabelValues(labelSuccess)
	replicationRunDuration.WithLabelValues(labelError)
	fetcher, err := newMetaFetcher(logger, fromBkt, reg, *minTime, *maxTime, 32, ignoreMarkedForDeletion)
	if err != nil {
		return errors.Wrapf(err, "create meta fetcher with bucket %v", fromBkt)
	}

	blockFilter := NewBlockFilter(
		logger,
		labelSelector,
		resolutions,
		compactions,
		blockIDs,
	).Filter
	metrics := newReplicationMetrics(reg)
	ctx, cancel := context.WithCancel(context.Background())

	replicateFn := func() error {
		timestamp := time.Now()
		entropy := ulid.Monotonic(rand.New(rand.NewSource(timestamp.UnixNano())), 0)

		runID, err := ulid.New(ulid.Timestamp(timestamp), entropy)
		if err != nil {
			return errors.Wrap(err, "generate replication run-id")
		}

		logger := log.With(logger, "replication-run-id", runID.String())
		level.Info(logger).Log("msg", "running replication attempt")

		if err := newReplicationScheme(logger, metrics, blockFilter, fetcher, fromBkt, toBkt, reg).execute(ctx); err != nil {
			return errors.Wrap(err, "replication execute")
		}

		return nil
	}

	g.Add(func() error {
		defer runutil.CloseWithLogOnErr(logger, fromBkt, "from bucket client")
		defer runutil.CloseWithLogOnErr(logger, toBkt, "to bucket client")

		statusProber.Ready()
		if singleRun || len(blockIDs) > 0 {
			return replicateFn()
		}

		return runutil.Repeat(time.Minute, ctx.Done(), func() error {
			start := time.Now()
			if err := replicateFn(); err != nil {
				level.Error(logger).Log("msg", "running replication failed", "err", err)
				replicationRunCounter.WithLabelValues(labelError).Inc()
				replicationRunDuration.WithLabelValues(labelError).Observe(time.Since(start).Seconds())

				// No matter the error we want to repeat indefinitely.
				return nil
			}
			replicationRunCounter.WithLabelValues(labelSuccess).Inc()
			replicationRunDuration.WithLabelValues(labelSuccess).Observe(time.Since(start).Seconds())
			level.Info(logger).Log("msg", "ran replication successfully")

			return nil
		})
	}, func(error) {
		cancel()
	})

	level.Info(logger).Log("msg", "starting replication")

	return nil
}

func newMetaFetcher(
	logger log.Logger,
	fromBkt objstore.InstrumentedBucket,
	reg prometheus.Registerer,
	minTime,
	maxTime thanosmodel.TimeOrDurationValue,
	concurrency int,
	ignoreMarkedForDeletion bool,
) (*thanosblock.MetaFetcher, error) {
	filters := []thanosblock.MetadataFilter{
		thanosblock.NewTimePartitionMetaFilter(minTime, maxTime),
	}
	if ignoreMarkedForDeletion {
		filters = append(filters, thanosblock.NewIgnoreDeletionMarkFilter(logger, fromBkt, 0, concurrency))
	}
	baseBlockIDsFetcher := thanosblock.NewBaseBlockIDsFetcher(logger, fromBkt)
	return thanosblock.NewMetaFetcher(
		logger,
		concurrency,
		fromBkt,
		baseBlockIDsFetcher,
		"",
		reg,
		filters,
	)
}
