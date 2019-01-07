package query

import (
	"context"
	"regexp"
	"sort"
	"strings"

	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

// WarningReporter allows to report warnings to frontend layer.
//
// Warning can include partial errors `partialResponse` is enabled. It occurs when only part of the results are ready and
// another is not available because of the failure.
// It is required to be thread-safe.
type WarningReporter func(error)

// QueryableCreator returns implementation of promql.Queryable that fetches data from the proxy store API endpoints.
// If deduplication is enabled, all data retrieved from it will be deduplicated along the replicaLabel by default.
// maxSourceResolution controls downsampling resolution that is allowed.
// partialResponse controls `partialResponseDisabled` option of StoreAPI and partial response behaviour of proxy.
type QueryableCreator func(deduplicate bool, maxSourceResolution time.Duration, partialResponse bool, r WarningReporter) storage.Queryable

// NewQueryableCreator creates QueryableCreator.
func NewQueryableCreator(logger log.Logger, proxy storepb.StoreServer, replicaLabel string, replicaPriorities map[string]int) QueryableCreator {
	priorities := make([]replicaPriority, 0, len(replicaPriorities))
	for k, v := range replicaPriorities {
		priorities = append(priorities, replicaPriority{
			pattern:           regexp.MustCompile(k),
			uncompiledPattern: k,
			priority:          v,
		})
	}
	sort.Slice(priorities, func(i, j int) bool {
		return strings.Compare(priorities[i].uncompiledPattern, priorities[j].uncompiledPattern) < 0
	})

	return func(deduplicate bool, maxSourceResolution time.Duration, partialResponse bool, r WarningReporter) storage.Queryable {
		return &queryable{
			logger:              logger,
			replicaLabel:        replicaLabel,
			replicaPriorities:   priorities,
			proxy:               proxy,
			deduplicate:         deduplicate,
			maxSourceResolution: maxSourceResolution,
			partialResponse:     partialResponse,
			warningReporter:     r,
		}
	}
}

type queryable struct {
	logger              log.Logger
	replicaLabel        string
	replicaPriorities   []replicaPriority
	proxy               storepb.StoreServer
	deduplicate         bool
	maxSourceResolution time.Duration
	partialResponse     bool
	warningReporter     WarningReporter
}

// Querier returns a new storage querier against the underlying proxy store API.
func (q *queryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return newQuerier(ctx, q.logger, mint, maxt, q.replicaLabel, q.replicaPriorities, q.proxy, q.deduplicate, int64(q.maxSourceResolution/time.Millisecond), q.partialResponse, q.warningReporter), nil
}

type querier struct {
	ctx                 context.Context
	logger              log.Logger
	cancel              func()
	mint, maxt          int64
	replicaLabel        string
	replicaPriorities   []replicaPriority
	proxy               storepb.StoreServer
	deduplicate         bool
	maxSourceResolution int64
	partialResponse     bool
	warningReporter     WarningReporter
}

// newQuerier creates implementation of storage.Querier that fetches data from the proxy
// store API endpoints.
func newQuerier(
	ctx context.Context,
	logger log.Logger,
	mint, maxt int64,
	replicaLabel string,
	replicaPriorities []replicaPriority,
	proxy storepb.StoreServer,
	deduplicate bool,
	maxSourceResolution int64,
	partialResponse bool,
	warningReporter WarningReporter,
) *querier {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if warningReporter == nil {
		warningReporter = func(error) {}
	}
	ctx, cancel := context.WithCancel(ctx)
	return &querier{
		ctx:                 ctx,
		logger:              logger,
		cancel:              cancel,
		mint:                mint,
		maxt:                maxt,
		replicaLabel:        replicaLabel,
		replicaPriorities:   replicaPriorities,
		proxy:               proxy,
		deduplicate:         deduplicate,
		maxSourceResolution: maxSourceResolution,
		partialResponse:     partialResponse,
		warningReporter:     warningReporter,
	}
}

func (q *querier) isDedupEnabled() bool {
	return q.deduplicate && q.replicaLabel != ""
}

type seriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer
	ctx context.Context

	seriesSet []storepb.Series
	warnings  []string
}

func (s *seriesServer) Send(r *storepb.SeriesResponse) error {
	if r.GetWarning() != "" {
		s.warnings = append(s.warnings, r.GetWarning())
		return nil
	}

	if r.GetSeries() == nil {
		return errors.New("no seriesSet")
	}
	s.seriesSet = append(s.seriesSet, *r.GetSeries())
	return nil
}

func (s *seriesServer) Context() context.Context {
	return s.ctx
}

type resAggr int

const (
	resAggrAvg resAggr = iota
	resAggrCount
	resAggrSum
	resAggrMin
	resAggrMax
	resAggrCounter
)

// aggrsFromFunc infers aggregates of the underlying data based on the wrapping
// function of a series selection.
func aggrsFromFunc(f string) ([]storepb.Aggr, resAggr) {
	if f == "min" || strings.HasPrefix(f, "min_") {
		return []storepb.Aggr{storepb.Aggr_MIN}, resAggrMin
	}
	if f == "max" || strings.HasPrefix(f, "max_") {
		return []storepb.Aggr{storepb.Aggr_MAX}, resAggrMax
	}
	if f == "count" || strings.HasPrefix(f, "count_") {
		return []storepb.Aggr{storepb.Aggr_COUNT}, resAggrCount
	}
	if f == "sum" || strings.HasPrefix(f, "sum_") {
		return []storepb.Aggr{storepb.Aggr_SUM}, resAggrSum
	}
	if f == "increase" || f == "rate" {
		return []storepb.Aggr{storepb.Aggr_COUNTER}, resAggrCounter
	}
	// In the default case, we retrieve count and sum to compute an average.
	return []storepb.Aggr{storepb.Aggr_COUNT, storepb.Aggr_SUM}, resAggrAvg
}

func (q *querier) Select(params *storage.SelectParams, ms ...*labels.Matcher) (storage.SeriesSet, error) {
	span, ctx := tracing.StartSpan(q.ctx, "querier_select")
	defer span.Finish()

	sms, err := translateMatchers(ms...)
	if err != nil {
		return nil, errors.Wrap(err, "convert matchers")
	}

	queryAggrs, resAggr := aggrsFromFunc(params.Func)

	resp := &seriesServer{ctx: ctx}
	if err := q.proxy.Series(&storepb.SeriesRequest{
		MinTime:                 q.mint,
		MaxTime:                 q.maxt,
		Matchers:                sms,
		MaxResolutionWindow:     q.maxSourceResolution,
		Aggregates:              queryAggrs,
		PartialResponseDisabled: !q.partialResponse,
	}, resp); err != nil {
		return nil, errors.Wrap(err, "proxy Series()")
	}

	for _, w := range resp.warnings {
		q.warningReporter(errors.New(w))
	}

	if !q.isDedupEnabled() {
		// Return data without any deduplication.
		return promSeriesSet{
			mint: q.mint,
			maxt: q.maxt,
			set:  newStoreSeriesSet(resp.seriesSet),
			aggr: resAggr,
		}, nil
	}

	// TODO(fabxc): this could potentially pushed further down into the store API
	// to make true streaming possible.
	sortDedupLabels(resp.seriesSet, q.replicaLabel, q.replicaPriorities)

	set := promSeriesSet{
		mint: q.mint,
		maxt: q.maxt,
		set:  newStoreSeriesSet(resp.seriesSet),
		aggr: resAggr,
	}

	// The merged series set assembles all potentially-overlapping time ranges
	// of the same series into a single one. The series are ordered so that equal series
	// from different replicas are sequential and ordered by priority. We can now deduplicate those.
	return newDedupSeriesSet(set, q.replicaLabel, q.replicaPriorities), nil
}

// sortDedupLabels resorts the set so that the same series with different replica
// labels are coming right after each other. It also applies replica priorities if they
// are configured.
func sortDedupLabels(set []storepb.Series, replicaLabel string, replicaPriorities []replicaPriority) {
	priorities := map[string]int{}
	// Move the replica label to the very end so we can reliably operate on it.
	for _, s := range set {
		sort.Slice(s.Labels, func(i, j int) bool {
			if s.Labels[i].Name == replicaLabel {
				return false
			}
			if s.Labels[j].Name == replicaLabel {
				return true
			}
			return s.Labels[i].Name < s.Labels[j].Name
		})

		// Update priorities map
		if len(s.Labels) < 1 || s.Labels[len(s.Labels)-1].Name != replicaLabel {
			continue
		}
		currentReplica := s.Labels[len(s.Labels)-1].Value
		if _, ok := priorities[currentReplica]; ok {
			continue
		}
		for _, priority := range replicaPriorities {
			if priority.pattern.MatchString(currentReplica) {
				priorities[currentReplica] = priority.priority
				goto found
			}
		}
		priorities[currentReplica] = MinInt
	found:
	}

	// Sort by labels (including replica label) first.
	// This correctly sorts the set for the base case (where there are no priorities).
	// If there are no priorities for anything, then the remaining sort.SliceStable calls after this
	// will make no swaps and consequently run in O(n) anyway, so the performance impact of
	// these extra sorts is minimal.
	sort.Slice(set, func(i, j int) bool {
		return storepb.CompareLabels(set[i].Labels, set[j].Labels) < 0
	})

	// Sort the set by priority first
	sort.SliceStable(set, func(i, j int) bool {
		if len(set[i].Labels) < 1 || len(set[j].Labels) < 1 {
			return false
		}

		// Apply priorities if they exist, otherwise push the item to the end of the slice.
		// The iterator starts with the last pair of replicas in the list, and slowly filters down
		// through them, so if we order replicas in ascending priority then the existing iterator
		// will handle priorities with minimal modification.
		iReplicaLabel, jReplicaLabel := set[i].Labels[len(set[i].Labels)-1], set[j].Labels[len(set[j].Labels)-1]
		if iReplicaLabel.Name == replicaLabel && jReplicaLabel.Name == replicaLabel {
			if iPriority, ok := priorities[iReplicaLabel.Value]; ok {
				if jPriority, ok := priorities[jReplicaLabel.Value]; ok {
					return iPriority > jPriority
				}
				// i has a priority but j doesn't, so i should go first anyway
				return true
			}
		}
		// Either i doesn't have a priority and shouldn't go first, or both i and j have priorities
		// and j is lower. Either way, false is return value for all cases not handled above.
		return false
	})

	// Everything is currently sorted by priority and then label values.
	// Series with the same priorities will therefore be adjacent and sorted by label values within
	// those priority buckets. Stable sorting by label value again but excluding replica labels will
	// result in the set being ordered by labelset then priority.
	sort.SliceStable(set, func(i, j int) bool {
		// No labels, default to the existing sort order
		if len(set[i].Labels) < 1 && len(set[i].Labels) < 1 {
			return false
		}

		// Strip the replica label if present
		iLabels, jLabels := set[i].Labels[:], set[j].Labels[:]
		if iLabels[len(iLabels)-1].Name == replicaLabel {
			iLabels = iLabels[:len(iLabels)-1]
		}
		if jLabels[len(jLabels)-1].Name == replicaLabel {
			jLabels = jLabels[:len(jLabels)-1]
		}

		return storepb.CompareLabels(iLabels, jLabels) < 0
	})
}

func (q *querier) LabelValues(name string) ([]string, error) {
	span, ctx := tracing.StartSpan(q.ctx, "querier_label_values")
	defer span.Finish()

	resp, err := q.proxy.LabelValues(ctx, &storepb.LabelValuesRequest{Label: name, PartialResponseDisabled: !q.partialResponse})
	if err != nil {
		return nil, errors.Wrap(err, "proxy LabelValues()")
	}

	for _, w := range resp.Warnings {
		q.warningReporter(errors.New(w))
	}

	return resp.Values, nil
}

func (q *querier) Close() error {
	q.cancel()
	return nil
}
