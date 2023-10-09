// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/teststorage"
)

var (
	minNormal = math.Float64frombits(0x0010000000000000) // The smallest positive normal value of type float64.

	patSpace = regexp.MustCompile("[\t ]+")
	// TODO(bwplotka): Parse external labels.
	patStore       = regexp.MustCompile(`^store\s+([{}=_"a-zA-Z0-9]+)\s+([0-9mds]+)\s+([0-9mds]+)$`)
	patLoad        = regexp.MustCompile(`^load\s+(.+?)$`)
	patEvalInstant = regexp.MustCompile(`^eval(?:_(fail|ordered))?\s+instant\s+(?:at\s+(.+?))?\s+(.+)$`)
)

const (
	epsilon = 0.000001 // Relative error allowed for sample values.
)

var testStartTime = time.Unix(0, 0).UTC()

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}

func timeMilliseconds(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond/time.Nanosecond)
}

type test struct {
	testing.TB

	cmds       []interface{}
	rootEngine *promql.Engine
	stores     []*testStore

	ctx       context.Context
	cancelCtx context.CancelFunc
}

type testStore struct {
	storeCmd

	storage *teststorage.TestStorage

	ctx       context.Context
	cancelCtx context.CancelFunc
}

func newTestStore(t testing.TB, cmd *storeCmd) *testStore {
	s := &testStore{
		storeCmd: *cmd,
		storage:  teststorage.New(t),
	}
	s.ctx, s.cancelCtx = context.WithCancel(context.Background())
	return s
}

// close closes resources associated with the testStore.
func (s *testStore) close(t testing.TB) {
	s.cancelCtx()

	if err := s.storage.Close(); err != nil {
		t.Fatalf("closing test storage: %s", err)
	}
}

// NewTest returns an initialized empty Test.
// It's compatible with promql.Test, allowing additionally multi StoreAPIs for query pushdown testing.
// TODO(bwplotka): Move to unittest and add add support for multi-store upstream. See: https://github.com/prometheus/prometheus/pull/8300
func newTest(t testing.TB, input string) (*test, error) {
	cmds, err := parse(input)
	if err != nil {
		return nil, err
	}

	te := &test{TB: t, cmds: cmds}
	te.reset()
	return te, err
}

func newTestFromFile(t testing.TB, filename string) (*test, error) {
	content, err := os.ReadFile(filepath.Clean(filename))
	if err != nil {
		return nil, err
	}
	return newTest(t, string(content))
}

// reset the current test storage of all inserted samples.
func (t *test) reset() {
	if t.cancelCtx != nil {
		t.cancelCtx()
	}
	t.ctx, t.cancelCtx = context.WithCancel(context.Background())

	opts := promql.EngineOpts{
		Logger:                   nil,
		Reg:                      nil,
		MaxSamples:               10000,
		Timeout:                  100 * time.Second,
		NoStepSubqueryIntervalFn: func(int64) int64 { return durationMilliseconds(1 * time.Minute) },
	}
	t.rootEngine = promql.NewEngine(opts)

	for _, s := range t.stores {
		s.close(t.TB)
	}
	t.stores = t.stores[:0]
}

// close closes resources associated with the Test.
func (t *test) close() {
	t.cancelCtx()
	for _, s := range t.stores {
		s.close(t.TB)
	}
}

// getLines returns trimmed lines after removing the comments.
func getLines(input string) []string {
	lines := strings.Split(input, "\n")
	for i, l := range lines {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "#") {
			l = ""
		}
		lines[i] = l
	}
	return lines
}

// parse parses the given input and returns command sequence.
func parse(input string) (cmds []interface{}, err error) {
	lines := getLines(input)

	// Scan for steps line by line.
	for i := 0; i < len(lines); i++ {
		l := lines[i]
		if l == "" {
			continue
		}
		var cmd interface{}

		switch c := strings.ToLower(patSpace.Split(l, 2)[0]); {
		case c == "clear":
			cmd = &clearCmd{}
		case c == "load":
			i, cmd, err = ParseLoad(lines, i)
		case strings.HasPrefix(c, "eval"):
			i, cmd, err = ParseEval(lines, i)
		case c == "store":
			i, cmd, err = ParseStore(lines, i)
		default:
			return nil, raise(i, "invalid command %q", l)
		}
		if err != nil {
			return nil, err
		}
		cmds = append(cmds, cmd)
	}
	return cmds, nil
}

func raise(line int, format string, v ...interface{}) error {
	return &parser.ParseErr{
		LineOffset: line,
		Err:        errors.Errorf(format, v...),
	}
}

// run executes the command sequence of the test. Until the maximum error number
// is reached, evaluation errors do not terminate execution.
func (t *test) run(createQueryableFn func([]*testStore) storage.Queryable) error {
	for _, cmd := range t.cmds {
		if err := t.exec(cmd, createQueryableFn); err != nil {
			return err
		}
	}
	return nil
}

// exec processes a single step of the test.
func (t *test) exec(tc interface{}, createQueryableFn func([]*testStore) storage.Queryable) error {
	switch cmd := tc.(type) {
	case *clearCmd:
		t.reset()
	case *storeCmd:
		t.stores = append(t.stores, newTestStore(t.TB, tc.(*storeCmd)))

	case *loadCmd:
		if len(t.stores) == 0 {
			t.stores = append(t.stores, newTestStore(t.TB, newStoreCmd(nil, math.MinInt64, math.MaxInt64)))
		}

		app := t.stores[len(t.stores)-1].storage.Appender(t.ctx)
		if err := cmd.Append(app); err != nil {
			_ = app.Rollback()
			return err
		}
		if err := app.Commit(); err != nil {
			return err
		}

	case *evalCmd:
		if err := cmd.Eval(t.ctx, t.rootEngine, createQueryableFn(t.stores)); err != nil {
			return err
		}

	default:
		return errors.Errorf("pkg/query.Test.exec: unknown test command type %v", cmd)
	}
	return nil
}

// storeCmd is a command that appends new storage with filter.
type storeCmd struct {
	matchers   []*labels.Matcher
	mint, maxt int64
}

func newStoreCmd(matchers []*labels.Matcher, mint, maxt int64) *storeCmd {
	return &storeCmd{
		matchers: matchers,
		mint:     mint,
		maxt:     maxt,
	}
}

func (cmd storeCmd) String() string {
	return "store"
}

// ParseStore parses store statements.
func ParseStore(lines []string, i int) (int, *storeCmd, error) {
	if !patStore.MatchString(lines[i]) {
		return i, nil, raise(i, "invalid store command. (store <matchers> <mint offset> <maxt offset>)")
	}
	parts := patStore.FindStringSubmatch(lines[i])

	m, err := parser.ParseMetricSelector(parts[1])
	if err != nil {
		return i, nil, raise(i, "invalid matcher definition %q: %s", parts[1], err)
	}

	offset, err := model.ParseDuration(parts[2])
	if err != nil {
		return i, nil, raise(i, "invalid mint definition %q: %s", parts[2], err)
	}
	mint := testStartTime.Add(time.Duration(offset))

	offset, err = model.ParseDuration(parts[3])
	if err != nil {
		return i, nil, raise(i, "invalid maxt definition %q: %s", parts[3], err)
	}
	maxt := testStartTime.Add(time.Duration(offset))
	return i, newStoreCmd(m, timestamp.FromTime(mint), timestamp.FromTime(maxt)), nil
}

// ParseLoad parses load statements.
func ParseLoad(lines []string, i int) (int, *loadCmd, error) {
	if !patLoad.MatchString(lines[i]) {
		return i, nil, raise(i, "invalid load command. (load <step:duration>)")
	}
	parts := patLoad.FindStringSubmatch(lines[i])

	gap, err := model.ParseDuration(parts[1])
	if err != nil {
		return i, nil, raise(i, "invalid step definition %q: %s", parts[1], err)
	}
	cmd := newLoadCmd(time.Duration(gap))
	for i+1 < len(lines) {
		i++
		defLine := lines[i]
		if defLine == "" {
			i--
			break
		}
		metric, vals, err := parser.ParseSeriesDesc(defLine)
		if err != nil {
			if perr, ok := err.(*parser.ParseErr); ok {
				perr.LineOffset = i
			}
			return i, nil, err
		}
		cmd.set(metric, vals...)
	}
	return i, cmd, nil
}

// ParseEval parses eval statements.
func ParseEval(lines []string, i int) (int, *evalCmd, error) {
	if !patEvalInstant.MatchString(lines[i]) {
		return i, nil, raise(i, "invalid evaluation command. (eval[_fail|_ordered] instant [at <offset:duration>] <query>")
	}
	parts := patEvalInstant.FindStringSubmatch(lines[i])
	var (
		mod  = parts[1]
		at   = parts[2]
		expr = parts[3]
	)
	_, err := parser.ParseExpr(expr)
	if err != nil {
		if perr, ok := err.(*parser.ParseErr); ok {
			perr.LineOffset = i
			posOffset := posrange.Pos(strings.Index(lines[i], expr))
			perr.PositionRange.Start += posOffset
			perr.PositionRange.End += posOffset
			perr.Query = lines[i]
		}
		return i, nil, err
	}

	offset, err := model.ParseDuration(at)
	if err != nil {
		return i, nil, raise(i, "invalid step definition %q: %s", parts[1], err)
	}
	ts := testStartTime.Add(time.Duration(offset))

	cmd := newEvalCmd(expr, ts, i+1)
	switch mod {
	case "ordered":
		cmd.ordered = true
	case "fail":
		cmd.fail = true
	}

	for j := 1; i+1 < len(lines); j++ {
		i++
		defLine := lines[i]
		if defLine == "" {
			i--
			break
		}
		if f, err := parseNumber(defLine); err == nil {
			cmd.expect(0, nil, parser.SequenceValue{Value: f})
			break
		}
		metric, vals, err := parser.ParseSeriesDesc(defLine)
		if err != nil {
			if perr, ok := err.(*parser.ParseErr); ok {
				perr.LineOffset = i
			}
			return i, nil, err
		}

		// Currently, we are not expecting any matrices.
		if len(vals) > 1 {
			return i, nil, raise(i, "expecting multiple values in instant evaluation not allowed")
		}
		cmd.expect(j, metric, vals...)
	}
	return i, cmd, nil
}

func parseNumber(s string) (float64, error) {
	n, err := strconv.ParseInt(s, 0, 64)
	f := float64(n)
	if err != nil {
		f, err = strconv.ParseFloat(s, 64)
	}
	if err != nil {
		return 0, errors.Wrap(err, "error parsing number")
	}
	return f, nil
}

// loadCmd is a command that loads sequences of sample values for specific
// metrics into the storage.
type loadCmd struct {
	gap     time.Duration
	metrics map[uint64]labels.Labels
	defs    map[uint64][]promql.FPoint
}

func newLoadCmd(gap time.Duration) *loadCmd {
	return &loadCmd{
		gap:     gap,
		metrics: map[uint64]labels.Labels{},
		defs:    map[uint64][]promql.FPoint{},
	}
}

func (cmd loadCmd) String() string {
	return "load"
}

// set a sequence of sample values for the given metric.
func (cmd *loadCmd) set(m labels.Labels, vals ...parser.SequenceValue) {
	h := m.Hash()

	samples := make([]promql.FPoint, 0, len(vals))
	ts := testStartTime
	for _, v := range vals {
		if !v.Omitted {
			samples = append(samples, promql.FPoint{
				T: ts.UnixNano() / int64(time.Millisecond/time.Nanosecond),
				F: v.Value,
			})
		}
		ts = ts.Add(cmd.gap)
	}
	cmd.defs[h] = samples
	cmd.metrics[h] = m
}

// Append the defined time series to the storage.
func (cmd *loadCmd) Append(a storage.Appender) error {
	for h, smpls := range cmd.defs {
		m := cmd.metrics[h]

		for _, s := range smpls {
			if _, err := a.Append(0, m, s.T, s.F); err != nil {
				return err
			}
		}
	}
	return nil
}

// evalCmd is a command that evaluates an expression for the given time (range)
// and expects a specific result.
type evalCmd struct {
	expr  string
	start time.Time
	line  int

	fail, ordered bool

	metrics  map[uint64]labels.Labels
	expected map[uint64]entry
}

type entry struct {
	pos  int
	vals []parser.SequenceValue
}

func (e entry) String() string {
	return fmt.Sprintf("%d: %s", e.pos, e.vals)
}

func newEvalCmd(expr string, start time.Time, line int) *evalCmd {
	return &evalCmd{
		expr:  expr,
		start: start,
		line:  line,

		metrics:  map[uint64]labels.Labels{},
		expected: map[uint64]entry{},
	}
}

func (ev *evalCmd) String() string {
	return "eval"
}

// expect adds a new metric with a sequence of values to the set of expected
// results for the query.
func (ev *evalCmd) expect(pos int, m labels.Labels, vals ...parser.SequenceValue) {
	if m == nil {
		ev.expected[0] = entry{pos: pos, vals: vals}
		return
	}
	h := m.Hash()
	ev.metrics[h] = m
	ev.expected[h] = entry{pos: pos, vals: vals}
}

// samplesAlmostEqual returns true if the two sample lines only differ by a
// small relative error in their sample value.
func almostEqual(a, b float64) bool {
	// NaN has no equality but for testing we still want to know whether both values
	// are NaN.
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}

	// Cf. http://floating-point-gui.de/errors/comparison/
	if a == b {
		return true
	}

	diff := math.Abs(a - b)

	if a == 0 || b == 0 || diff < minNormal {
		return diff < epsilon*minNormal
	}
	return diff/(math.Abs(a)+math.Abs(b)) < epsilon
}

// compareResult compares the result value with the defined expectation.
func (ev *evalCmd) compareResult(result parser.Value) error {
	switch val := result.(type) {
	case promql.Matrix:
		return errors.New("received range result on instant evaluation")

	case promql.Vector:
		seen := map[uint64]bool{}
		for pos, v := range val {
			fp := v.Metric.Hash()
			if _, ok := ev.metrics[fp]; !ok {
				return errors.Errorf("unexpected metric %s in result", v.Metric)
			}
			exp := ev.expected[fp]
			if ev.ordered && exp.pos != pos+1 {
				return errors.Errorf("expected metric %s with %v at position %d but was at %d", v.Metric, exp.vals, exp.pos, pos+1)
			}
			if !almostEqual(exp.vals[0].Value, v.F) {
				return errors.Errorf("expected %v for %s but got %v", exp.vals[0].Value, v.Metric, v.F)
			}

			seen[fp] = true
		}
		for fp, expVals := range ev.expected {
			if !seen[fp] {
				details := fmt.Sprintln("vector result", len(val), ev.expr)
				for _, ss := range val {
					details += fmt.Sprintln("    ", ss.Metric, ss.T, ss.F)
				}
				return errors.Errorf("expected metric %s with %v not found; details: %v", ev.metrics[fp], expVals, details)
			}
		}

	case promql.Scalar:
		if !almostEqual(ev.expected[0].vals[0].Value, val.V) {
			return errors.Errorf("expected Scalar %v but got %v", val.V, ev.expected[0].vals[0].Value)
		}

	default:
		panic(errors.Errorf("promql.Test.compareResult: unexpected result type %T", result))
	}
	return nil
}

func (ev *evalCmd) Eval(ctx context.Context, queryEngine *promql.Engine, queryable storage.Queryable) error {
	q, err := queryEngine.NewInstantQuery(ctx, queryable, promql.NewPrometheusQueryOpts(false, 0), ev.expr, ev.start)
	if err != nil {
		return err
	}
	defer q.Close()

	res := q.Exec(ctx)
	if res.Err != nil {
		if ev.fail {
			return nil
		}
		return errors.Wrapf(res.Err, "error evaluating query %q (line %d)", ev.expr, ev.line)
	}
	if res.Err == nil && ev.fail {
		return errors.Errorf("expected error evaluating query %q (line %d) but got none", ev.expr, ev.line)
	}

	err = ev.compareResult(res.Value)
	if err != nil {
		return errors.Wrapf(err, "error in %s %s", ev, ev.expr)
	}

	// Check query returns same result in range mode,
	// by checking against the middle step.
	q, err = queryEngine.NewRangeQuery(ctx, queryable, promql.NewPrometheusQueryOpts(false, 0), ev.expr, ev.start.Add(-time.Minute), ev.start.Add(time.Minute), time.Minute)
	if err != nil {
		return err
	}
	rangeRes := q.Exec(ctx)
	if rangeRes.Err != nil {
		return errors.Wrapf(rangeRes.Err, "error evaluating query %q (line %d) in range mode", ev.expr, ev.line)
	}
	defer q.Close()
	if ev.ordered {
		// Ordering isn't defined for range queries.
		return nil
	}
	mat := rangeRes.Value.(promql.Matrix)
	vec := make(promql.Vector, 0, len(mat))
	for _, series := range mat {
		for _, point := range series.Floats {
			if point.T == timeMilliseconds(ev.start) {
				vec = append(vec, promql.Sample{Metric: series.Metric, T: point.T, F: point.F})
				break
			}
		}
	}
	if _, ok := res.Value.(promql.Scalar); ok {
		err = ev.compareResult(promql.Scalar{V: vec[0].F})
	} else {
		err = ev.compareResult(vec)
	}
	if err != nil {
		return errors.Wrapf(err, "error in %s %s (line %d) rande mode", ev, ev.expr, ev.line)
	}
	return nil
}

// clearCmd is a command that wipes the test's storage state.
type clearCmd struct{}

func (cmd clearCmd) String() string {
	return "clear"
}
