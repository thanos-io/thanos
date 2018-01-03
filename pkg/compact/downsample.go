package compact

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strings"

	"github.com/improbable-eng/thanos/pkg/block"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// Downsample downsamples the given block. It writes a new block into dir and returns its ID.
func Downsample(ctx context.Context, b *tsdb.Block, dir string, window int64) (id ulid.ULID, err error) {
	h, err := tsdb.NewHead(nil, nil, tsdb.NopWAL(), math.MaxInt64)
	if err != nil {
		return id, err
	}
	defer h.Close()

	q, err := tsdb.NewBlockQuerier(b, b.Meta().MinTime, b.Meta().MaxTime)
	if err != nil {
		return id, errors.Wrap(err, "create block querier")
	}
	defer q.Close()

	set, err := q.Select(labels.NewEqualMatcher(index.AllPostingsKey()))
	if err != nil {
		return id, errors.Wrap(err, "select all series")
	}
	for set.Next() {
		s := set.At()

		app := h.Appender()

		if err := downsampleAggr(app, s.Labels(), s.Iterator(), window); err != nil {
			return id, errors.Wrap(err, "downsample average")
		}
		if err := app.Commit(); err != nil {
			return id, errors.Wrap(err, "commit")
		}
	}
	if set.Err() != nil {
		return id, errors.Wrap(set.Err(), "iterate series set")
	}
	comp, err := tsdb.NewLeveledCompactor(nil, log.NewNopLogger(), []int64{b.Meta().MaxTime - b.Meta().MinTime}, nil)
	if err != nil {
		return id, errors.Wrap(err, "create compactor")
	}
	id, err = comp.Write(dir, h, b.Meta().MinTime, b.Meta().MaxTime)
	if err != nil {
		return id, errors.Wrap(err, "compact head")
	}
	bdir := filepath.Join(dir, id.String())

	origMeta, err := block.ReadMetaFile(b.Dir())
	if err != nil {
		return id, errors.Wrap(err, "read block meta")
	}

	meta, err := block.ReadMetaFile(bdir)
	if err != nil {
		return id, errors.Wrap(err, "read block meta")
	}
	meta.Thanos.Labels = origMeta.Thanos.Labels
	meta.Thanos.DownsamplingWindow = window
	meta.Compaction = origMeta.Compaction

	if err := block.WriteMetaFile(bdir, meta); err != nil {
		return id, errors.Wrap(err, "write block meta")
	}
	return id, nil
}

type aggregator struct {
	total   int     // total samples processed
	count   int     // samples in current window
	sum     float64 // value sum of current window
	min     float64 // min of current window
	max     float64 // max of current window
	counter float64 // total counter state since beginning
	resets  int     // number of counter resests since beginning
	last    float64 // last added value
}

func (a *aggregator) reset() {
	a.count = 0
	a.sum = 0
	a.min = math.MaxFloat64
	a.max = -math.MaxFloat64
}

func (a *aggregator) add(v float64) {
	if a.total > 0 {
		if v < a.last {
			// Counter reset, correct the value.
			a.counter += v
			a.resets++
		} else {
			// Add delta with last value to the counter.
			a.counter += v - a.last
		}
	} else {
		// First sample sets the counter.
		a.counter = v
	}
	a.last = v

	a.sum += v
	a.count++
	a.total++

	if v < a.min {
		a.min = v
	}
	if v > a.max {
		a.max = v
	}
}

func (a *aggregator) get() float64 {
	return a.sum / float64(a.count)
}

func aggrLset(lset labels.Labels, aggr string) labels.Labels {
	res := make(labels.Labels, len(lset))
	copy(res, lset)

	for i, l := range res {
		if l.Name == "__name__" {
			res[i].Value = fmt.Sprintf("%s$%s", l.Value, aggr)
		}
	}
	return res
}

func isCounter(lset labels.Labels) bool {
	metric := lset.Get("__name__")
	return strings.HasSuffix(metric, "_total") ||
		strings.HasSuffix(metric, "_bucket") ||
		strings.HasSuffix(metric, "_sum")
}

func downsampleAggr(app tsdb.Appender, lset labels.Labels, it tsdb.SeriesIterator, window int64) error {
	countLset := aggrLset(lset, "count")
	sumLset := aggrLset(lset, "sum")
	minLset := aggrLset(lset, "min")
	maxLset := aggrLset(lset, "max")
	counterLset := aggrLset(lset, "counter")

	var countRef, sumRef, minRef, maxRef, counterRef uint64

	var aggr aggregator
	gauge := !isCounter(lset)

	add := func(ref *uint64, lset labels.Labels, t int64, v float64) error {
		if *ref > 0 {
			if err := app.AddFast(*ref, t, v); err != nil {
				return err
			}
		} else {
			r, err := app.Add(lset, t, v)
			if err != nil {
				return err
			}
			*ref = r
		}
		return nil
	}
	addAll := func(t int64) error {
		if gauge {
			if err := add(&countRef, countLset, t, float64(aggr.count)); err != nil {
				return err
			}
			if err := add(&sumRef, sumLset, t, aggr.sum); err != nil {
				return err
			}
			if err := add(&minRef, minLset, t, aggr.min); err != nil {
				return err
			}
			if err := add(&maxRef, maxLset, t, aggr.max); err != nil {
				return err
			}
		}
		if err := add(&counterRef, counterLset, t, aggr.counter); err != nil {
			return err
		}
		return nil
	}

	nextT := int64(-1)

	var t int64
	var v float64
	for i := 0; it.Next(); i++ {
		t, v = it.At()

		if t > nextT {
			if nextT != -1 {
				if err := addAll(nextT); err != nil {
					return errors.Wrap(err, "add sample")
				}
			}
			aggr.reset()
			nextT = t - (t % window) + window - 1
		}
		aggr.add(v)
	}
	if it.Err() != nil {
		return it.Err()
	}
	// We are done if there was no further sample after we added the last aggregate.
	if aggr.count > 0 {
		if err := addAll(nextT); err != nil {
			return errors.Wrap(err, "add sample")
		}
	}
	// For the counter, we append the last sample at the very end. It is not meant for general
	// usage but to indicate what the last value was, so that resets w.r.t to adjacent data
	// can be determined.
	return errors.Wrap(add(&counterRef, counterLset, 0, v), "add sample")
}
