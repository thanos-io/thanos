package main

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/labels"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/tsdb"
	"gopkg.in/alecthomas/kingpin.v2"
)

// Allow for more realistic output.
type series struct {
	Jitter float64
	Max    float64
	Min    float64
	Result queryData
}

type queryData struct {
	ResultType model.ValueType `json:"resultType"`
	Result     model.Vector    `json:"result"`
}

func main() {
	app := kingpin.New(filepath.Base(os.Args[0]), "Generates artificial metrics from min time to given max time in compacted TSDB format (including head WAL).")
	app.HelpFlag.Short('h')
	input := app.Flag("input", "Input file for series config.").Required().String()
	outputDir := app.Flag("output-dir", "Output directory for generated TSDB data.").Required().String()
	scrapeInterval := app.Flag("scrape-interval", "Interval for to generate samples with.").Default("15s").Duration()

	retention := app.Flag("retention", "Defines the the max time in relation to current time for generated samples.").Required().Duration()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	_, err := app.Parse(os.Args[1:])
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	f, err := ioutil.ReadFile(*input)
	if err != nil {
		level.Error(logger).Log("err", err, "file", input)
		os.Exit(1)
	}

	var s []series
	if err := json.Unmarshal(f, &s); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	// Same code as Prometheus for compaction levels and max block.
	rngs := tsdb.ExponentialBlockRanges(int64(time.Duration(2*time.Hour).Seconds()*1000), 10, 3)
	maxBlockDuration := *retention / 10
	for i, v := range rngs {
		if v > int64(maxBlockDuration.Seconds()*1000) {
			rngs = rngs[:i]
			break
		}
	}

	db, err := tsdb.Open(*outputDir, nil, nil, &tsdb.Options{
		BlockRanges:       rngs,
		RetentionDuration: uint64(retention.Seconds() * 1000),
		NoLockfile:        true,
	})
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	// Of course there will be small gap in minTime vs time.Now once we finish.
	// We are fine with this.
	n := time.Now()
	maxTime := timestamp.FromTime(n)
	minTime := timestamp.FromTime(n.Add(-*retention))

	a := db.Appender()
	for _, in := range s {
		lset := labels.New()
		for n, v := range in.Result.Result[0].Metric {
			lset = append(lset, labels.Label{Name: string(n), Value: string(v)})
		}
		level.Info(logger).Log("msg", "generating series", "lset", lset, "minTime", minTime, "maxTime", maxTime)
		if err := generateSeries(a, lset, minTime, maxTime, *scrapeInterval, &basicGen{min: in.Min, max: in.Max, jitter: in.Jitter}); err != nil {
			level.Error(logger).Log("err", err)
			os.Exit(1)
		}
	}

	if err := a.Commit(); err != nil {
		level.Error(logger).Log("msg", "commit", "err", err)
		os.Exit(1)
	}

	// Don't wait for compact, it will be compacted by Prometheus anyway.

	if err := db.Close(); err != nil {
		level.Error(logger).Log("msg", "close", "err", err)
		os.Exit(1)
	}

	level.Info(logger).Log("msg", "done")
}

type basicGen struct {
	min, max, jitter float64

	v float64
	i int64
}

func (b *basicGen) NextValue() float64 {
	defer func() { b.i++ }()

	if b.i == 0 {
		b.v = rand.Float64() * (b.max - b.min)
	}

	var mod float64
	if b.jitter > 0 {
		mod = (rand.Float64() - 0.5) * b.jitter
	}

	return b.v + mod
}

type gen interface {
	NextValue() float64
}

func generateSeries(a tsdb.Appender, labels labels.Labels, minTime, maxTime int64, interval time.Duration, g gen) error {
	r, err := a.Add(labels, minTime, g.NextValue())
	if err != nil {
		if err == tsdb.ErrOutOfBounds {
			err = errors.Wrap(err, "have you removed old blocks and wal?")
		}
		return errors.Wrap(err, "add")
	}

	minTime += int64(interval.Seconds() * 1000)
	for minTime <= maxTime {
		if err := a.AddFast(r, minTime, g.NextValue()); err != nil {
			r, err = a.Add(labels, minTime, g.NextValue())
			if err != nil {
				return errors.Wrap(err, "add+1")
			}
		}

		minTime += int64(interval.Seconds() * 1000)
	}

	return nil
}
