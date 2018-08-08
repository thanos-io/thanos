package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// Querier is a server which will repeatedly perform a number of queries against a prometheus endpoint, collecting
// statistics about its performance. Results are printed to stdout. This component is designed to sit inside the same
// cluster as the prometheus instance to reduce variance from network uncertainties.

var (
	fEndpoint           = flag.String("endpoint", "", "The prometheus endpoint to query.")
	fQueries            = flag.String("queries", "", "A semicolon separated list of queries to test.")
	fQueryTotalTime     = flag.Duration("query-time", time.Minute, "The length of time that queries will be run. As many queries as possible will be run within this time.")
	fQueryRangeStart    = flag.Duration("range-offset-start", 1*time.Hour, "The offset to the start of the range to use in queries.")
	fQueryRangeEnd      = flag.Duration("range-offset-end", 0, "The offset to the end of the range to use in queries.")
	fConcurrentRequests = flag.Int("concurrent-requests", 1, "The maximum amount of concurrent requests to perform. Default is no concurrency.")
	fLogLevel           = flag.String("log-level", "info", "The logging verbosity. Values are debug, info, warn, error")
)

const (
	// Stop querying if a query has more than `errorThreshold` errors.
	errorThreshold = 250
)

type QueryResult struct {
	Query            string  `json:"query"`
	Errors           int     `json:"errors"`
	Successes        int     `json:"successes"`
	AverageDuration  float64 `json:"avg_duration"`
	MinDuration      float64 `json:"min_duration"`
	MaxDuration      float64 `json:"max_duration"`
	TotalDuration    float64 `json:"total_duration"`
	QueriesPerSecond float64 `json:"queries_per_second"`

	lock sync.Mutex
}

func (q *QueryResult) GetErrors() int {
	q.lock.Lock()
	defer q.lock.Unlock()

	return q.Errors
}

func (q *QueryResult) AddError() {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.Errors++
}

type PromResult struct {
	Status string `json:"status"`
}

func main() {
	flag.Parse()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
	var lvl level.Option
	switch *fLogLevel {
	case "error":
		lvl = level.AllowError()
	case "warn":
		lvl = level.AllowWarn()
	case "info":
		lvl = level.AllowInfo()
	case "debug":
		lvl = level.AllowDebug()
	default:
		panic("unexpected log level")
	}
	logger = level.NewFilter(logger, lvl)

	queries := strings.Split(*fQueries, ";")

	now := time.Now()
	start := now.Add(-*fQueryRangeStart)
	end := now.Add(-*fQueryRangeEnd)
	step := int(end.Sub(start).Seconds() / 250)
	queryURLTemplate := fmt.Sprintf("%s/api/v1/query_range?query=%%s&start=%d&end=%d&step=%d", *fEndpoint, start.Unix(), end.Unix(), step)
	level.Debug(logger).Log("Using query url template", queryURLTemplate)

	// Gather query statistics.
	results := make([]*QueryResult, len(queries))
	for i, q := range queries {
		results[i] = runQuery(logger, queryURLTemplate, q, *fQueryTotalTime, *fConcurrentRequests)
	}

	resultsBytes, err := json.Marshal(results)
	if err != nil {
		level.Error(logger).Log("could not marshal results")
		return
	}

	http.HandleFunc("/results", func(w http.ResponseWriter, req *http.Request) {
		if _, err := w.Write(resultsBytes); err != nil {
			level.Error(logger).Log("failed to serve results")
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	level.Info(logger).Log("Serving results")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		level.Error(logger).Log("could not start http server")
	}
}

// runQuery runs a query a number of times. We aim for the query to run for about `queryTotalTime` before returning.
func runQuery(logger log.Logger, queryURLTemplate string, query string, timeout time.Duration, concurrentReqs int) *QueryResult {
	res := &QueryResult{
		Query:       query,
		MinDuration: math.MaxInt64,
	}

	queryURL := fmt.Sprintf(queryURLTemplate, url.QueryEscape(query))

	level.Info(logger).Log("Starting testing query", query)

	// Run queries concurrently.
	var wg sync.WaitGroup
	for i := 0; i < concurrentReqs; i++ {
		wg.Add(1)
		go performQuery(logger, res, queryURL, timeout, &wg)
	}

	wg.Wait()

	level.Info(logger).Log("Finished testing query", query)

	return res
}

func performQuery(logger log.Logger, resultStorage *QueryResult, query string, timeout time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()

	var totalDuration time.Duration

	timeoutC := time.After(timeout)
	for {
		// Perform as many queries as possible within this time.
		select {
		case <-timeoutC:
			// Calculate averages on completion.
			resultStorage.lock.Lock()
			resultStorage.TotalDuration = totalDuration.Seconds()
			if resultStorage.Successes > 0 {
				resultStorage.AverageDuration = totalDuration.Seconds() / float64(resultStorage.Successes)
				resultStorage.QueriesPerSecond = 1 / resultStorage.AverageDuration
			}
			resultStorage.lock.Unlock()

			return
		default:
		}

		// Drop out if we have too many errors.
		if resultStorage.GetErrors() >= errorThreshold {
			level.Error(logger).Log("too many errors, skipping the remainder", resultStorage.Query)
			return
		}

		// Start timer.
		queryStart := time.Now()

		// Run query.
		resp, err := http.Get(query)
		if err != nil || resp.StatusCode != 200 {
			resultStorage.AddError()

			if resp != nil {
				level.Error(logger).Log("query failed", resp.StatusCode)
			} else {
				level.Error(logger).Log("query failed")
			}

			continue
		}

		// Check prometheus response success code.
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			resultStorage.AddError()
			level.Error(logger).Log("failed to read query response body")
			continue
		}
		level.Debug(logger).Log("Received query response", body)

		var promResult PromResult
		if err := json.Unmarshal(body, &promResult); err != nil {
			resultStorage.AddError()
			level.Error(logger).Log("failed to unmarshal prometheus result")
			continue
		}

		if promResult.Status != "success" {
			resultStorage.AddError()
			level.Error(logger).Log("prometheus query reported failure: %s", resp.Body)
			continue
		}

		// End timer.
		duration := time.Since(queryStart)
		totalDuration += duration

		resultStorage.lock.Lock()
		if duration.Seconds() > resultStorage.MaxDuration {
			resultStorage.MaxDuration = duration.Seconds()
		}
		if duration.Seconds() < resultStorage.MinDuration {
			resultStorage.MinDuration = duration.Seconds()
		}

		resultStorage.Successes++
		resultStorage.lock.Unlock()
	}
}
