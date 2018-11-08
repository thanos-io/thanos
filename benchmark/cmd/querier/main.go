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
	"strconv"
)

// Querier is a server which will repeatedly perform a number of queries against a prometheus endpoint, collecting
// statistics about its performance. Results are printed to stdout. This component is designed to sit inside the same
// cluster as the prometheus instance to reduce variance from network uncertainties.

var (
	fHost            = flag.String("host", "localhost", "The prometheus host to query.")
	fPath			 = flag.String("path", "/api/v1/query_range", "Path to append if needed.")
	fUsername	     = flag.String("username", "", "Username for basic auth (if needed).")
	fPassword	     = flag.String("password", "", "Password for basic auth (if needed).")
	fQueries         = flag.String("queries", "", "A semicolon separated list of queries to test.")
	fQueryTotalTime  = flag.Duration("query-time", time.Minute, "The length of time that queries will be run. As many queries as possible will be run within this time.")
	fQueryRangeStart = flag.Duration("range-offset-start", 1*time.Hour, "The offset to the start of the range to use in queries.")
	fQueryRangeEnd   = flag.Duration("range-offset-end", 0, "The offset to the end of the range to use in queries.")
	fConcurrentRequests = flag.Int("concurrent-requests", 1, "The maximum amount of concurrent requests to perform. Default is no concurrency.")
	fLogLevel           = flag.String("log-level", "info", "The logging verbosity. Values are debug, info, warn, error.")
	fServer			 = flag.Bool("server", false, "Run Prometheus Querier as a server.")
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

	// Gather query statistics.
	// TODO(dom): create map of all queries and randomly pick one to execute

	results := make([]*QueryResult, len(queries))
	for i, query := range queries {
		queryURL := url.URL{
			Scheme: "https",
			Host:   *fHost,
			Path:   *fPath,
		}
		q := queryURL.Query()
		q.Add("start", strconv.FormatInt(start.Unix(), 10))
		q.Add("end", strconv.FormatInt(end.Unix(), 10))
		q.Add("step", strconv.Itoa(step))
		q.Add("query", query)
		queryURL.RawQuery = q.Encode()

		if *fUsername != "" && *fPassword != "" {
			queryURL.User = url.UserPassword(*fUsername, *fPassword)
		}

		results[i] = runQuery(logger, queryURL, query, *fQueryTotalTime, *fConcurrentRequests)
	}

	if *fServer {
		level.Info(logger).Log("msg", "Starting Server")
		resultsBytes, err := json.Marshal(results)
		if err != nil {
			level.Error(logger).Log("could not marshal results")
			return
		}

		http.HandleFunc("/results", func(w http.ResponseWriter, req *http.Request) {
			if _, err := w.Write(resultsBytes); err != nil {
				level.Error(logger).Log("msg", "failed to serve results")
				w.WriteHeader(http.StatusInternalServerError)
			}
		})

		level.Info(logger).Log("msg", "Serving results")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			level.Error(logger).Log("msg", "could not start http server")
		}
	}

	for _, r := range results{
		level.Info(logger).Log("query", r.Query)
		level.Info(logger).Log("-avg_duration_seconds", r.AverageDuration)
		level.Info(logger).Log("-queries_per_seconds", r.QueriesPerSecond)
		level.Info(logger).Log("-query_success_total", r.Successes)
		level.Info(logger).Log("-query_errors_total", r.Errors)
	}
}

// runQuery runs a query a number of times. We aim for the query to run for about `queryTotalTime` before returning.
func runQuery(logger log.Logger, queryURL url.URL, query string, timeout time.Duration, concurrentReqs int) *QueryResult {
	res := &QueryResult{
		Query:       query,
		MinDuration: math.MaxInt64,
	}

	// Run queries concurrently.
	var wg sync.WaitGroup
	for i := 0; i < concurrentReqs; i++ {
		wg.Add(1)
		go performQuery(logger, res, queryURL, timeout, &wg)
	}

	wg.Wait()

	level.Info(logger).Log("msg", fmt.Sprintf("Finished testing query %s", query))
	return res
}

func performQuery(logger log.Logger, resultStorage *QueryResult, query url.URL, timeout time.Duration, wg *sync.WaitGroup) {
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
			level.Error(logger).Log("msg", fmt.Sprintf("too many errors, skipping the remainder %s", resultStorage.Query))
			return
		}

		// Start timer.
		queryStart := time.Now()

		// Run query.
		resp, err := http.Get(query.String())
		// End timer.
		duration := time.Since(queryStart)
		if err != nil || resp.StatusCode != 200 {
			resultStorage.AddError()

			if resp != nil {
				level.Error(logger).Log("msg", "query failed with %d", resp.StatusCode)
			} else {
				level.Error(logger).Log("msg", "query failed")
			}

			continue
		}

		// Check prometheus response success code.
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			resultStorage.AddError()
			level.Error(logger).Log("msg", "failed to read query response body")
			continue
		}

		var promResult PromResult
		if err := json.Unmarshal(body, &promResult); err != nil {
			resultStorage.AddError()
			level.Error(logger).Log("msg", "failed to unmarshal prometheus result")
			continue
		}

		if promResult.Status != "success" {
			resultStorage.AddError()
			level.Error(logger).Log("msg", fmt.Sprintf("prometheus query reported failure: %s", resp.Body))
			continue
		}


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
