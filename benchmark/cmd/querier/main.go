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
	"math/rand"
)

// Querier will repeatedly perform a number of queries at random against a Prometheus endpoint, collecting
// statistics about its performance.

var (
	fHost            = flag.String("host", "localhost", "The Prometheus host to query.")
	fPath			 = flag.String("path", "/api/v1/query_range", "Path to append to host.")
	fUsername	     = flag.String("username", "", "Username for basic auth (if needed).")
	fPassword	     = flag.String("password", "", "Password for basic auth (if needed).")
	fQueries         = flag.String("queries", "", "A semicolon separated list of queries to use.")
	fQueryTotalTime  = flag.Duration("query-time", time.Minute, "The length of time that queries will be run. As many queries as possible will be run within this time.")
	fQueryRangeStart = flag.Duration("range-offset-start", 1*time.Hour, "The offset to the start of the range to use in queries.")
	fQueryRangeEnd   = flag.Duration("range-offset-end", 0, "The offset to the end of the range to use in queries.")
	fConcurrentRequests = flag.Int("concurrent-requests", 1, "The maximum amount of concurrent requests to perform. Default is no concurrency.")
	fLogLevel           = flag.String("log-level", "info", "The logging verbosity. Values are debug, info, warn, error.")
	fServer			 = flag.Bool("server", false, "Run Prometheus Querier as a server.")
	fErrorThreshold = flag.Int("error-threshold", 250, "The maximum amount of errors per query before killing the run.")
)

type QueryResult struct {
	Query            string  `json:"query"`
	Errors           int     `json:"errors"`
	Successes        int     `json:"successes"`
	AverageDuration  float64 `json:"avg_duration"`
	MinDuration      float64 `json:"min_duration"`
	MaxDuration      float64 	`json:"max_duration"`
	TotalDuration    time.Duration `json:"total_duration"`
	QueriesPerSecond float64 `json:"queries_per_second"`

	lock     sync.Mutex
	QueryURL url.URL
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

	if *fQueries == ""{
		level.Error(logger).Log("msg", "No Queries supplied.")
		return
	}

	queries := strings.Split(*fQueries, ";")
	now := time.Now()
	start := now.Add(-*fQueryRangeStart)
	end := now.Add(-*fQueryRangeEnd)
	step := int(end.Sub(start).Seconds() / 250)

	// Gather query statistics.
	var results []*QueryResult
	for _, query := range queries {
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

		results = append(results, &QueryResult{
			Query:       query,
			QueryURL:	 queryURL,
			MinDuration: math.MaxInt64,
		})
	}

	summaryStart := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *fConcurrentRequests; i++ {
		wg.Add(1)
		go performQuery(logger, results, *fQueryTotalTime, &wg)
	}
	wg.Wait()
	duration := time.Since(summaryStart)

	// Calculate summary for queries.
	var totalDuration time.Duration
	totalSuccess, totalError := 0, 0
	for _, resultStorage := range results {
		if resultStorage.Successes > 0 {
			resultStorage.AverageDuration = resultStorage.TotalDuration.Seconds() / float64(resultStorage.Successes)
			resultStorage.QueriesPerSecond = float64(resultStorage.Successes) / resultStorage.TotalDuration.Seconds()

			totalDuration += resultStorage.TotalDuration
			totalSuccess += resultStorage.Successes
			totalError += resultStorage.Errors
		}
	}

	// launch server if needed
	if *fServer {
		resultsBytes, err := json.Marshal(results)
		if err != nil {
			level.Error(logger).Log("err", err)
			return
		}

		http.HandleFunc("/results", func(w http.ResponseWriter, req *http.Request) {
			if _, err := w.Write(resultsBytes); err != nil {
				level.Error(logger).Log("err", err)
				w.WriteHeader(http.StatusInternalServerError)
			}
		})

		level.Info(logger).Log("msg", "Serving results")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			level.Error(logger).Log("msg", "could not start http server")
		}
	}

	// Print results to Command Line.
	avg := totalDuration.Seconds() / float64(totalSuccess)
	level.Info(logger).Log("query", "ALL")
	level.Info(logger).Log("-avg_duration_seconds", avg)
	level.Info(logger).Log("-queries_per_seconds", float64(totalSuccess) / duration.Seconds())
	level.Info(logger).Log("-query_success_total", totalSuccess)
	level.Info(logger).Log("-query_errors_total", totalError)

	for _, r := range results{
		level.Info(logger).Log("query", r.Query)
		level.Info(logger).Log("-avg_duration_seconds", r.AverageDuration)
		level.Info(logger).Log("-queries_per_seconds", r.QueriesPerSecond)
		level.Info(logger).Log("-query_success_total", r.Successes)
		level.Info(logger).Log("-query_errors_total", r.Errors)
	}
}

func performQuery(logger log.Logger, results []*QueryResult, timeout time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()

	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	timeoutC := time.After(timeout)
	for {
		// Perform as many queries as possible within this time.
		select {
		case <-timeoutC:
			return
		default:
		}

		n := r.Intn(len(results))
		resultStorage := results[n]
		// Drop out if we have too many errors.
		if resultStorage.GetErrors() >= *fErrorThreshold {
			level.Info(logger).Log("msg", fmt.Sprintf("too many errors, skipping the remainder %s", resultStorage.Query))
			return
		}

		queryStart := time.Now()
		resp, err := http.Get(resultStorage.QueryURL.String())
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
			level.Error(logger).Log("err",  err)
			continue
		}

		var promResult PromResult
		if err := json.Unmarshal(body, &promResult); err != nil {
			resultStorage.AddError()
			level.Error(logger).Log("err", err)
			continue
		}

		if promResult.Status != "success" {
			resultStorage.AddError()
			level.Error(logger).Log("err", fmt.Sprintf("prometheus query reported failure: %s", resp.Body))
			continue
		}

		resultStorage.lock.Lock()
		resultStorage.TotalDuration += duration

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
