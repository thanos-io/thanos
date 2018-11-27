package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// Querier will repeatedly perform a number of queries at random against a Prometheus endpoint, collecting
// statistics about its performance.

var (
	fHost               = flag.String("host", "localhost:9090", "The Prometheus host or host:port to query.")
	fPath               = flag.String("path", "/api/v1/query_range", "Path to append to host.")
	fHTTPScheme         = flag.String("scheme", "http", "Scheme to use for calling into Prometheus.")
	fUsername           = flag.String("username", "", "Username for basic auth (if needed).")
	fPassword           = flag.String("password", "", "Password for basic auth (if needed).")
	fQueries            = flag.String("queries", "", "A semicolon separated list of queries to use.")
	fQueryTotalTime     = flag.Duration("query-time", time.Minute, "The length of time that queries will be run. As many queries as possible will be run within this time.")
	fQueryRangeStart    = flag.Duration("range-offset-start", 1*time.Hour, "The offset to the start of the range to use in queries.")
	fQueryRangeEnd      = flag.Duration("range-offset-end", 0, "The offset to the end of the range to use in queries.")
	fConcurrentRequests = flag.Int("concurrent-requests", 1, "The maximum amount of concurrent requests to perform. Default is no concurrency.")
	fLogLevel           = flag.String("log-level", "info", "The logging verbosity. Values are debug, info, warn, error.")
	fServer             = flag.Bool("server", false, "Run Prometheus Querier as a server.")
	fErrorThreshold     = flag.Int("error-threshold", 250, "The maximum amount of errors per query before killing the run.")
)

type totals struct {
	Errors           int           `json:"errors"`
	Successes        int           `json:"successes"`
	AverageDuration  float64       `json:"avg_duration"`
	TotalDuration    time.Duration `json:"total_duration"`
	QueriesPerSecond float64       `json:"queries_per_second"`

	lock      sync.Mutex
	queryPool []*queryResult
}

func (t *totals) getErrors() int {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.Errors
}

func (t *totals) addError() {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.Errors++
}

func (t *totals) addSuccess() {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.Successes++
}

type queryResult struct {
	Query            string        `json:"query"`
	Errors           int           `json:"errors"`
	Successes        int           `json:"successes"`
	AverageDuration  float64       `json:"avg_duration"`
	MinDuration      float64       `json:"min_duration"`
	MaxDuration      float64       `json:"max_duration"`
	TotalDuration    time.Duration `json:"total_duration"`
	QueriesPerSecond float64       `json:"queries_per_second"`

	lock     sync.Mutex
	queryURL url.URL
}

func (qr *queryResult) addError() {
	qr.lock.Lock()
	defer qr.lock.Unlock()

	qr.Errors++
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

	if *fQueries == "" {
		level.Error(logger).Log("msg", "no Queries supplied")
		os.Exit(1)
	}

	queries := strings.Split(*fQueries, ";")
	now := time.Now()
	start := now.Add(-*fQueryRangeStart)
	end := now.Add(-*fQueryRangeEnd)
	step := int(end.Sub(start).Seconds() / 250)

	t := &totals{
		queryPool: []*queryResult{},
	}

	for _, query := range queries {
		queryURL := url.URL{
			Scheme: *fHTTPScheme,
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

		t.queryPool = append(t.queryPool, &queryResult{
			Query:       query,
			queryURL:    queryURL,
			MinDuration: math.MaxInt64,
		})
	}

	summaryStart := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *fConcurrentRequests; i++ {
		wg.Add(1)
		go performQuery(logger, t, *fQueryTotalTime, &wg)
	}
	wg.Wait()
	t.TotalDuration = time.Since(summaryStart)

	if t.getErrors() >= *fErrorThreshold {
		level.Error(logger).Log("msg", "too many errors, exiting early")
	}

	if err := printResults(t); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	// If no server needs to be run return early.
	if !*fServer {
		return
	}

	// Launch server to display queryPool if needed.
	resultsBytes, err := json.Marshal(t.queryPool)
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	http.HandleFunc("/queryPool", func(w http.ResponseWriter, req *http.Request) {
		if _, err := w.Write(resultsBytes); err != nil {
			level.Error(logger).Log("err", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	level.Info(logger).Log("msg", "serving results")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		level.Error(logger).Log("msg", "could not start http server")
		os.Exit(1)
	}
}

func performQuery(logger log.Logger, totals *totals, timeout time.Duration, wg *sync.WaitGroup) {
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

		n := r.Intn(len(totals.queryPool))
		resultStorage := totals.queryPool[n]

		// Drop out if we have too many errors.
		if totals.getErrors() >= *fErrorThreshold {
			return
		}

		queryStart := time.Now()
		resp, err := http.Get(resultStorage.queryURL.String())
		duration := time.Since(queryStart)
		if err != nil {
			totals.addError()
			level.Info(logger).Log("msg", "query failed")
			continue
		}

		// Check prometheus response success code.
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 {
			totals.addError()
			resultStorage.addError()

			if resp != nil {
				level.Info(logger).Log("msg", "query failed with %d", resp.StatusCode)
			} else {
				level.Info(logger).Log("err", err)
			}
			continue
		}

		var promResult PromResult
		if err := json.Unmarshal(body, &promResult); err != nil {
			totals.addError()
			resultStorage.addError()
			level.Info(logger).Log("err", err)
			continue
		}

		if promResult.Status != "success" {
			totals.addError()
			resultStorage.addError()
			level.Info(logger).Log("err", fmt.Sprintf("prometheus query reported failure: %s", resp.Body))
			continue
		}

		totals.addSuccess()
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

// printResults will calculate totals and print queryPool for individual queries and for the run overall.
func printResults(totals *totals) error {
	var totalTime time.Duration
	for _, resultStorage := range totals.queryPool {
		if resultStorage.Successes > 0 {
			totalTime += resultStorage.TotalDuration
			resultStorage.AverageDuration = resultStorage.TotalDuration.Seconds() / float64(resultStorage.Successes)
			resultStorage.QueriesPerSecond = float64(resultStorage.Successes) / resultStorage.TotalDuration.Seconds()
		}
	}

	avg := totalTime.Seconds() / float64(totals.Successes)
	const tableFormat = "%s\t%f\t%f\t%d\t%d\t\n"
	table := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.Debug)
	fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t\n", "Query", "avg", "qps", "success_total", "errors_total")
	fmt.Fprintf(table, tableFormat,
		"ALL",
		avg,
		float64(totals.Successes)/totals.TotalDuration.Seconds(),
		totals.Successes,
		totals.Errors,
	)

	for _, result := range totals.queryPool {
		fmt.Fprintf(table, tableFormat,
			result.Query,
			result.AverageDuration,
			result.QueriesPerSecond,
			result.Successes,
			result.Errors)
	}
	return table.Flush()
}
