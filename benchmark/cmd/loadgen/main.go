package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"
)

// Loadgen is a lightweight server which will serve a number of randomly generated time series. This can be scraped by
// a prometheus instance to simulate noisy metrics.

var (
	fNumTimeseries = flag.Int("num-timeseries", 100, "The number of unique timeseries to serve.")
	fLifetime      = flag.Duration("lifetime", 0, "If non-zero, the server will suicide after this amount of time.")

	// Use an RNG seeded with current time so we get unique metrics.
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func main() {
	flag.Parse()

	if *fLifetime > 0 {
		time.AfterFunc(*fLifetime, func() {
			os.Exit(0)
		})
	}

	http.HandleFunc("/metrics", randomMetrics)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}

func randomMetrics(w http.ResponseWriter, _ *http.Request) {
	// TODO(adamhosier) these are dummy metrics. These should be more realistic & follow https://github.com/improbable-eng/thanos/issues/346
	var metrics string
	for i := 0; i < *fNumTimeseries; i++ {
		metrics += fmt.Sprintf("ts_%d %f\n", i, rng.Float64())
	}

	if _, err := w.Write([]byte(metrics)); err != nil {
		w.WriteHeader(500)
	}
}
