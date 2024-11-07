package exthttp

import (
	"net/http"
	"sync"
	"time"

	"github.com/caio/go-tdigest"
	"github.com/cristalhq/hedgedhttp"
)

type hedgedOptions struct {
	maxHedgedRequests uint `yaml:"max_hedged_requests"`
}

var HedgedOptions hedgedOptions

type durationRoundTripper struct {
	Transport http.RoundTripper
	TDigest   *tdigest.TDigest
	mu        sync.Mutex
}

func (drt *durationRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := drt.Transport.RoundTrip(req)
	duration := time.Since(start).Seconds()
	drt.mu.Lock()
	drt.TDigest.Add(duration)
	drt.mu.Unlock()
	return resp, err
}

func (drt *durationRoundTripper) nextFn() (int, time.Duration) {
	drt.mu.Lock()
	defer drt.mu.Unlock()

	delaySec := drt.TDigest.Quantile(0.9)
	delay := time.Duration(delaySec * float64(time.Second))
	upto := int(HedgedOptions.maxHedgedRequests)
	return upto, delay
}

func WrapHedgedRoundTripper(rt http.RoundTripper) http.RoundTripper {
	td, err := tdigest.New()
	if err != nil {
		panic("Failed to initialize T-Digest")
	}
	drt := &durationRoundTripper{
		Transport: rt,
		TDigest:   td,
	}
	cfg := hedgedhttp.Config{
		Transport: drt,
		Upto:      int(HedgedOptions.maxHedgedRequests),
		Next:      drt.nextFn,
	}
	hedgedrt, err := hedgedhttp.New(cfg)
	if err != nil {
		panic("Failed to create hedged transport")
	}
	return hedgedrt
}
