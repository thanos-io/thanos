package exthttp

import (
	"net/http"
	"sync"
	"time"

	"github.com/caio/go-tdigest"
	"github.com/cristalhq/hedgedhttp"
)

type CustomBucketConfig struct {
	HedgingConfig HedgingConfig `yaml:"hedging_config"`
}

type HedgingConfig struct {
	UpTo uint `yaml:"up_to"`
}

var HedgedOptions CustomBucketConfig

type durationRoundTripper struct {
	Transport http.RoundTripper
	TDigest   *tdigest.TDigest
	mu        sync.Mutex
}

func (drt *durationRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := drt.Transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	duration := time.Since(start).Seconds()
	drt.mu.Lock()
	err = drt.TDigest.Add(duration)
	if err != nil {
		return nil, err
	}
	drt.mu.Unlock()
	return resp, err
}

func (drt *durationRoundTripper) nextFn() (int, time.Duration) {
	drt.mu.Lock()
	defer drt.mu.Unlock()

	delaySec := drt.TDigest.Quantile(0.9)
	delay := time.Duration(delaySec * float64(time.Second))
	upto := int(HedgedOptions.HedgingConfig.UpTo)
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
		Upto:      int(HedgedOptions.HedgingConfig.UpTo),
		Next:      drt.nextFn,
	}
	hedgedrt, err := hedgedhttp.New(cfg)
	if err != nil {
		panic("Failed to create hedged transport")
	}
	return hedgedrt
}
