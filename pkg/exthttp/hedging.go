// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

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
	UpTo     uint    `yaml:"up_to"`
	Quantile float64 `yaml:"quantile"`
}

var CustomBktConfig = CustomBucketConfig{
	HedgingConfig: HedgingConfig{
		Quantile: 0.9,
	},
}

type hedgingRoundTripper struct {
	Transport http.RoundTripper
	TDigest   *tdigest.TDigest
	mu        sync.RWMutex
}

func (hrt *hedgingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := hrt.Transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	duration := float64(time.Since(start).Milliseconds())
	hrt.mu.Lock()
	err = hrt.TDigest.Add(duration)
	if err != nil {
		return nil, err
	}
	hrt.mu.Unlock()
	return resp, err
}

func (hrt *hedgingRoundTripper) nextFn() (int, time.Duration) {
	hrt.mu.Lock()
	defer hrt.mu.Unlock()

	delayMs := hrt.TDigest.Quantile(CustomBktConfig.HedgingConfig.Quantile)
	delay := time.Duration(delayMs) * time.Millisecond
	upto := int(CustomBktConfig.HedgingConfig.UpTo)
	return upto, delay
}

func WrapHedgedRoundTripper(rt http.RoundTripper) http.RoundTripper {
	td, err := tdigest.New()
	if err != nil {
		panic("Failed to initialize T-Digest")
	}
	hrt := &hedgingRoundTripper{
		Transport: rt,
		TDigest:   td,
	}
	cfg := hedgedhttp.Config{
		Transport: hrt,
		Upto:      int(CustomBktConfig.HedgingConfig.UpTo),
		Next:      hrt.nextFn,
	}
	hedgedrt, err := hedgedhttp.New(cfg)
	if err != nil {
		panic("Failed to create hedged transport")
	}
	return hedgedrt
}