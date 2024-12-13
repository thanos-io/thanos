// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exthttp

import (
	"fmt"
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
	Enabled  bool    `yaml:"enabled"`
	UpTo     uint    `yaml:"up_to"`
	Quantile float64 `yaml:"quantile"`
}

func DefaultCustomBucketConfig() CustomBucketConfig {
	return CustomBucketConfig{
		HedgingConfig: HedgingConfig{
			Enabled:  false,
			UpTo:     3,
			Quantile: 0.9,
		},
	}
}

type hedgingRoundTripper struct {
	Transport http.RoundTripper
	TDigest   *tdigest.TDigest
	mu        sync.RWMutex
	config    HedgingConfig
}

func (hrt *hedgingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := hrt.Transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	duration := float64(time.Since(start).Milliseconds())
	hrt.mu.Lock()
	defer hrt.mu.Unlock()
	err = hrt.TDigest.Add(duration)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func (hrt *hedgingRoundTripper) nextFn() (int, time.Duration) {
	hrt.mu.RLock()
	defer hrt.mu.RUnlock()

	delayMs := hrt.TDigest.Quantile(hrt.config.Quantile)
	delay := time.Duration(delayMs) * time.Millisecond
	upto := int(hrt.config.UpTo)
	return upto, delay
}

func CreateHedgedTransportWithConfig(config CustomBucketConfig) func(rt http.RoundTripper) http.RoundTripper {
	if !config.HedgingConfig.Enabled {
		return func(rt http.RoundTripper) http.RoundTripper {
			return rt
		}
	}
	return func(rt http.RoundTripper) http.RoundTripper {
		td, err := tdigest.New()
		if err != nil {
			panic(fmt.Sprintf("BUG: Failed to initialize T-Digest: %v", err))
		}
		hrt := &hedgingRoundTripper{
			Transport: rt,
			TDigest:   td,
			config:    config.HedgingConfig,
		}
		cfg := hedgedhttp.Config{
			Transport: hrt,
			Upto:      int(config.HedgingConfig.UpTo),
			Next:      hrt.nextFn,
		}
		hedgedrt, err := hedgedhttp.New(cfg)
		if err != nil {
			panic(fmt.Sprintf("BUG: Failed to create hedged transport: %v", err))
		}
		return hedgedrt
	}
}
