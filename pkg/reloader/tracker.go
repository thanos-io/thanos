// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package reloader

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// prometheusReloadTracker keeps track of the last configuration status.
type prometheusReloadTracker struct {
	runtimeURL *url.URL
	client     http.Client
	logger     log.Logger

	lastReload  time.Time
	lastSuccess bool
}

func (prt *prometheusReloadTracker) getConfigStatus(ctx context.Context) (bool, time.Time, error) {
	r, err := http.NewRequestWithContext(ctx, "GET", prt.runtimeURL.String(), nil)
	if err != nil {
		return false, time.Time{}, err
	}

	resp, err := prt.client.Do(r)
	if err != nil {
		return false, time.Time{}, fmt.Errorf("%s: %w", r.URL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return false, time.Time{}, fmt.Errorf("%s: invalid status code: %d", r.URL.String(), resp.StatusCode)
	}

	var runtimeInfo = struct {
		Status string `json:"status"`
		Data   struct {
			ReloadConfigSuccess bool      `json:"reloadConfigSuccess"`
			LastConfigTime      time.Time `json:"lastConfigTime"`
		} `json:"data"`
	}{}

	if err := json.NewDecoder(resp.Body).Decode(&runtimeInfo); err != nil {
		return false, time.Time{}, fmt.Errorf("invalid response: %w", err)
	}

	if runtimeInfo.Status != "success" {
		return false, time.Time{}, fmt.Errorf("unexpected status: %s", runtimeInfo.Status)
	}

	return runtimeInfo.Data.ReloadConfigSuccess, runtimeInfo.Data.LastConfigTime, nil
}

func (prt *prometheusReloadTracker) preReload(ctx context.Context) error {
	if prt.runtimeURL == nil {
		level.Info(prt.logger).Log("msg", "Pre-reload check skipped because the runtimeInfo URL isn't defined")
		return nil
	}

	success, last, err := prt.getConfigStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to get config status before reload: %w", err)
	}

	level.Debug(prt.logger).Log("msg", "Pre-reload check", "success", success, "last_config_reload", last)
	prt.lastReload = last
	prt.lastSuccess = success

	return nil
}

func (prt *prometheusReloadTracker) postReload(ctx context.Context) error {
	if prt.runtimeURL == nil {
		level.Info(prt.logger).Log("msg", "Post-reload check skipped because the runtimeInfo URL isn't defined")
		return nil
	}

	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		success, last, err := prt.getConfigStatus(ctx)
		if err != nil {
			return fmt.Errorf("failed to get config status after reload: %w", err)
		}
		level.Debug(prt.logger).Log("msg", "Post-reload hook in progress", "config_reload_success", success, "config_reload_success_ts", last)

		// The configuration has been updated successfully.
		if success && last.After(prt.lastReload) {
			level.Debug(prt.logger).Log("msg", "Post-reload hook successful")
			return nil
		}

		// The previous configuration was valid but the current configuration
		// isn't so there's no need to poll again for the status.
		if prt.lastSuccess && !success {
			return fmt.Errorf("configuration reload has failed")
		}

		select {
		case <-ctx.Done():
			result := "failed"
			if success {
				result = "successful"
			}
			return fmt.Errorf("%w: configuration reload %s (last successful reload at %v)", ctx.Err(), result, last)
		case <-t.C:
		}
	}
}
