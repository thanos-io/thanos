// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package reloader contains helpers to trigger reloads of Prometheus instances
// on configuration changes and to substitute environment variables in config files.
//
// Reloader type is useful when you want to:
//
// 	* Watch on changes against certain file e.g (`cfgFile`).
// 	* Optionally, specify different different output file for watched `cfgFile` (`cfgOutputFile`).
// 	This will also try decompress the `cfgFile` if needed and substitute ALL the envvars using Kubernetes substitution format: (`$(var)`)
// 	* Watch on changes against certain directories (`ruleDires`).
//
// Once any of those two changes Prometheus on given `reloadURL` will be notified, causing Prometheus to reload configuration and rules.
//
// This and below for reloader:
//
// 	u, _ := url.Parse("http://localhost:9090")
// 	rl := reloader.New(nil, nil, &reloader.Options{
// 		ReloadURL:     reloader.ReloadURLFromBase(u),
// 		CfgFile:       "/path/to/cfg",
// 		CfgOutputFile: "/path/to/cfg.out",
// 		RuleDirs:      []string{"/path/to/dirs"},
// 		WatchInterval: 3 * time.Minute,
// 		RetryInterval: 5 * time.Second,
//  })
//
// The url of reloads can be generated with function ReloadURLFromBase().
// It will append the default path of reload into the given url:
//
// 	u, _ := url.Parse("http://localhost:9090")
// 	reloader.ReloadURLFromBase(u) // It will return "http://localhost:9090/-/reload"
//
// Start watching changes and stopped until the context gets canceled:
//
// 	ctx, cancel := context.WithCancel(context.Background())
// 	go func() {
// 		if err := rl.Watch(ctx); err != nil {
// 			log.Fatal(err)
// 		}
// 	}()
// 	// ...
// 	cancel()
//
// Reloader will make a schedule to check the given config files and dirs of sum of hash with the last result,
// even if it is no changes.
//
// A basic example of configuration template with environment variables:
//
//   global:
//     external_labels:
//       replica: '$(HOSTNAME)'
package reloader

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// Reloader can watch config files and trigger reloads of a Prometheus server.
// It optionally substitutes environment variables in the configuration.
// Referenced environment variables must be of the form `$(var)` (not `$var` or `${var}`).
type Reloader struct {
	logger        log.Logger
	reloadURL     *url.URL
	cfgFile       string
	cfgOutputFile string
	ruleDirs      []string
	watchInterval time.Duration
	retryInterval time.Duration

	lastCfgHash  []byte
	lastRuleHash []byte

	reloads      prometheus.Counter
	reloadErrors prometheus.Counter
	watches      prometheus.Gauge
	watchEvents  prometheus.Counter
	watchErrors  prometheus.Counter
	configErrors prometheus.Counter
}

// Options bundles options for the Reloader.
type Options struct {
	// ReloadURL is a prometheus URL to trigger reloads.
	ReloadURL *url.URL
	// CfgFile is a path to the prometheus config file to watch.
	CfgFile string
	// CfgOutputFile is a path for the output config file.
	// If cfgOutputFile is not empty the config file will be decompressed if needed, environment variables
	// will be substituted and the output written into the given path. Prometheus should then use
	// cfgOutputFile as its config file path.
	CfgOutputFile string
	// RuleDirs is a collection of paths for this reloader to watch over.
	RuleDirs []string
	// WatchInterval controls how often reloader re-reads config and rules.
	WatchInterval time.Duration
	// RetryInterval controls how often reloader retries config reload in case of error.
	RetryInterval time.Duration
}

var firstGzipBytes = []byte{0x1f, 0x8b, 0x08}

// New creates a new reloader that watches the given config file and rule directory
// and triggers a Prometheus reload upon changes.
func New(logger log.Logger, reg prometheus.Registerer, o *Options) *Reloader {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	r := &Reloader{
		logger:        logger,
		reloadURL:     o.ReloadURL,
		cfgFile:       o.CfgFile,
		cfgOutputFile: o.CfgOutputFile,
		ruleDirs:      o.RuleDirs,
		watchInterval: o.WatchInterval,
		retryInterval: o.RetryInterval,

		reloads: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "reloader_reloads_total",
				Help: "Total number of reload requests.",
			},
		),
		reloadErrors: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "reloader_reloads_failed_total",
				Help: "Total number of reload requests that failed.",
			},
		),
		configErrors: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "reloader_config_apply_errors_total",
				Help: "Total number of config applies that failed.",
			},
		),
		watches: promauto.With(reg).NewGauge(
			prometheus.GaugeOpts{
				Name: "reloader_watches",
				Help: "Number of resources watched by the reloader.",
			},
		),
		watchEvents: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "reloader_watch_events_total",
				Help: "Total number of events received by the reloader from the watcher.",
			},
		),
		watchErrors: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "reloader_watch_errors_total",
				Help: "Total number of errors received by the reloader from the watcher.",
			},
		),
	}
	return r
}

// We cannot detect everything via watch. Watch interval controls how often we re-read given dirs non-recursively.
func (r *Reloader) WithWatchInterval(duration time.Duration) {
	r.watchInterval = duration
}

// Watch starts to watch periodically the config file and rules and process them until the context
// gets canceled. Config file gets env expanded if cfgOutputFile is specified and reload is trigger if
// config or rules changed.
// Watch watchers periodically based on r.watchInterval.
// For config file it watches it directly as well via fsnotify.
// It watches rule dirs as well, but lot's of edge cases are missing, so rely on interval mostly.
func (r *Reloader) Watch(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrap(err, "create watcher")
	}
	defer runutil.CloseWithLogOnErr(r.logger, watcher, "config watcher close")

	watchables := map[string]struct{}{}
	if r.cfgFile != "" {
		watchables[filepath.Dir(r.cfgFile)] = struct{}{}
		if err := watcher.Add(r.cfgFile); err != nil {
			return errors.Wrapf(err, "add config file %s to watcher", r.cfgFile)
		}

		if err := r.apply(ctx); err != nil {
			return err
		}
	}

	// Watch rule dirs in best effort manner.
	for _, ruleDir := range r.ruleDirs {
		watchables[filepath.Dir(ruleDir)] = struct{}{}
		if err := watcher.Add(ruleDir); err != nil {
			return errors.Wrapf(err, "add rule dir %s to watcher", ruleDir)
		}
	}

	tick := time.NewTicker(r.watchInterval)
	defer tick.Stop()

	r.watches.Set(float64(len(watchables)))
	level.Info(r.logger).Log(
		"msg", "started watching config file and recursively rule dirs for changes",
		"cfg", r.cfgFile,
		"out", r.cfgOutputFile,
		"dirs", strings.Join(r.ruleDirs, ","))

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		case event := <-watcher.Events:
			r.watchEvents.Inc()
			if _, ok := watchables[filepath.Dir(event.Name)]; !ok {
				continue
			}
		case err := <-watcher.Errors:
			r.watchErrors.Inc()
			level.Error(r.logger).Log("msg", "watch error", "err", err)
			continue
		}

		if err := r.apply(ctx); err != nil {
			r.configErrors.Inc()
			level.Error(r.logger).Log("msg", "apply error", "err", err)
		}
	}
}

// apply triggers Prometheus reload if rules or config changed. If cfgOutputFile is set, we also
// expand env vars into config file before reloading.
// Reload is retried in retryInterval until watchInterval.
func (r *Reloader) apply(ctx context.Context) error {
	var (
		cfgHash  []byte
		ruleHash []byte
	)
	if r.cfgFile != "" {
		h := sha256.New()
		if err := hashFile(h, r.cfgFile); err != nil {
			return errors.Wrap(err, "hash file")
		}
		cfgHash = h.Sum(nil)
		if r.cfgOutputFile != "" {
			b, err := ioutil.ReadFile(r.cfgFile)
			if err != nil {
				return errors.Wrap(err, "read file")
			}

			// Detect and extract gzipped file.
			if bytes.Equal(b[0:3], firstGzipBytes) {
				zr, err := gzip.NewReader(bytes.NewReader(b))
				if err != nil {
					return errors.Wrap(err, "create gzip reader")
				}
				defer runutil.CloseWithLogOnErr(r.logger, zr, "gzip reader close")

				b, err = ioutil.ReadAll(zr)
				if err != nil {
					return errors.Wrap(err, "read compressed config file")
				}
			}

			b, err = expandEnv(b)
			if err != nil {
				return errors.Wrap(err, "expand environment variables")
			}

			tmpFile := r.cfgOutputFile + ".tmp"
			defer func() {
				_ = os.Remove(tmpFile)
			}()
			if err := ioutil.WriteFile(tmpFile, b, 0666); err != nil {
				return errors.Wrap(err, "write file")
			}
			if err := os.Rename(tmpFile, r.cfgOutputFile); err != nil {
				return errors.Wrap(err, "rename file")
			}
		}
	}

	h := sha256.New()
	for _, ruleDir := range r.ruleDirs {
		walkDir, err := filepath.EvalSymlinks(ruleDir)
		if err != nil {
			return errors.Wrap(err, "ruleDir symlink eval")
		}
		err = filepath.Walk(walkDir, func(path string, f os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// filepath.Walk uses Lstat to retrieve os.FileInfo. Lstat does not
			// follow symlinks. Make sure to follow a symlink before checking
			// if it is a directory.
			targetFile, err := os.Stat(path)
			if err != nil {
				return err
			}

			if targetFile.IsDir() {
				return nil
			}

			if err := hashFile(h, path); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "build hash")
		}
	}
	if len(r.ruleDirs) > 0 {
		ruleHash = h.Sum(nil)
	}

	if bytes.Equal(r.lastCfgHash, cfgHash) && bytes.Equal(r.lastRuleHash, ruleHash) {
		// Nothing to do.
		return nil
	}

	// Retry trigger reload until it succeeded or next tick is near.
	retryCtx, cancel := context.WithTimeout(ctx, r.watchInterval)
	defer cancel()

	if err := runutil.RetryWithLog(r.logger, r.retryInterval, retryCtx.Done(), func() error {
		r.reloads.Inc()
		if err := r.triggerReload(ctx); err != nil {
			r.reloadErrors.Inc()
			return errors.Wrap(err, "trigger reload")
		}

		r.lastCfgHash = cfgHash
		r.lastRuleHash = ruleHash
		level.Info(r.logger).Log(
			"msg", "Prometheus reload triggered",
			"cfg_in", r.cfgFile,
			"cfg_out", r.cfgOutputFile,
			"rule_dirs", strings.Join(r.ruleDirs, ", "))
		return nil
	}); err != nil {
		level.Error(r.logger).Log("msg", "Failed to trigger reload. Retrying.", "err", err)
	}

	return nil
}

func hashFile(h hash.Hash, fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := h.Write([]byte{'\xff'}); err != nil {
		return err
	}
	if _, err := h.Write([]byte(fn)); err != nil {
		return err
	}
	if _, err := h.Write([]byte{'\xff'}); err != nil {
		return err
	}

	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	return nil
}

func (r *Reloader) triggerReload(ctx context.Context) error {
	req, err := http.NewRequest("POST", r.reloadURL.String(), nil)
	if err != nil {
		return errors.Wrap(err, "create request")
	}
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "reload request failed")
	}
	defer runutil.ExhaustCloseWithLogOnErr(r.logger, resp.Body, "trigger reload resp body")

	if resp.StatusCode != 200 {
		return errors.Errorf("received non-200 response: %s; have you set `--web.enable-lifecycle` Prometheus flag?", resp.Status)
	}
	return nil
}

// ReloadURLFromBase returns the standard Prometheus reload URL from its base URL.
func ReloadURLFromBase(u *url.URL) *url.URL {
	r := *u
	r.Path = path.Join(r.Path, "/-/reload")
	return &r
}

var envRe = regexp.MustCompile(`\$\(([a-zA-Z_0-9]+)\)`)

func expandEnv(b []byte) (r []byte, err error) {
	r = envRe.ReplaceAllFunc(b, func(n []byte) []byte {
		if err != nil {
			return nil
		}
		n = n[2 : len(n)-1]

		v, ok := os.LookupEnv(string(n))
		if !ok {
			err = errors.Errorf("found reference to unset environment variable %q", n)
			return nil
		}
		return []byte(v)
	})
	return r, err
}
