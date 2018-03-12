// Package reloader contains helpers to trigger reloads of Prometheus instances
// on configuration changes and to substitude environment variables in config files.
package reloader

import (
	"bytes"
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
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/pkg/errors"
	fsnotify "gopkg.in/fsnotify.v1"
)

// Reloader can watch config files and trigger reloads of a Prometheus server.
// It optionally substitutes environment variables in the configuration.
// Referenced environment variables must be of the form `$(var)` (not `$var` or `${var}`).
type Reloader struct {
	logger       log.Logger
	reloadURL    *url.URL
	retryInteval time.Duration
}

// New creates a new reloader that watches the given config file and rule directory
// and triggers a Prometheus reload upon changes.
// If cfgEnvsubstFile is not empty, environment variables in the config file will be
// substituted and the out put written into the given path. Prometheus should then
// use cfgEnvsubstFile as its config file path.
func New(logger log.Logger, reloadURL *url.URL) *Reloader {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Reloader{
		logger:       logger,
		reloadURL:    reloadURL,
		retryInteval: 5 * time.Second,
	}
}

// Watch starts to watch the config file and processes it until the context
// gets canceled.
func (r *Reloader) WatchConfig(ctx context.Context, cfgFile string, cfgEnvsubstFile string) error {
	if cfgFile == "" {
		return errors.New("empty input config file path")
	}

	configWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrap(err, "create watcher")
	}
	defer configWatcher.Close()

	if err := configWatcher.Add(cfgFile); err != nil {
		return errors.Wrap(err, "add config file watch")
	}
	level.Info(r.logger).Log(
		"msg", "started watching config file for changes",
		"in", cfgFile,
		"out", cfgEnvsubstFile)

	var lastHash []byte
	initial := make(chan struct{}, 1)
	initial <- struct{}{}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-initial:
		case event := <-configWatcher.Events:
			level.Debug(r.logger).Log("msg", "received watch event", "op", event.Op, "name", event.Name)

			if event.Name != cfgFile {
				continue
			}
		case err := <-configWatcher.Errors:
			level.Error(r.logger).Log("msg", "watch error", "err", err)
			continue
		}

		h := sha256.New()
		if err := hashFile(h, cfgFile); err != nil {
			return errors.Wrap(err, "hash file")
		}

		hb := h.Sum(nil)
		if bytes.Equal(lastHash, hb) {
			// Nothing to do.
			return nil
		}

		if cfgEnvsubstFile != "" {
			b, err := ioutil.ReadFile(cfgFile)
			if err != nil {
				return errors.Wrap(err, "read file")
			}

			b, err = expandEnv(b)
			if err != nil {
				return errors.Wrap(err, "expand environment variables")
			}

			if err := ioutil.WriteFile(cfgEnvsubstFile, b, 0666); err != nil {
				return errors.Wrap(err, "write file")
			}
		}

		// Retry trigger reload until it suceeddes.
		err := runutil.RetryWithLog(r.logger, r.retryInteval, ctx.Done(), func() error {
			if err := r.triggerReload(ctx); err != nil {
				return errors.Wrap(err, "trigger config reload")
			}
			return nil
		})
		if err != nil {
			return err
		}

		lastHash = hb
		level.Info(r.logger).Log(
			"msg", "config file refreshed",
			"in", cfgFile,
			"out", cfgEnvsubstFile)
	}
}

func (r *Reloader) WatchRules(ctx context.Context, ruleDir string, ruleInterval time.Duration) error {
	if ruleDir == "" {
		return errors.New("empty rule dir")
	}

	tick := time.NewTicker(ruleInterval)
	defer tick.Stop()

	var lastHash []byte
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}

		err := runutil.RetryWithLog(r.logger, r.retryInteval, ctx.Done(), func() error {
			h := sha256.New()
			err := filepath.Walk(ruleDir, func(path string, f os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				if f.IsDir() {
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

			hb := h.Sum(nil)
			if bytes.Equal(hb, lastHash) {
				return nil
			}

			if err := r.triggerReload(ctx); err != nil {
				return errors.Wrap(err, "trigger rule reload")
			}
			lastHash = hb
			level.Info(r.logger).Log(
				"msg", "rule files refreshed",
				"ruleDir", ruleDir)

			return nil
		})
		if err != nil {
			return err
		}
	}
}

func hashFile(h hash.Hash, fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return err
	}
	h.Write([]byte{'\xff'})
	h.Write([]byte(fn))
	h.Write([]byte{'\xff'})

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
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.Errorf("received non-200 response: %s", resp.Status)
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
