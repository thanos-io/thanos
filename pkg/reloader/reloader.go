// Package reloader contains helpers to trigger reloads of Prometheus instances
// on configuration changes and to substitute environment variables in config files.
//
// Reloader type is useful when you want to:
//
// 	* Watch on changes against certain file e.g (`cfgFile`) .
// 	* Optionally, specify different different output file for watched `cfgFile` (`cfgOutputFile`).
// 	This will also try decompress the `cfgFile` if needed and substitute ALL the envvars using Kubernetes substitution format: (`$(var)`)
// 	* Watch on changes against certain directories (`ruleDires`).
//
// Once any of those two changes Prometheus on given `reloadURL` will be notified, causing Prometheus to reload configuration and rules.
//
// This and below for reloader:
//
// 	u, _ := url.Parse("http://localhost:9090")
// 	rl := reloader.New(
// 		nil,
// 		reloader.ReloadURLFromBase(u),
// 		"/path/to/cfg",
// 		"/path/to/cfg.out",
// 		[]string{"/path/to/dirs"},
// 	)
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
// By default, reloader will make a schedule to check the given config files and dirs of sum of hash with the last result,
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
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/pkg/errors"
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
	ruleInterval  time.Duration
	retryInterval time.Duration

	lastCfgHash  []byte
	lastRuleHash []byte
}

var firstGzipBytes = []byte{0x1f, 0x8b, 0x08}

// New creates a new reloader that watches the given config file and rule directory
// and triggers a Prometheus reload upon changes.
// If cfgOutputFile is not empty the config file will be decompressed if needed, environment variables
// will be substituted and the output written into the given path. Prometheus should then use
// cfgOutputFile as its config file path.
func New(logger log.Logger, reloadURL *url.URL, cfgFile string, cfgOutputFile string, ruleDirs []string) *Reloader {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Reloader{
		logger:        logger,
		reloadURL:     reloadURL,
		cfgFile:       cfgFile,
		cfgOutputFile: cfgOutputFile,
		ruleDirs:      ruleDirs,
		ruleInterval:  3 * time.Minute,
		retryInterval: 5 * time.Second,
	}
}

// Watch starts to watch the config file and rules and process them until the context
// gets canceled. Config file gets env expanded if cfgOutputFile is specified and reload is trigger if
// config or rules changed.
func (r *Reloader) Watch(ctx context.Context) error {
	configWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrap(err, "create watcher")
	}
	defer runutil.CloseWithLogOnErr(r.logger, configWatcher, "config watcher close")

	if r.cfgFile != "" {
		if err := configWatcher.Add(r.cfgFile); err != nil {
			return errors.Wrap(err, "add config file watch")
		}
		level.Info(r.logger).Log(
			"msg", "started watching config file for changes",
			"in", r.cfgFile,
			"out", r.cfgOutputFile)

		err := r.apply(ctx)
		if err != nil {
			return err
		}
	}

	tick := time.NewTicker(r.ruleInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		case event := <-configWatcher.Events:
			if event.Name != r.cfgFile {
				continue
			}
		case err := <-configWatcher.Errors:
			level.Error(r.logger).Log("msg", "watch error", "err", err)
			continue
		}

		err := r.apply(ctx)
		if err != nil {
			// Critical error.
			return err
		}
	}
}

// apply triggers Prometheus reload if rules or config changed. If cfgOutputFile is set, we also
// expand env vars into config file before reloading.
// Reload is retried in retryInterval until ruleInterval.
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

			// detect and extract gzipped file
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

			if err := ioutil.WriteFile(r.cfgOutputFile, b, 0666); err != nil {
				return errors.Wrap(err, "write file")
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

			// filepath.Walk uses Lstat to retriev os.FileInfo. Lstat does not
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
	retryCtx, cancel := context.WithTimeout(ctx, r.ruleInterval)
	err := runutil.RetryWithLog(r.logger, r.retryInterval, retryCtx.Done(), func() error {
		if err := r.triggerReload(ctx); err != nil {
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
	})
	cancel()
	if err != nil {
		level.Error(r.logger).Log("msg", "Failed to trigger reload. Retrying.", "err", err)
	}

	return nil
}

func hashFile(h hash.Hash, fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return err
	}

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
	defer runutil.CloseWithLogOnErr(r.logger, resp.Body, "trigger reload resp body")

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
