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
	"github.com/pkg/errors"
	fsnotify "gopkg.in/fsnotify.v1"
)

// Reloader can watch config files and trigger reloads of a Prometheus server.
// It optionally substitutes environment variables in the configuration.
// Referenced environment variables must be of the form `$(var)` (not `$var` or `${var}`).
type Reloader struct {
	logger           log.Logger
	promURL          *url.URL
	cfgFilename      string
	cfgSubstFilename string
	ruleDir          string
	lastCfgHash      []byte
	lastRulesHash    []byte
}

// New creates a new reloader that watches the given config file and rule directory
// and triggers a Prometheus reload upon changes.
// If cfgEnvsubstFile is not empty, environment variables in the config file will be
// substituted and the out put written into the given path. Promethes should then
// use cfgEnvsubstFile as its config file path.
func New(logger log.Logger, promURL *url.URL, cfgFile, cfgEnvsubstFile, ruleDir string) *Reloader {
	return &Reloader{
		logger:           logger,
		promURL:          promURL,
		cfgFilename:      cfgFile,
		cfgSubstFilename: cfgEnvsubstFile,
	}
}

// Watch starts to watch the config file and processes it until the context
// gets canceled.
func (r *Reloader) Watch(ctx context.Context) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrap(err, "create watcher")
	}
	defer w.Close()

	cfgDir := filepath.Dir(r.cfgFilename)

	if err := w.Add(cfgDir); err != nil {
		return errors.Wrap(err, "add config file directory watch")
	}
	if _, err := r.applyConfig(); err != nil {
		return errors.Wrap(err, "initial apply")
	}
	if err := r.triggerReload(ctx); err != nil {
		level.Error(r.logger).Log("msg", "triggering reload failed", "err", err)
	}

	tick := time.NewTicker(3 * time.Minute)
	defer tick.Stop()

	for {
		select {
		case event := <-w.Events:
			level.Debug(r.logger).Log("msg", "received watch event", "op", event.Op, "name", event.Name)

			if event.Name != r.cfgFilename {
				continue
			}
			changes, err := r.applyConfig()
			if err != nil {
				level.Error(r.logger).Log("msg", "apply failed", "err", err)
			}
			if !changes {
				continue
			}
			if err := r.triggerReload(ctx); err != nil {
				level.Error(r.logger).Log("msg", "triggering reload failed", "err", err)
			}

		case err := <-w.Errors:
			level.Error(r.logger).Log("msg", "watch error", "err", err)

		case <-tick.C:
			changes, err := r.refreshRules()
			if err != nil {
				level.Error(r.logger).Log("msg", "refreshing rules failed", "err", err)
			}
			if !changes {
				continue
			}
			if err := r.triggerReload(ctx); err != nil {
				level.Error(r.logger).Log("msg", "triggering reload failed", "err", err)
			}

		case <-ctx.Done():
			return nil
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

func (r *Reloader) applyConfig() (ok bool, err error) {
	if r.cfgFilename == "" {
		return false, nil
	}
	h := sha256.New()

	if err := hashFile(h, r.cfgFilename); err != nil {
		return false, errors.Wrap(err, "hash file")
	}
	hb := h.Sum(nil)

	if bytes.Equal(r.lastCfgHash, hb) {
		return false, nil
	}
	r.lastCfgHash = hb

	if r.cfgSubstFilename == "" {
		return true, nil
	}

	b, err := ioutil.ReadFile(r.cfgFilename)
	if err != nil {
		return false, errors.Wrap(err, "read file")
	}
	b, err = expandEnv(b)
	if err != nil {
		return false, errors.Wrap(err, "expand environment variables")
	}

	if err := ioutil.WriteFile(r.cfgSubstFilename, b, 0666); err != nil {
		return false, errors.Wrap(err, "write file")
	}
	return true, nil
}

func (r *Reloader) refreshRules() (bool, error) {
	if r.ruleDir == "" {
		return false, nil
	}
	h := sha256.New()

	err := filepath.Walk(r.ruleDir, func(path string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if err := hashFile(h, path); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return false, errors.Wrap(err, "build hash")
	}
	hb := h.Sum(nil)

	if bytes.Equal(hb, r.lastRulesHash) {
		return false, nil
	}
	r.lastRulesHash = hb

	return true, nil
}

func (r *Reloader) triggerReload(ctx context.Context) error {
	req, err := http.NewRequest("POST", r.promURL.String(), nil)
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
