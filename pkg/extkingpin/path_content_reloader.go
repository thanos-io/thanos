// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extkingpin

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
)

type fileContent interface {
	Content() ([]byte, error)
	Path() string
}

// PathContentReloader starts a file watcher that monitors the file indicated by fileContent.Path() and runs
// reloadFunc whenever a change is detected.
// A debounce timer can be configured via opts to handle situations where many "write" events are received together or
// a "create" event is followed up by a "write" event, for example. Files will be effectively reloaded at the latest
// after 2 times the debounce timer. By default the debouncer timer is 1 second.
// To ensure renames and deletes are properly handled, the file watcher is put at the file's parent folder. See
// https://github.com/fsnotify/fsnotify/issues/214 for more details.
func PathContentReloader(ctx context.Context, fileContent fileContent, logger log.Logger, reloadFunc func(), opts ...reloaderOption) error {
	filePath := fileContent.Path()
	watcher, err := fsnotify.NewWatcher()
	if filePath == "" {
		level.Debug(logger).Log("msg", "no path detected for config reload")
	}
	if err != nil {
		return errors.Wrap(err, "creating file watcher")
	}
	config := &reloaderConfig{
		debounceTime: 1 * time.Second,
	}
	for _, opt := range opts {
		opt(config)
	}

	go func() {
		var reloadTimer *time.Timer
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-watcher.Events:
				// fsnotify sometimes sends a bunch of events without name or operation.
				// It's unclear what they are and why they are sent - filter them out.
				if event.Name == "" {
					break
				}
				// We are watching the file's parent folder (more details on this is done can be found below), but are
				// only interested in changed to the target file. Discard every other file as quickly as possible.
				if path.Base(event.Name) != path.Base(filePath) {
					break
				}
				// Everything but a CHMOD requires rereading.
				// If the file was removed, we can't read it, so skip.
				if event.Op&fsnotify.Chmod == fsnotify.Chmod || event.Op&fsnotify.Remove == fsnotify.Remove {
					break
				}
				level.Debug(logger).Log("msg", fmt.Sprintf("change detected for %s", filePath), "eventName", event.Name, "eventOp", event.Op)
				if reloadTimer != nil {
					reloadTimer.Stop()
				}
				reloadTimer = time.AfterFunc(config.debounceTime, func() {
					reloadFunc()
					level.Debug(logger).Log("msg", "configuration reloaded after debouncing")
				})
			case err := <-watcher.Errors:
				level.Error(logger).Log("msg", "watcher error", "error", err)
			}
		}
	}()
	// We watch the file's parent folder and not the file itself to better handle DELETE and RENAME events. Check
	// https://github.com/fsnotify/fsnotify/issues/214 for more details.
	if err := watcher.Add(path.Dir(filePath)); err != nil {
		return errors.Wrapf(err, "adding path %s to file watcher", filePath)
	}
	return nil
}

func withinDebounceTime(lastUpdate time.Time, debounceTime time.Duration) bool {
	return time.Now().Before(lastUpdate.Add(debounceTime))
}

type reloaderConfig struct {
	debounceTime time.Duration
}

type reloaderOption func(cfg *reloaderConfig)

func WithDebounceTime(debounceTime time.Duration) func(cfg *reloaderConfig) {
	return func(cfg *reloaderConfig) {
		cfg.debounceTime = debounceTime
	}
}

type staticPathContent struct {
	content []byte
	path    string
}

var _ fileContent = (*staticPathContent)(nil)

// Content returns the cached content.
func (t *staticPathContent) Content() ([]byte, error) {
	return t.content, nil
}

// Path returns the path to the file that contains the content.
func (t *staticPathContent) Path() string {
	return t.path
}

// NewStaticPathContent creates a new content that can be used to serve a static configuration. It copies the
// configuration from `fromPath` into `destPath` to avoid confusion with file watchers.
func NewStaticPathContent(fromPath string) (*staticPathContent, error) {
	content, err := os.ReadFile(fromPath)
	if err != nil {
		return nil, errors.Wrapf(err, "could not load test content: %s", fromPath)
	}
	return &staticPathContent{content, fromPath}, nil
}

// Rewrite rewrites the file backing this staticPathContent and swaps the local content cache. The file writing
// is needed to trigger the file system monitor.
func (t *staticPathContent) Rewrite(newContent []byte) error {
	t.content = newContent
	// Write the file to ensure possible file watcher reloaders get triggered.
	return os.WriteFile(t.path, newContent, 0666)
}
