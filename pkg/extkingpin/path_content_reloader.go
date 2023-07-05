// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extkingpin

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path"
	"path/filepath"
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

// PathContentReloader runs the reloadFunc when it detects that the contents of fileContent have changed.
func PathContentReloader(ctx context.Context, fileContent fileContent, logger log.Logger, reloadFunc func(), debounceTime time.Duration) error {
	filePath, err := filepath.Abs(fileContent.Path())
	if err != nil {
		return errors.Wrap(err, "getting absolute file path")
	}
	if filePath == "" {
		level.Debug(logger).Log("msg", "no path detected for config reload")
	}

	// Check if filePath is symlink
	filePathStat, err := os.Lstat(filePath)
	if err != nil {
		return errors.Wrap(err, "getting file info")
	}
	// Check if filePath's parent folder is symlink
	parentFolder := path.Dir(filePath)
	parentFolderStat, err := os.Lstat(parentFolder)
	if err != nil {
		return errors.Wrap(err, "getting parent folder info")
	}
	var engine reloaderEngine
	if filePathStat.Mode()&os.ModeSymlink != 0 || parentFolderStat.Mode()&os.ModeSymlink != 0 {
		level.Debug(logger).Log("msg", "file and/or its parent folder are symlink, using polling approach", "file", filePath)
		engine = &pollingEngine{
			filePath:   filePath,
			logger:     logger,
			debounce:   debounceTime,
			reloadFunc: reloadFunc,
		}
	} else {
		engine = &fsNotifyEngine{
			filePath:   filePath,
			logger:     logger,
			debounce:   debounceTime,
			reloadFunc: reloadFunc,
		}
	}
	return engine.Start(ctx)
}

// reloaderEngine is an interface that abstracts different underlying logics for reloading a `fileContent`.
type reloaderEngine interface {
	Start(ctx context.Context) error
}

// pollingEngine is an implementation of reloaderEngine that keeps rereading the contents at filePath and when the
// checksum changes it runs the reloadFunc.
type pollingEngine struct {
	filePath         string
	logger           log.Logger
	debounce         time.Duration
	reloadFunc       func()
	previousChecksum [sha256.Size]byte
}

var _ reloaderEngine = &pollingEngine{}

func (p *pollingEngine) Start(ctx context.Context) error {
	configReader := func() {
		// check if file still exists
		if _, err := os.Stat(p.filePath); os.IsNotExist(err) {
			level.Error(p.logger).Log("msg", "file does not exist", "error", err)
			return
		}
		file, err := os.ReadFile(p.filePath)
		if err != nil {
			level.Error(p.logger).Log("msg", "error opening file", "error", err)
			return
		}
		checksum := sha256.Sum256(file)
		if checksum == p.previousChecksum {
			return
		}
		p.reloadFunc()
		p.previousChecksum = checksum
		level.Debug(p.logger).Log("msg", "configuration reloaded")
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(p.debounce):
				configReader()
			}
		}
	}()
	return nil
}

// fsNotifyEngine is an implementation of reloaderEngine that uses fsnotify to watch for changes to a file and then
// runs the reloadFunc.
// A debounce timer can be configured via opts to handle situations where many "write" events are received together or
// a "create" event is followed up by a "write" event, for example. Files will be effectively reloaded at the latest
// after 2 times the debounce timer. By default the debouncer timer is 1 second.
// To ensure renames and deletes are properly handled, the file watcher is put at the file's parent folder. See
// https://github.com/fsnotify/fsnotify/issues/214 for more details.
type fsNotifyEngine struct {
	filePath   string
	logger     log.Logger
	debounce   time.Duration
	reloadFunc func()
}

var _ reloaderEngine = &fsNotifyEngine{}

func (f *fsNotifyEngine) Start(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrap(err, "creating file watcher")
	}
	go func() {
		var reloadTimer *time.Timer
		if f.debounce != 0 {
			reloadTimer = time.AfterFunc(f.debounce, func() {
				f.reloadFunc()
				level.Debug(f.logger).Log("msg", "configuration reloaded after debouncing")
			})
			reloadTimer.Stop()
		}
		defer watcher.Close()
		for {
			select {
			case <-ctx.Done():
				if reloadTimer != nil {
					reloadTimer.Stop()
				}
				return
			case event := <-watcher.Events:
				// fsnotify sometimes sends a bunch of events without name or operation.
				// It's unclear what they are and why they are sent - filter them out.
				if event.Name == "" {
					break
				}
				// We are watching the file's parent folder (more details on this is done can be found below), but are
				// only interested in changed to the target file. Discard every other file as quickly as possible.
				if event.Name != f.filePath {
					break
				}
				// We only react to files being written or created.
				// On chmod or remove we have nothing to do.
				// On rename we have the old file name (not useful). A create event for the new file will come later.
				if event.Op&fsnotify.Write == 0 && event.Op&fsnotify.Create == 0 {
					break
				}
				level.Debug(f.logger).Log("msg", fmt.Sprintf("change detected for %s", f.filePath), "eventName", event.Name, "eventOp", event.Op)
				if reloadTimer != nil {
					reloadTimer.Reset(f.debounce)
				}
			case err := <-watcher.Errors:
				level.Error(f.logger).Log("msg", "watcher error", "error", err)
			}
		}
	}()
	// We watch the file's parent folder and not the file itself to better handle DELETE and RENAME events. Check
	// https://github.com/fsnotify/fsnotify/issues/214 for more details.
	if err := watcher.Add(path.Dir(f.filePath)); err != nil {
		return errors.Wrapf(err, "adding path %s to file watcher", f.filePath)
	}
	return nil
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
