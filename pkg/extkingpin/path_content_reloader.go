// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extkingpin

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"sync"
	"time"

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

	engine := &pollingEngine{
		filePath:   filePath,
		logger:     logger,
		debounce:   debounceTime,
		reloadFunc: reloadFunc,
	}
	return engine.start(ctx)
}

// pollingEngine keeps rereading the contents at filePath and when its checksum changes it runs the reloadFunc.
type pollingEngine struct {
	filePath         string
	logger           log.Logger
	debounce         time.Duration
	reloadFunc       func()
	previousChecksum [sha256.Size]byte
}

func (p *pollingEngine) start(ctx context.Context) error {
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
		level.Debug(p.logger).Log("msg", "configuration reloaded", "path", p.filePath)
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

type staticPathContent struct {
	contentMtx sync.Mutex
	content    []byte
	path       string
}

var _ fileContent = (*staticPathContent)(nil)

// Content returns the cached content.
func (t *staticPathContent) Content() ([]byte, error) {
	t.contentMtx.Lock()
	defer t.contentMtx.Unlock()

	c := make([]byte, 0, len(t.content))
	c = append(c, t.content...)

	return c, nil
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
	return &staticPathContent{content: content, path: fromPath}, nil
}

// Rewrite rewrites the file backing this staticPathContent and swaps the local content cache. The file writing
// is needed to trigger the file system monitor.
func (t *staticPathContent) Rewrite(newContent []byte) error {
	t.contentMtx.Lock()
	defer t.contentMtx.Unlock()

	t.content = newContent
	// Write the file to ensure possible file watcher reloaders get triggered.
	return os.WriteFile(t.path, newContent, 0666)
}
