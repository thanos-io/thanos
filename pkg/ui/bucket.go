// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package ui

import (
	"context"
	"encoding/json"
	"html/template"
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// Bucket is a web UI representing state of buckets as a timeline.
type Bucket struct {
	*BaseUI
	flagsMap map[string]string
	// Unique Prometheus label that identifies each shard, used as the title. If
	// not present, all labels are displayed externally as a legend.
	Label       string
	Blocks      template.JS
	RefreshedAt time.Time
	Err         error
}

func NewBucketUI(logger log.Logger, label string, flagsMap map[string]string) *Bucket {
	return &Bucket{
		BaseUI:   NewBaseUI(log.With(logger, "component", "bucketUI"), "bucket_menu.html", queryTmplFuncs()),
		Blocks:   "[]",
		Label:    label,
		flagsMap: flagsMap,
	}
}

// Register registers http routes for bucket UI.
func (b *Bucket) Register(r *route.Router, ins extpromhttp.InstrumentationMiddleware) {
	instrf := func(name string, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
		return ins.NewHandler(name, http.HandlerFunc(next))
	}

	r.Get("/", instrf("root", b.root))
	r.Get("/static/*filepath", instrf("static", b.serveStaticAsset))
}

// Handle / of bucket UIs.
func (b *Bucket) root(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(b.logger, b.flagsMap, r)
	b.executeTemplate(w, "bucket.html", prefix, b)
}

func (b *Bucket) Set(data string, err error) {
	b.RefreshedAt = time.Now()
	b.Blocks = template.JS(data)
	b.Err = err
}

// RunRefreshLoop refreshes periodically metadata from remote storage periodically and update the UI.
func (b *Bucket) RunRefreshLoop(ctx context.Context, fetcher block.MetadataFetcher, interval time.Duration, loopTimeout time.Duration) error {
	return runutil.Repeat(interval, ctx.Done(), func() error {
		return runutil.RetryWithLog(b.logger, time.Minute, ctx.Done(), func() error {
			iterCtx, iterCancel := context.WithTimeout(ctx, loopTimeout)
			defer iterCancel()

			level.Debug(b.logger).Log("msg", "synchronizing block metadata")
			metas, _, err := fetcher.Fetch(iterCtx)
			if err != nil {
				level.Error(b.logger).Log("msg", "failed to sync metas", "err", err)
				b.Set("[]", err)
				return err
			}
			blocks := make([]metadata.Meta, 0, len(metas))
			for _, meta := range metas {
				blocks = append(blocks, *meta)
			}
			level.Debug(b.logger).Log("msg", "downloaded blocks meta.json", "num", len(blocks))

			data, err := json.Marshal(blocks)
			if err != nil {
				b.Set("[]", err)
				return err
			}
			// TODO(bwplotka): Allow setting info about partial blocks as well.
			b.Set(string(data), nil)
			return nil
		})
	})
}
