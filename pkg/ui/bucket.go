// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package ui

import (
	"encoding/json"
	"html/template"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
)

// Bucket is a web UI representing state of buckets as a timeline.
type Bucket struct {
	*BaseUI

	externalPrefix, prefixHeader string
	uiPrefix                     string
	// Unique Prometheus label that identifies each shard, used as the title. If
	// not present, all labels are displayed externally as a legend.
	Label       string
	Blocks      template.JS
	RefreshedAt time.Time
	Err         error
}

func NewBucketUI(logger log.Logger, label, externalPrefix, prefixHeader, uiPrefix string, comp component.Component) *Bucket {
	tmplVariables := map[string]string{
		"Component": comp.String(),
	}

	return &Bucket{
		BaseUI:         NewBaseUI(log.With(logger, "component", "bucketUI"), "bucket_menu.html", queryTmplFuncs(), tmplVariables, externalPrefix, prefixHeader, comp),
		Blocks:         "[]",
		Label:          label,
		externalPrefix: externalPrefix,
		prefixHeader:   prefixHeader,
		uiPrefix:       uiPrefix,
	}
}

// Register registers http routes for bucket UI.
func (b *Bucket) Register(r *route.Router, ins extpromhttp.InstrumentationMiddleware) {
	/*	redirectPath := "/blocks"
		if len(b.uiPrefix) > 0 {
			redirectPath = b.uiPrefix
		}
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, path.Join(GetWebPrefix(b.logger, b.externalPrefix, b.prefixHeader, r), redirectPath), http.StatusFound)
		})
		// Redirect the original React UI's path (under "/new") to its new path at the root.
		r.Get("/new/*path", func(w http.ResponseWriter, r *http.Request) {
			p := route.Param(r.Context(), "path")
			http.Redirect(w, r, path.Join(GetWebPrefix(b.logger, b.externalPrefix, b.prefixHeader, r), strings.TrimPrefix(p, "/new"))+"?"+r.URL.RawQuery, http.StatusFound)
		})

		// here we have two routes that serve the same document. It's because it depends where do we come from.
		// If we are coming from the new UI, it will use the first route.
		// If we are coming from the old UI, it will use the second route.
		r.Get(path.Join("/classic", b.uiPrefix), instrf("bucket", ins, b.bucket))
		r.WithPrefix(b.uiPrefix).Get("/classic", instrf("bucket", ins, b.bucket))
		r.WithPrefix(b.uiPrefix).Get("/classic/static/*filepath", instrf("static", ins, b.serveStaticAsset))
		registerReactApp(r, ins, b.BaseUI)*/
	instrf := func(name string, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
		return ins.NewHandler(b.externalPrefix+name, http.HandlerFunc(next))
	}
	r.WithPrefix(b.uiPrefix).Get("/", instrf("root", b.root))
	r.WithPrefix(b.uiPrefix).Get("/static/*filepath", instrf("static", b.serveStaticAsset))
}

func (b *Bucket) root(w http.ResponseWriter, r *http.Request) {
	b.executeTemplate(w, "bucket.html", GetWebPrefix(b.logger, path.Join(b.externalPrefix, strings.TrimPrefix(b.uiPrefix, "/")), b.prefixHeader, r), b)
}

// Handle / of bucket UIs.
func (b *Bucket) bucket(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(b.logger, path.Join(b.externalPrefix, strings.TrimPrefix(b.uiPrefix, "/")), b.prefixHeader, r)
	b.executeTemplate(w, "bucket.html", prefix, b)
}

func (b *Bucket) Set(blocks []metadata.Meta, err error) {
	if err != nil {
		// Last view is maintained.
		b.RefreshedAt = time.Now()
		b.Err = err
		return
	}

	data := "[]"
	dataB, err := json.Marshal(blocks)
	if err == nil {
		data = string(dataB)
	}

	b.RefreshedAt = time.Now()
	b.Blocks = template.JS(data)
	b.Err = err
}
