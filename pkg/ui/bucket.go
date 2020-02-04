// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package ui

import (
	"html/template"
	"net/http"
	"path"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/component"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
)

// Bucket is a web UI representing state of buckets as a timeline.
type Bucket struct {
	*BaseUI
	// Unique Prometheus label that identifies each shard, used as the title. If
	// not present, all labels are displayed externally as a legend.
	Label       string
	Blocks      template.JS
	RefreshedAt time.Time
	Err         error
}

func NewBucketUI(logger log.Logger, label string, flagsMap map[string]string) *Bucket {
	return &Bucket{
		BaseUI: NewBaseUI(logger, "bucket_menu.html", queryTmplFuncs(), flagsMap, component.Bucket),
		Blocks: "[]",
		Label:  label,
	}
}

// Register registers http routes for bucket UI.
func (b *Bucket) Register(r *route.Router, ins extpromhttp.InstrumentationMiddleware) {
	instrf := func(name string, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
		return ins.NewHandler(name, http.HandlerFunc(next))
	}

	r.Get("/", instrf("root", b.root))
	r.Get("/static/*filepath", instrf("static", b.serveStaticAsset))
	// Make sure that "<path-prefix>/new" is redirected to "<path-prefix>/new/" and
	// not just the naked "/new/", which would be the default behavior of the router
	// with the "RedirectTrailingSlash" option (https://godoc.org/github.com/julienschmidt/httprouter#Router.RedirectTrailingSlash),
	// and which breaks users with a --web.route-prefix that deviates from the path derived
	// from the external URL.
	r.Get("/new", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, path.Join(GetWebPrefix(b.logger, b.flagsMap, r), "new")+"/", http.StatusFound)
	})
	r.Get("/new/*filepath", instrf("react-static", b.serveReactUI))
}

// Handle / of bucket UIs.
func (b *Bucket) root(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(b.logger, b.flagsMap, r)
	b.executeTemplate(w, "bucket.html", prefix, b)
}

func (b *Bucket) Set(data string, err error) {
	b.RefreshedAt = time.Now()
	b.Blocks = template.JS(string(data))
	b.Err = err
}
