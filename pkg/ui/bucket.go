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
func (b *Bucket) Register(r *route.Router, registerNewUI bool, ins extpromhttp.InstrumentationMiddleware) {
	instrf := func(name string, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
		return ins.NewHandler(b.externalPrefix+name, http.HandlerFunc(next))
	}
	r.WithPrefix(b.uiPrefix).Get("/", instrf("root", b.root))
	r.WithPrefix(b.uiPrefix).Get("/static/*filepath", instrf("static", b.serveStaticAsset))
	if registerNewUI {
		// Make sure that "<path-prefix>/new" is redirected to "<path-prefix>/new/" and
		// not just the naked "/new/", which would be the default behavior of the router
		// with the "RedirectTrailingSlash" option (https://godoc.org/github.com/julienschmidt/httprouter#Router.RedirectTrailingSlash),
		// and which breaks users with a --web.route-prefix that deviates from the path derived
		// from the external URL.
		r.Get("/new", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, path.Join(GetWebPrefix(b.logger, b.externalPrefix, b.prefixHeader, r), "new")+"/", http.StatusFound)
		})
		r.Get("/new/*filepath", instrf("react-static", b.serveReactUI))
	}
}

// Handle / of bucket UIs.
func (b *Bucket) root(w http.ResponseWriter, r *http.Request) {
	b.executeTemplate(w, "bucket.html", GetWebPrefix(b.logger, path.Join(b.externalPrefix, strings.TrimPrefix(b.uiPrefix, "/")), b.prefixHeader, r), b)
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
