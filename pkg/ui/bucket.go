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

	tmplFuncs := queryTmplFuncs()
	// here the uiPrefix is empty because the uiPrefix is injected in the pathPrefix.
	// Which seems to be the only way to the correct path in the file bucket.html
	tmplFuncs["uiPrefix"] = func() string { return "" }

	return &Bucket{
		BaseUI:         NewBaseUI(log.With(logger, "component", "bucketUI"), "bucket_menu.html", tmplFuncs, tmplVariables, externalPrefix, prefixHeader, comp),
		Blocks:         "[]",
		Label:          label,
		externalPrefix: externalPrefix,
		prefixHeader:   prefixHeader,
		uiPrefix:       uiPrefix,
	}
}

// Register registers http routes for bucket UI.
func (b *Bucket) Register(r *route.Router, registerNewUI bool, ins extpromhttp.InstrumentationMiddleware) {
	classicPrefix := path.Join("/classic", b.uiPrefix)
	r.WithPrefix(classicPrefix).Get("/", instrf("bucket", ins, b.bucket))
	r.WithPrefix(classicPrefix).Get("/static/*filepath", instrf("static", ins, b.serveStaticAsset))

	if registerNewUI {
		// Redirect the original React UI's path (under "/new") to its new path at the root.
		r.Get("/new/*path", func(w http.ResponseWriter, r *http.Request) {
			p := route.Param(r.Context(), "path")
			prefix := GetWebPrefix(b.logger, b.externalPrefix, b.prefixHeader, r)
			http.Redirect(w, r, path.Join("/", prefix, p)+"?"+r.URL.RawQuery, http.StatusFound)
		})

		registerReactApp(r, ins, b.BaseUI)
	}
}

// Handle / of bucket UIs.
func (b *Bucket) bucket(w http.ResponseWriter, r *http.Request) {
	classicPrefix := path.Join("/classic", b.uiPrefix)
	prefix := GetWebPrefix(b.logger, path.Join(b.externalPrefix, strings.TrimPrefix(classicPrefix, "/")), b.prefixHeader, r)
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
