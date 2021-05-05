// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package ui

import (
	"net/http"
	"path"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/component"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
)

// Bucket is a web UI representing state of buckets as a timeline.
type Bucket struct {
	*BaseUI

	externalPrefix, prefixHeader string
	// Unique Prometheus label that identifies each shard, used as the title. If
	// not present, all labels are displayed externally as a legend.
	Err error
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
		externalPrefix: externalPrefix,
		prefixHeader:   prefixHeader,
	}
}

// Register registers http routes for bucket UI.
func (b *Bucket) Register(r *route.Router, ins extpromhttp.InstrumentationMiddleware) {
	// Redirect the original React UI's path (under "/new") to its new path at the root.
	r.Get("/new/*path", func(w http.ResponseWriter, r *http.Request) {
		p := route.Param(r.Context(), "path")
		http.Redirect(w, r, path.Join(GetWebPrefix(b.logger, b.externalPrefix, b.prefixHeader, r), strings.TrimPrefix(p, "/new"))+"?"+r.URL.RawQuery, http.StatusFound)
	})
	registerReactApp(r, ins, b.BaseUI)
}
