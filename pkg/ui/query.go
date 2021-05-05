// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package ui

import (
	"html/template"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/component"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/query"
)

type Query struct {
	*BaseUI
	storeSet *query.StoreSet

	externalPrefix, prefixHeader string

	cwd     string
	birth   time.Time
	version api.ThanosVersion
	now     func() model.Time
}

func NewQueryUI(logger log.Logger, storeSet *query.StoreSet, externalPrefix, prefixHeader string) *Query {
	tmplVariables := map[string]string{
		"Component": component.Query.String(),
	}
	runtimeInfo := api.GetRuntimeInfoFunc(logger)

	return &Query{
		BaseUI:         NewBaseUI(logger, "query_menu.html", queryTmplFuncs(), tmplVariables, externalPrefix, prefixHeader, component.Query),
		storeSet:       storeSet,
		externalPrefix: externalPrefix,
		prefixHeader:   prefixHeader,
		cwd:            runtimeInfo().CWD,
		birth:          runtimeInfo().StartTime,
		version:        *api.BuildInfo,
		now:            model.Now,
	}
}

func queryTmplFuncs() template.FuncMap {
	return template.FuncMap{
		"since": func(t time.Time) time.Duration {
			return time.Since(t) / time.Millisecond * time.Millisecond
		},
		"formatTimestamp": func(timestamp int64) string {
			return time.Unix(timestamp/1000, 0).Format(time.RFC3339)
		},
		"title": strings.Title,
	}
}

// Register registers new GET routes for subpages and redirects from / to /graph.
func (q *Query) Register(r *route.Router, ins extpromhttp.InstrumentationMiddleware) {
	// Redirect the original React UI's path (under "/new") to its new path at the root.
	r.Get("/new/*path", func(w http.ResponseWriter, r *http.Request) {
		p := route.Param(r.Context(), "path")
		http.Redirect(w, r, path.Join(GetWebPrefix(q.logger, q.externalPrefix, q.prefixHeader, r), strings.TrimPrefix(p, "/new"))+"?"+r.URL.RawQuery, http.StatusFound)
	})

	registerReactApp(r, ins, q.BaseUI)

	// TODO(bplotka): Consider adding more Thanos related data e.g:
	// - What store nodes we see currently.
	// - What sidecars we see currently.
}
