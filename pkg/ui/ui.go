// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package ui

import (
	"bytes"
	"embed"
	"html/template"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"

	"github.com/thanos-io/thanos/pkg/component"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
)

var reactRouterPaths = []string{
	"/",
	"/alerts",
	"/blocks",
	"/config",
	"/flags",
	"/global",
	"/graph",
	"/loaded",
	"/rules",
	"/service-discovery",
	"/status",
	"/stores",
	"/targets",
	"/tsdb-status",
}

//go:embed static/react
var reactUI embed.FS

type BaseUI struct {
	logger                       log.Logger
	tmplFuncs                    template.FuncMap
	tmplVariables                map[string]string
	externalPrefix, prefixHeader string
	component                    component.Component
}

func NewBaseUI(logger log.Logger, funcMap template.FuncMap, tmplVariables map[string]string, externalPrefix, prefixHeader string, component component.Component) *BaseUI {
	funcMap["pathPrefix"] = func() string { return "" }
	funcMap["buildVersion"] = func() string { return version.Revision }

	return &BaseUI{logger: logger, tmplFuncs: funcMap, tmplVariables: tmplVariables, externalPrefix: externalPrefix, prefixHeader: prefixHeader, component: component}
}

func (bu *BaseUI) serveReactUI(w http.ResponseWriter, req *http.Request) {
	bu.serveReactIndex("static/react/index.html", w, req)
}

func (bu *BaseUI) serveReactIndex(index string, w http.ResponseWriter, req *http.Request) {
	_, file, err := bu.getAssetFile(index)
	if err != nil {
		level.Warn(bu.logger).Log("msg", "Could not get file", "err", err, "file", index)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	prefix := GetWebPrefix(bu.logger, bu.externalPrefix, bu.prefixHeader, req)

	tmpl, err := template.New("").Funcs(bu.tmplFuncs).
		Funcs(template.FuncMap{"pathPrefix": absolutePrefix(prefix)}).
		Parse(string(file))

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := tmpl.Execute(w, bu.tmplVariables); err != nil {
		level.Warn(bu.logger).Log("msg", "template expansion failed", "err", err)
	}
}

func (bu *BaseUI) getAssetFile(filename string) (os.FileInfo, []byte, error) {
	filesys := fs.FS(reactUI)

	info, err := fs.Stat(filesys, filename)
	if err != nil {
		return nil, nil, err
	}

	file, err := fs.ReadFile(filesys, filename)
	if err != nil {
		return nil, nil, err
	}

	return info, file, nil
}

func (bu *BaseUI) serveAsset(fp string, w http.ResponseWriter, req *http.Request) error {
	info, file, err := bu.getAssetFile(fp)
	if err != nil {
		return err
	}
	http.ServeContent(w, req, info.Name(), info.ModTime(), bytes.NewReader(file))
	return nil
}

func absolutePrefix(prefix string) func() string {
	return func() string {
		if prefix == "" {
			return ""
		}
		return path.Join("/", prefix)
	}
}

// GetWebPrefix sanitizes an external URL path prefix value.
// A value provided by web.external-prefix flag is preferred over the one supplied through an HTTP header.
func GetWebPrefix(logger log.Logger, externalPrefix, prefixHeader string, r *http.Request) string {
	prefix := r.Header.Get(prefixHeader)

	// Ignore web.prefix-header value if web.external-prefix is defined.
	if len(externalPrefix) > 0 {
		prefix = externalPrefix
	}

	// Even if rfc2616 suggests that Location header "value consists of a single absolute URI", browsers
	// support relative location too. So for extra security, scheme and host parts are stripped from a dynamic prefix.
	prefix, err := SanitizePrefix(prefix)
	if err != nil {
		level.Warn(logger).Log("msg", "Could not parse value of UI external prefix", "prefix", prefix, "err", err)
	}

	return prefix
}

func instrf(name string, ins extpromhttp.InstrumentationMiddleware, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return ins.NewHandler(name, http.HandlerFunc(next))
}

func registerReactApp(r *route.Router, ins extpromhttp.InstrumentationMiddleware, bu *BaseUI) {
	for _, p := range reactRouterPaths {
		r.Get(p, instrf("react-static", ins, bu.serveReactUI))
	}

	// The favicon and manifest are bundled as part of the React app, but we want to serve
	// them on the root.
	for _, p := range []string{"/favicon.ico", "/manifest.json"} {
		assetPath := "static/react" + p
		r.Get(p, func(w http.ResponseWriter, r *http.Request) {
			if err := bu.serveAsset(assetPath, w, r); err != nil {
				level.Warn(bu.logger).Log("msg", "Could not get file", "err", err, "file", assetPath)
				w.WriteHeader(http.StatusNotFound)
			}
		})
	}

	// Static files required by the React app.
	r.Get("/static/*filepath", func(w http.ResponseWriter, r *http.Request) {
		fp := route.Param(r.Context(), "filepath")
		fp = filepath.Join("static/react/static", fp)
		if err := bu.serveAsset(fp, w, r); err != nil {
			level.Warn(bu.logger).Log("msg", "Could not get file", "err", err, "file", fp)
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

// SanitizePrefix makes sure that path prefix value is valid.
// A prefix is returned without a trailing slash. Hence empty string is returned for the root path.
func SanitizePrefix(prefix string) (string, error) {
	u, err := url.Parse(prefix)
	if err != nil {
		return "", err
	}

	// Remove double slashes, convert to absolute path.
	sanitizedPrefix := strings.TrimPrefix(path.Clean(u.Path), ".")
	sanitizedPrefix = strings.TrimSuffix(sanitizedPrefix, "/")

	return sanitizedPrefix, nil
}
