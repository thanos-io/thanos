package ui

import (
	"bytes"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"
)

type BaseUI struct {
	logger    log.Logger
	menuTmpl  string
	tmplFuncs template.FuncMap
}

func NewBaseUI(logger log.Logger, menuTmpl string, funcMap template.FuncMap) *BaseUI {
	funcMap["pathPrefix"] = func() string { return "" }
	funcMap["buildVersion"] = func() string { return version.Revision }

	return &BaseUI{logger: logger, menuTmpl: menuTmpl, tmplFuncs: funcMap}
}

func (bu *BaseUI) serveStaticAsset(w http.ResponseWriter, req *http.Request) {
	fp := route.Param(req.Context(), "filepath")
	fp = filepath.Join("pkg/ui/static", fp)

	info, err := AssetInfo(fp)
	if err != nil {
		level.Warn(bu.logger).Log("msg", "Could not get file info", "err", err, "file", fp)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	file, err := Asset(fp)
	if err != nil {
		if err != io.EOF {
			level.Warn(bu.logger).Log("msg", "Could not get file", "err", err, "file", fp)
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}

	http.ServeContent(w, req, info.Name(), info.ModTime(), bytes.NewReader(file))
}

func (bu *BaseUI) getTemplate(name string) (string, error) {
	baseTmpl, err := Asset("pkg/ui/templates/_base.html")
	if err != nil {
		return "", fmt.Errorf("error reading base template: %s", err)
	}
	menuTmpl, err := Asset(filepath.Join("pkg/ui/templates", bu.menuTmpl))
	if err != nil {
		return "", fmt.Errorf("error reading menu template %s: %s", bu.menuTmpl, err)
	}
	pageTmpl, err := Asset(filepath.Join("pkg/ui/templates", name))
	if err != nil {
		return "", fmt.Errorf("error reading page template %s: %s", name, err)
	}
	return string(baseTmpl) + string(menuTmpl) + string(pageTmpl), nil
}

func (bu *BaseUI) executeTemplate(w http.ResponseWriter, name string, prefix string, data interface{}) {
	text, err := bu.getTemplate(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	bu.tmplFuncs["pathPrefix"] = func() string { return prefix }

	t, err := template.New("").Funcs(bu.tmplFuncs).Parse(text)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, data); err != nil {
		level.Warn(bu.logger).Log("msg", "template expansion failed", "err", err)
	}
}

// GetWebPrefix sanitizes an external URL path prefix value.
// A value provided by web.external-prefix flag is preferred over the one supplied through an HTTP header.
func GetWebPrefix(logger log.Logger, flagsMap map[string]string, r *http.Request) string {
	// Ignore web.prefix-header value if web.external-prefix is defined.
	if len(flagsMap["web.external-prefix"]) > 0 {
		return flagsMap["web.external-prefix"]
	}

	prefix := r.Header.Get(flagsMap["web.prefix-header"])

	// Even if rfc2616 suggests that Location header "value consists of a single absolute URI", browsers
	// support relative location too. So for extra security, scheme and host parts are stripped from a dynamic prefix.
	prefix, err := SanitizePrefix(prefix)
	if err != nil {
		level.Warn(logger).Log("msg", "Could not parse value of UI external prefix", "prefix", prefix, "err", err)
	}

	return prefix
}

// SanitizePrefix makes sure that path prefix value is valid.
// A prefix is returned without a trailing slash. Hence empty string is returned for the root path.
func SanitizePrefix(prefix string) (string, error) {
	u, err := url.Parse(prefix)
	if err != nil {
		return "", err
	}

	// remove double slashes, convert to absolute path
	sanitizedPrefix := strings.TrimPrefix(path.Clean(u.Path), ".")

	if strings.HasSuffix(sanitizedPrefix, "/") {
		sanitizedPrefix = strings.TrimSuffix(sanitizedPrefix, "/")
	}

	return sanitizedPrefix, nil
}
