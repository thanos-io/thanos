package ui

import (
	"bytes"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"path/filepath"

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

func (bu *BaseUI) executeTemplate(w http.ResponseWriter, name string, data interface{}) {
	text, err := bu.getTemplate(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	t, err := template.New("").Funcs(bu.tmplFuncs).Parse(text)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, data); err != nil {
		level.Warn(bu.logger).Log("msg", "template expansion failed", "err", err)
	}
}
