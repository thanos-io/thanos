package ui

import (
	"bytes"
	"fmt"
	"html/template"
	"io"
	"math"
	"net/http"
	"path/filepath"
	"regexp"
	"time"

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

	funcMap["since"] = func(t time.Time) time.Duration {
		return time.Since(t) / time.Millisecond * time.Millisecond
	}
	funcMap["pathPrefix"] = func() string { return "" }
	funcMap["buildVersion"] = func() string { return version.Revision }
	funcMap["reReplaceAll"] = func(pattern, repl, text string) string {
		re := regexp.MustCompile(pattern)
		return re.ReplaceAllString(text, repl)
	}

	funcMap["stripLabels"] = func(lset map[string]string, labels ...string) map[string]string {
		for _, ln := range labels {
			delete(lset, ln)
		}
		return lset
	}

	funcMap["humanizeDuration"] = func(v float64) string {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Sprintf("%.4g", v)
		}
		if v == 0 {
			return fmt.Sprintf("%.4gs", v)
		}
		if math.Abs(v) >= 1 {
			sign := ""
			if v < 0 {
				sign = "-"
				v = -v
			}
			seconds := int64(v) % 60
			minutes := (int64(v) / 60) % 60
			hours := (int64(v) / 60 / 60) % 24
			days := (int64(v) / 60 / 60 / 24)
			// For days to minutes, we display seconds as an integer.
			if days != 0 {
				return fmt.Sprintf("%s%dd %dh %dm %ds", sign, days, hours, minutes, seconds)
			}
			if hours != 0 {
				return fmt.Sprintf("%s%dh %dm %ds", sign, hours, minutes, seconds)
			}
			if minutes != 0 {
				return fmt.Sprintf("%s%dm %ds", sign, minutes, seconds)
			}
			// For seconds, we display 4 significant digts.
			return fmt.Sprintf("%s%.4gs", sign, v)
		}
		prefix := ""
		for _, p := range []string{"m", "u", "n", "p", "f", "a", "z", "y"} {
			if math.Abs(v) >= 1 {
				break
			}
			prefix = p
			v *= 1000
		}
		return fmt.Sprintf("%.4g%ss", v, prefix)
	}

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
