package ui

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"path/filepath"

	"html/template"
	"time"

	"os"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"
)

var localhostRepresentations = []string{"127.0.0.1", "localhost"}

type UI struct {
	logger   log.Logger
	flagsMap map[string]string

	cwd   string
	birth time.Time
	now   func() model.Time
}

type thanosVersion struct {
	Version   string `json:"version"`
	Revision  string `json:"revision"`
	Branch    string `json:"branch"`
	BuildUser string `json:"buildUser"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
}

func New(logger log.Logger, flagsMap map[string]string) *UI {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "<error retrieving current working directory>"
	}
	return &UI{
		logger:   logger,
		flagsMap: flagsMap,
		cwd:      cwd,
		birth:    time.Now(),
		now:      model.Now,
	}
}

func (u *UI) Register(r *route.Router) {
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/graph", http.StatusFound)
	})

	instrf := prometheus.InstrumentHandlerFunc

	r.Get("/graph", instrf("graph", u.graph))
	r.Get("/status", instrf("status", u.status))
	r.Get("/flags", instrf("flags", u.flags))

	r.Get("/static/*filepath", instrf("static", u.serveStaticAsset))
	// TODO(bplotka): Consider adding more Thanos related data e.g:
	// - what store nodes we see currently
	// - what sidecars we see currently
}

func (u *UI) graph(w http.ResponseWriter, r *http.Request) {
	u.executeTemplate(w, "graph.html", nil)
}

func (u *UI) status(w http.ResponseWriter, r *http.Request) {
	u.executeTemplate(w, "status.html", struct {
		Birth   time.Time
		CWD     string
		Version thanosVersion
	}{
		Birth: u.birth,
		CWD:   u.cwd,
		Version: thanosVersion{
			Version:   version.Version,
			Revision:  version.Revision,
			Branch:    version.Branch,
			BuildUser: version.BuildUser,
			BuildDate: version.BuildDate,
			GoVersion: version.GoVersion,
		},
	})
}

func (u *UI) flags(w http.ResponseWriter, r *http.Request) {
	u.executeTemplate(w, "flags.html", u.flagsMap)
}

func (u *UI) tmplFuncs() template.FuncMap {
	return template.FuncMap{
		"since": func(t time.Time) time.Duration {
			return time.Since(t) / time.Millisecond * time.Millisecond
		},
		"pathPrefix":   func() string { return "" },
		"buildVersion": func() string { return version.Revision },
		"stripLabels": func(lset map[string]string, labels ...string) map[string]string {
			for _, ln := range labels {
				delete(lset, ln)
			}
			return lset
		},
	}
}

func (u *UI) serveStaticAsset(w http.ResponseWriter, req *http.Request) {
	fp := route.Param(req.Context(), "filepath")
	fp = filepath.Join("pkg/query/ui/static", fp)

	info, err := AssetInfo(fp)
	if err != nil {
		level.Warn(u.logger).Log("msg", "Could not get file info", "err", err, "file", fp)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	file, err := Asset(fp)
	if err != nil {
		if err != io.EOF {
			level.Warn(u.logger).Log("msg", "Could not get file", "err", err, "file", fp)
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}

	http.ServeContent(w, req, info.Name(), info.ModTime(), bytes.NewReader(file))
}

func (u *UI) getTemplate(name string) (string, error) {
	baseTmpl, err := Asset("pkg/query/ui/templates/_base.html")
	if err != nil {
		return "", fmt.Errorf("error reading base template: %s", err)
	}
	pageTmpl, err := Asset(filepath.Join("pkg/query/ui/templates", name))
	if err != nil {
		return "", fmt.Errorf("error reading page template %s: %s", name, err)
	}
	return string(baseTmpl) + string(pageTmpl), nil
}

func (u *UI) executeTemplate(w http.ResponseWriter, name string, data interface{}) {
	text, err := u.getTemplate(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	t, err := template.New("").Funcs(u.tmplFuncs()).Parse(text)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, data); err != nil {
		level.Warn(u.logger).Log("msg", "template expansion failed", "err", err)
	}
}
