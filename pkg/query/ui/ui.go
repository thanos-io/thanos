package ui

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"

	"context"
	"encoding/json"
	"net"
	template_text "text/template"
	"time"

	"os"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/template"
)

var localhostRepresentations = []string{"127.0.0.1", "localhost"}

type UI struct {
	ctx         context.Context
	logger      log.Logger
	externalURL *url.URL
	listenURL   *url.URL
	queryEngine *promql.Engine
	versionInfo ThanosVersion
	flagsMap    map[string]string

	cwd   string
	birth time.Time
	now   func() model.Time
}

// ThanosVersion contains build information about Prometheus.
type ThanosVersion struct {
	Version   string `json:"version"`
	Revision  string `json:"revision"`
	Branch    string `json:"branch"`
	BuildUser string `json:"buildUser"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
}

func New(
	ctx context.Context,
	logger log.Logger,
	externalURL *url.URL,
	listenURL *url.URL,
	queryEngine *promql.Engine,
	versionInfo ThanosVersion,
	flagsMap map[string]string,
) *UI {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "<error retrieving current working directory>"
	}

	return &UI{
		ctx:         ctx,
		logger:      logger,
		externalURL: externalURL,
		listenURL:   listenURL,
		queryEngine: queryEngine,
		versionInfo: versionInfo,
		flagsMap:    flagsMap,

		cwd:   cwd,
		birth: time.Now(),
		now:   model.Now,
	}
}

func (u *UI) Register(r *route.Router) {
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, path.Join(u.externalURL.Path, "/graph"), http.StatusFound)
	})

	instrf := prometheus.InstrumentHandlerFunc

	r.Get("/graph", instrf("graph", u.graph))
	r.Get("/status", instrf("status", u.status))
	r.Get("/flags", instrf("flags", u.flags))
	r.Get("/version", instrf("version", u.version))

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
		Birth         time.Time
		CWD           string
		Version       ThanosVersion
		Alertmanagers []*url.URL
	}{
		Birth:   u.birth,
		CWD:     u.cwd,
		Version: u.versionInfo,
	})
}

func (u *UI) flags(w http.ResponseWriter, r *http.Request) {
	u.executeTemplate(w, "flags.html", u.flagsMap)
}

func (u *UI) version(w http.ResponseWriter, r *http.Request) {
	dec := json.NewEncoder(w)
	if err := dec.Encode(u.versionInfo); err != nil {
		http.Error(w, fmt.Sprintf("error encoding JSON: %s", err), http.StatusInternalServerError)
	}
}

func (u *UI) tmplFuncs() template_text.FuncMap {
	return template_text.FuncMap{
		"since": func(t time.Time) time.Duration {
			return time.Since(t) / time.Millisecond * time.Millisecond
		},
		"pathPrefix":   func() string { return u.externalURL.Path },
		"buildVersion": func() string { return u.versionInfo.Revision },
		"stripLabels": func(lset map[string]string, labels ...string) map[string]string {
			for _, ln := range labels {
				delete(lset, ln)
			}
			return lset
		},
		"globalURL": func(fetchedURL *url.URL) *url.URL {
			host, port, err := net.SplitHostPort(fetchedURL.Host)
			if err != nil {
				return fetchedURL
			}
			for _, lhr := range localhostRepresentations {
				if host == lhr {
					ownPort := u.listenURL.Port()
					if port == ownPort {
						// Only in the case where the target is on localhost and its port is
						// the same as the one we're listening on, we know for sure that
						// we're monitoring our own process and that we need to change the
						// scheme, hostname, and port to the externally reachable ones as
						// well. We shouldn't need to touch the path at all, since if a
						// path prefix is defined, the path under which we scrape ourselves
						// should already contain the prefix.
						fetchedURL.Scheme = u.externalURL.Scheme
						fetchedURL.Host = u.externalURL.Host
					} else {
						// Otherwise, we only know that localhost is not reachable
						// externally, so we replace only the hostname by the one in the
						// external URL. It could be the wrong hostname for the service on
						// this port, but it's still the best possible guess.
						host, _, err := net.SplitHostPort(u.externalURL.Host)
						if err != nil {
							return fetchedURL
						}
						fetchedURL.Host = host + ":" + port
					}
					break
				}
			}
			return fetchedURL
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
	}

	tmpl := template.NewTemplateExpander(u.ctx, text, name, data, u.now(), u.queryEngine, u.externalURL)
	tmpl.Funcs(u.tmplFuncs())

	result, err := tmpl.ExpandHTML(nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, result)
}
