package ui

import (
	"html/template"
	"net/http"
	"time"

	"github.com/improbable-eng/thanos/pkg/query"

	"os"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"
)

var localhostRepresentations = []string{"127.0.0.1", "localhost"}

type Query struct {
	*BaseUI
	storeSet *query.StoreSet

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

func NewQueryUI(logger log.Logger, storeSet *query.StoreSet, flagsMap map[string]string) *Query {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "<error retrieving current working directory>"
	}
	return &Query{
		BaseUI:   NewBaseUI(logger, "query_menu.html", queryTmplFuncs()),
		storeSet: storeSet,
		flagsMap: flagsMap,
		cwd:      cwd,
		birth:    time.Now(),
		now:      model.Now,
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
	}
}

func (q *Query) Register(r *route.Router) {
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/graph", http.StatusFound)
	})

	instrf := prometheus.InstrumentHandlerFunc

	r.Get("/graph", instrf("graph", q.graph))
	r.Get("/stores", instrf("stores", q.stores))
	r.Get("/status", instrf("status", q.status))
	r.Get("/flags", instrf("flags", q.flags))

	r.Get("/static/*filepath", instrf("static", q.serveStaticAsset))
	// TODO(bplotka): Consider adding more Thanos related data e.g:
	// - what store nodes we see currently
	// - what sidecars we see currently
}

func (q *Query) graph(w http.ResponseWriter, r *http.Request) {
	q.executeTemplate(w, "graph.html", nil)
}

func (q *Query) status(w http.ResponseWriter, r *http.Request) {
	q.executeTemplate(w, "status.html", struct {
		Birth   time.Time
		CWD     string
		Version thanosVersion
	}{
		Birth: q.birth,
		CWD:   q.cwd,
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

func (q *Query) stores(w http.ResponseWriter, r *http.Request) {
	q.executeTemplate(w, "stores.html", q.storeSet.GetStoreStatus())
}

func (q *Query) flags(w http.ResponseWriter, r *http.Request) {
	q.executeTemplate(w, "flags.html", q.flagsMap)
}
