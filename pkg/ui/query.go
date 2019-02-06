package ui

import (
	"html/template"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/component"
	"github.com/improbable-eng/thanos/pkg/query"
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
		"title": strings.Title,
	}
}

// Register registers new GET routes for subpages and retirects from / to /graph.
func (q *Query) Register(r *route.Router) {
	instrf := prometheus.InstrumentHandlerFunc

	r.Get("/", instrf("root", q.root))
	r.Get("/graph", instrf("graph", q.graph))
	r.Get("/stores", instrf("stores", q.stores))
	r.Get("/status", instrf("status", q.status))
	r.Get("/flags", instrf("flags", q.flags))

	r.Get("/static/*filepath", instrf("static", q.serveStaticAsset))
	// TODO(bplotka): Consider adding more Thanos related data e.g:
	// - what store nodes we see currently
	// - what sidecars we see currently
}

// root redirects "/" requests to "/graph", taking into account the path prefix value
func (q *Query) root(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(q.logger, q.flagsMap, r)

	http.Redirect(w, r, path.Join(prefix, "/graph"), http.StatusFound)
}

func (q *Query) graph(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(q.logger, q.flagsMap, r)

	q.executeTemplate(w, "graph.html", prefix, nil)
}

func (q *Query) status(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(q.logger, q.flagsMap, r)

	q.executeTemplate(w, "status.html", prefix, struct {
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
	prefix := GetWebPrefix(q.logger, q.flagsMap, r)
	statuses := make(map[component.StoreAPI][]query.StoreStatus)
	for _, status := range q.storeSet.GetStoreStatus() {
		statuses[status.StoreType] = append(statuses[status.StoreType], status)
	}
	q.executeTemplate(w, "stores.html", prefix, statuses)
}

func (q *Query) flags(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(q.logger, q.flagsMap, r)
	q.executeTemplate(w, "flags.html", prefix, q.flagsMap)
}
