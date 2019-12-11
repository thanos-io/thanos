package ui

import (
	"html/template"
	"net/http"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
)

// Bucket is a web UI representing state of buckets as a timeline.
type Bucket struct {
	*BaseUI
	flagsMap map[string]string
	// Unique Prometheus label that identifies each shard, used as the title. If
	// not present, all labels are displayed externally as a legend.
	Label       string
	Blocks      template.JS
	RefreshedAt time.Time
	Err         error

	cwd   string
	birth time.Time
}

func NewBucketUI(logger log.Logger, label string, flagsMap map[string]string) *Bucket {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "<error retrieving current working directory>"
	}
	return &Bucket{
		BaseUI:   NewBaseUI(logger, "bucket_menu.html", queryTmplFuncs()),
		Blocks:   "[]",
		Label:    label,
		flagsMap: flagsMap,
		cwd:      cwd,
		birth:    time.Now(),
	}
}

// Register registers http routes for bucket UI.
func (b *Bucket) Register(r *route.Router, ins extpromhttp.InstrumentationMiddleware) {
	instrf := func(name string, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
		return ins.NewHandler(name, http.HandlerFunc(next))
	}

	r.Get("/", instrf("root", b.root))
	r.Get("/status", instrf("status", b.status))

	r.Get("/static/*filepath", instrf("static", b.serveStaticAsset))
}

// Handle / of bucket UIs.
func (b *Bucket) root(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(b.logger, b.flagsMap, r)
	b.executeTemplate(w, "bucket.html", prefix, b)
}

func (b *Bucket) Set(data string, err error) {
	b.RefreshedAt = time.Now()
	b.Blocks = template.JS(string(data))
	b.Err = err
}

func (b *Bucket) status(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(b.logger, b.flagsMap, r)

	b.executeTemplate(w, "status.html", prefix, struct {
		Birth   time.Time
		CWD     string
		Version thanosVersion
	}{
		Birth: b.birth,
		CWD:   b.cwd,
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
