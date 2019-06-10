package receive

import (
	"io/ioutil"
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/improbable-eng/thanos/pkg/prober"
	"github.com/improbable-eng/thanos/pkg/store/prompb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/route"
	promtsdb "github.com/prometheus/prometheus/storage/tsdb"
)

var (
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "thanos_http_request_duration_seconds",
			Help:    "Histogram of latencies for HTTP requests.",
			Buckets: []float64{.1, .2, .4, 1, 3, 8, 20, 60, 120},
		},
		[]string{"handler"},
	)
	responseSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "thanos_http_response_size_bytes",
			Help:    "Histogram of response size for HTTP requests.",
			Buckets: prometheus.ExponentialBuckets(100, 10, 8),
		},
		[]string{"handler"},
	)
)

// Options for the web Handler.
type Options struct {
	Receiver        *Writer
	ReadinessProber *prober.Prober
	Registry        prometheus.Registerer
	ReadyStorage    *promtsdb.ReadyStorage
}

// Handler serves a Prometheus remote write receiving HTTP endpoint.
type Handler struct {
	readyStorage    *promtsdb.ReadyStorage
	logger          log.Logger
	receiver        *Writer
	router          *route.Router
	options         *Options
	quitCh          chan struct{}
	readinessProber *prober.Prober
}

func instrumentHandler(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
	return promhttp.InstrumentHandlerDuration(
		requestDuration.MustCurryWith(prometheus.Labels{"handler": handlerName}),
		promhttp.InstrumentHandlerResponseSize(
			responseSize.MustCurryWith(prometheus.Labels{"handler": handlerName}),
			handler,
		),
	)
}

func NewHandler(logger log.Logger, o *Options) *Handler {
	router := route.New().WithInstrumentation(instrumentHandler)
	if logger == nil {
		logger = log.NewNopLogger()
	}

	h := &Handler{
		logger:          logger,
		router:          router,
		readyStorage:    o.ReadyStorage,
		receiver:        o.Receiver,
		options:         o,
		quitCh:          make(chan struct{}),
		readinessProber: o.ReadinessProber,
	}

	router.Post("/api/v1/receive", h.readinessProber.HandleIfReady(h.receive))
	o.Registry.MustRegister(
		requestDuration,
		responseSize,
	)

	return h
}

// Ready sets Handler to be healthy.
func (h *Handler) Healthy() {
	h.readinessProber.SetHealthy()
}

// Ready sets Handler to be ready.
func (h *Handler) Ready() {
	h.readinessProber.SetReady()
}

// Verifies whether the server is ready or not.
func (h *Handler) isReady() error {
	return h.readinessProber.IsReady()
}

// HandleInMux hadles this router in specified mux on given part
func (h *Handler) HandleInMux(path string, mux *http.ServeMux) {
	mux.Handle(path, h.router)
}

// Quit returns the receive-only quit channel.
func (h *Handler) Quit() <-chan struct{} {
	return h.quitCh
}

func (h *Handler) receive(w http.ResponseWriter, req *http.Request) {
	compressed, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		level.Error(h.logger).Log("msg", "snappy decode error", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var wreq prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &wreq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.receiver.Receive(&wreq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
