// Package alert contains logic to send alert notifications to Alertmanager clusters.
package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	alertPushEndpoint = "/api/v1/alerts"
	contentTypeJSON   = "application/json"
)

// Alert is a generic representation of an alert in the Prometheus eco-system.
type Alert struct {
	// Label value pairs for purpose of aggregation, matching, and disposition
	// dispatching. This must minimally include an "alertname" label.
	Labels labels.Labels `json:"labels"`

	// Extra key/value information which does not define alert identity.
	Annotations labels.Labels `json:"annotations"`

	// The known time range for this alert. Start and end time are both optional.
	StartsAt     time.Time `json:"startsAt,omitempty"`
	EndsAt       time.Time `json:"endsAt,omitempty"`
	GeneratorURL string    `json:"generatorURL,omitempty"`
}

// Name returns the name of the alert. It is equivalent to the "alertname" label.
func (a *Alert) Name() string {
	return a.Labels.Get(labels.AlertName)
}

// Hash returns a hash over the alert. It is equivalent to the alert labels hash.
func (a *Alert) Hash() uint64 {
	return a.Labels.Hash()
}

func (a *Alert) String() string {
	s := fmt.Sprintf("%s[%s]", a.Name(), fmt.Sprintf("%016x", a.Hash())[:7])
	if a.Resolved() {
		return s + "[resolved]"
	}
	return s + "[active]"
}

// Resolved returns true iff the activity interval ended in the past.
func (a *Alert) Resolved() bool {
	return a.ResolvedAt(time.Now())
}

// ResolvedAt returns true off the activity interval ended before
// the given timestamp.
func (a *Alert) ResolvedAt(ts time.Time) bool {
	if a.EndsAt.IsZero() {
		return false
	}
	return !a.EndsAt.After(ts)
}

// Queue is a queue of alert notifications waiting to be sent. The queue is consumed in batches
// and entries are dropped at the front if it runs full.
type Queue struct {
	logger          log.Logger
	maxBatchSize    int
	capacity        int
	toAddLset       labels.Labels
	toExcludeLabels labels.Labels

	mtx   sync.Mutex
	queue []*Alert
	morec chan struct{}

	pushed  prometheus.Counter
	popped  prometheus.Counter
	dropped prometheus.Counter
}

func relabelLabels(lset labels.Labels, excludeLset []string) (toAdd labels.Labels, toExclude labels.Labels) {
	for _, ln := range excludeLset {
		toExclude = append(toExclude, labels.Label{Name: ln})
	}

	for _, l := range lset {
		// Exclude labels to  to add straight away.
		if toExclude.Has(l.Name) {
			continue
		}
		toAdd = append(toAdd, labels.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return toAdd, toExclude
}

// NewQueue returns a new queue. The given label set is attached to all alerts pushed to the queue.
// The given exclude label set tells what label names to drop including external labels.
func NewQueue(logger log.Logger, reg prometheus.Registerer, capacity, maxBatchSize int, externalLset labels.Labels, excludeLabels []string) *Queue {
	toAdd, toExclude := relabelLabels(externalLset, excludeLabels)

	if logger == nil {
		logger = log.NewNopLogger()
	}
	q := &Queue{
		logger:          logger,
		capacity:        capacity,
		morec:           make(chan struct{}, 1),
		maxBatchSize:    maxBatchSize,
		toAddLset:       toAdd,
		toExcludeLabels: toExclude,

		dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "thanos_alert_queue_alerts_dropped_total",
			Help: "Total number of alerts that were dropped from the queue.",
		}),
		pushed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "thanos_alert_queue_alerts_pushed_total",
			Help: "Total number of alerts pushed to the queue.",
		}),
		popped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "thanos_alert_queue_alerts_popped_total",
			Help: "Total number of alerts popped from the queue.",
		}),
	}
	capMetric := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_alert_queue_capacity",
		Help: "Capacity of the alert queue.",
	}, func() float64 {
		return float64(q.Cap())
	})
	lenMetric := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_alert_queue_length",
		Help: "Length of the alert queue.",
	}, func() float64 {
		return float64(q.Len())
	})
	if reg != nil {
		reg.MustRegister(q.pushed, q.popped, q.dropped, lenMetric, capMetric)
	}
	return q
}

// Len returns the current length of the queue.
func (q *Queue) Len() int {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return len(q.queue)
}

// Cap returns the fixed capacity of the queue.
func (q *Queue) Cap() int {
	return q.capacity
}

// Pop takes a batch of alerts from the front of the queue. The batch size is limited
// according to the queues maxBatchSize limit.
// It blocks until elements are available or a termination signal is send on termc.
func (q *Queue) Pop(termc <-chan struct{}) []*Alert {
	select {
	case <-termc:
		return nil
	case <-q.morec:
	}

	q.mtx.Lock()
	defer q.mtx.Unlock()

	as := make([]*Alert, q.maxBatchSize)
	n := copy(as, q.queue)
	q.queue = q.queue[n:]

	q.popped.Add(float64(n))

	return as[:n]
}

// Push adds a list of alerts to the queue.
func (q *Queue) Push(alerts []*Alert) {
	if len(alerts) == 0 {
		return
	}

	q.mtx.Lock()
	defer q.mtx.Unlock()

	q.pushed.Add(float64(len(alerts)))

	// Attach external labels and drop excluded labels before sending.
	// TODO(bwplotka): User proper relabelling with https://github.com/improbable-eng/thanos/issues/660
	for _, a := range alerts {
		lb := labels.NewBuilder(labels.Labels{})
		for _, l := range a.Labels {
			if q.toExcludeLabels.Has(l.Name) {
				continue
			}
			lb.Set(l.Name, l.Value)
		}
		for _, l := range q.toAddLset {
			lb.Set(l.Name, l.Value)
		}
		a.Labels = lb.Labels()
	}

	// Queue capacity should be significantly larger than a single alert
	// batch could be.
	if d := len(alerts) - q.capacity; d > 0 {
		alerts = alerts[d:]

		level.Warn(q.logger).Log(
			"msg", "Alert batch larger than queue capacity, dropping alerts",
			"numDropped", d)
		q.dropped.Add(float64(d))
	}

	// If the queue is full, remove the oldest alerts in favor
	// of newer ones.
	if d := (len(q.queue) + len(alerts)) - q.capacity; d > 0 {
		q.queue = q.queue[d:]

		level.Warn(q.logger).Log(
			"msg", "Alert notification queue full, dropping alerts",
			"numDropped", d)
		q.dropped.Add(float64(d))
	}

	q.queue = append(q.queue, alerts...)

	select {
	case q.morec <- struct{}{}:
	default:
	}
}

// Sender sends notifications to a dynamic set of alertmanagers.
type Sender struct {
	logger        log.Logger
	alertmanagers func() []*url.URL
	doReq         func(req *http.Request) (*http.Response, error)
	timeout       time.Duration

	sent    *prometheus.CounterVec
	errs    *prometheus.CounterVec
	dropped prometheus.Counter
	latency *prometheus.HistogramVec
}

// NewSender returns a new sender. On each call to Send the entire alert batch is sent
// to each Alertmanager returned by the getter function.
func NewSender(
	logger log.Logger,
	reg prometheus.Registerer,
	alertmanagers func() []*url.URL,
	doReq func(req *http.Request) (*http.Response, error),
	timeout time.Duration,
) *Sender {
	if doReq == nil {
		doReq = http.DefaultClient.Do
	}
	if logger == nil {
		logger = log.NewNopLogger()
	}
	s := &Sender{
		logger:        logger,
		alertmanagers: alertmanagers,
		doReq:         doReq,
		timeout:       timeout,

		sent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_alert_sender_alerts_sent_total",
			Help: "Total number of alerts sent by alertmanager.",
		}, []string{"alertmanager"}),

		errs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_alert_sender_errors_total",
			Help: "Total number of errors while sending alerts to alertmanager.",
		}, []string{"alertmanager"}),

		dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "thanos_alert_sender_alerts_dropped_total",
			Help: "Total number of alerts dropped in case of all sends to alertmanagers failed.",
		}),

		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "thanos_alert_sender_latency_seconds",
			Help: "Latency for sending alert notifications (not including dropped notifications).",
		}, []string{"alertmanager"}),
	}
	if reg != nil {
		reg.MustRegister(s.sent, s.dropped, s.latency)
	}
	return s
}

// Send an alert batch to all given Alertmanager URLs.
// TODO(bwplotka): https://github.com/improbable-eng/thanos/issues/660
func (s *Sender) Send(ctx context.Context, alerts []*Alert) {
	if len(alerts) == 0 {
		return
	}
	b, err := json.Marshal(alerts)
	if err != nil {
		level.Warn(s.logger).Log("msg", "sending alerts failed", "err", err)
		return
	}

	var (
		wg         sync.WaitGroup
		numSuccess uint64
	)
	amrs := s.alertmanagers()
	for _, u := range amrs {
		amURL := *u
		sendCtx, cancel := context.WithTimeout(ctx, s.timeout)

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cancel()

			start := time.Now()
			amURL.Path = path.Join(amURL.Path, alertPushEndpoint)

			if err := s.sendOne(sendCtx, amURL.String(), b); err != nil {
				level.Warn(s.logger).Log(
					"msg", "sending alerts failed",
					"alertmanager", amURL.Host,
					"numAlerts", len(alerts),
					"err", err)
				s.errs.WithLabelValues(amURL.Host).Inc()
				return
			}
			s.latency.WithLabelValues(amURL.Host).Observe(time.Since(start).Seconds())
			s.sent.WithLabelValues(amURL.Host).Add(float64(len(alerts)))

			atomic.AddUint64(&numSuccess, 1)
		}()
	}
	wg.Wait()

	if numSuccess > 0 {
		return
	}

	s.dropped.Add(float64(len(alerts)))
	level.Warn(s.logger).Log("msg", "failed to send alerts to all alertmanagers", "alertmanagers", amrs, "alerts", string(b))
	return
}

func (s *Sender) sendOne(ctx context.Context, url string, b []byte) error {
	req, err := http.NewRequest("POST", url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", contentTypeJSON)

	resp, err := s.doReq(req)
	if err != nil {
		return errors.Wrapf(err, "send request to %q", url)
	}
	defer runutil.CloseWithLogOnErr(s.logger, resp.Body, "send one alert")

	if resp.StatusCode/100 != 2 {
		return errors.Errorf("bad response status %v from %q", resp.Status, url)
	}
	return nil
}
