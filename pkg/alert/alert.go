// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package alert contains logic to send alert notifications to Alertmanager clusters.
package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/api/v2/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/notifier"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/tracing"
)

const (
	defaultAlertmanagerPort = 9093
	contentTypeJSON         = "application/json"
)

// Queue is a queue of alert notifications waiting to be sent. The queue is consumed in batches
// and entries are dropped at the front if it runs full.
type Queue struct {
	logger              log.Logger
	maxBatchSize        int
	capacity            int
	toAddLset           labels.Labels
	toExcludeLabels     labels.Labels
	alertRelabelConfigs []*relabel.Config

	mtx   sync.Mutex
	queue []*notifier.Alert
	morec chan struct{}

	pushed  prometheus.Counter
	popped  prometheus.Counter
	dropped prometheus.Counter
}

func relabelLabels(lset labels.Labels, excludeLset []string) (toAdd, toExclude labels.Labels) {
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
func NewQueue(logger log.Logger, reg prometheus.Registerer, capacity, maxBatchSize int, externalLset labels.Labels, excludeLabels []string, alertRelabelConfigs []*relabel.Config) *Queue {
	toAdd, toExclude := relabelLabels(externalLset, excludeLabels)

	if logger == nil {
		logger = log.NewNopLogger()
	}
	q := &Queue{
		logger:              logger,
		capacity:            capacity,
		morec:               make(chan struct{}, 1),
		maxBatchSize:        maxBatchSize,
		toAddLset:           toAdd,
		toExcludeLabels:     toExclude,
		alertRelabelConfigs: alertRelabelConfigs,

		dropped: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_alert_queue_alerts_dropped_total",
			Help: "Total number of alerts that were dropped from the queue.",
		}),
		pushed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_alert_queue_alerts_pushed_total",
			Help: "Total number of alerts pushed to the queue.",
		}),
		popped: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_alert_queue_alerts_popped_total",
			Help: "Total number of alerts popped from the queue.",
		}),
	}
	_ = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_alert_queue_capacity",
		Help: "Capacity of the alert queue.",
	}, func() float64 {
		return float64(q.Cap())
	})
	_ = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_alert_queue_length",
		Help: "Length of the alert queue.",
	}, func() float64 {
		return float64(q.Len())
	})
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
func (q *Queue) Pop(termc <-chan struct{}) []*notifier.Alert {
	select {
	case <-termc:
		return nil
	case <-q.morec:
	}

	q.mtx.Lock()
	defer q.mtx.Unlock()

	as := make([]*notifier.Alert, q.maxBatchSize)
	n := copy(as, q.queue)
	q.queue = q.queue[n:]

	q.popped.Add(float64(n))

	if len(q.queue) > 0 {
		select {
		case q.morec <- struct{}{}:
		default:
		}
	}
	return as[:n]
}

// Push adds a list of alerts to the queue.
func (q *Queue) Push(alerts []*notifier.Alert) {
	if len(alerts) == 0 {
		return
	}

	q.mtx.Lock()
	defer q.mtx.Unlock()

	q.pushed.Add(float64(len(alerts)))

	// Attach external labels, drop excluded labels and process relabeling before sending.
	var relabeledAlerts []*notifier.Alert
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
		a.Labels = relabel.Process(lb.Labels(nil), q.alertRelabelConfigs...)
		if a.Labels != nil {
			relabeledAlerts = append(relabeledAlerts, a)
		}
	}

	alerts = relabeledAlerts
	if len(alerts) == 0 {
		return
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
	alertmanagers []*Alertmanager
	versions      []APIVersion

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
	alertmanagers []*Alertmanager,
) *Sender {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	var (
		versions       []APIVersion
		versionPresent map[APIVersion]struct{}
	)
	for _, am := range alertmanagers {
		if _, found := versionPresent[am.version]; found {
			continue
		}
		versions = append(versions, am.version)
	}
	s := &Sender{
		logger:        logger,
		alertmanagers: alertmanagers,
		versions:      versions,

		sent: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_alert_sender_alerts_sent_total",
			Help: "Total number of alerts sent by alertmanager.",
		}, []string{"alertmanager"}),

		errs: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_alert_sender_errors_total",
			Help: "Total number of errors while sending alerts to alertmanager.",
		}, []string{"alertmanager"}),

		dropped: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_alert_sender_alerts_dropped_total",
			Help: "Total number of alerts dropped in case of all sends to alertmanagers failed.",
		}),

		latency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name: "thanos_alert_sender_latency_seconds",
			Help: "Latency for sending alert notifications (not including dropped notifications).",
		}, []string{"alertmanager"}),
	}
	return s
}

func toAPILabels(labels labels.Labels) models.LabelSet {
	apiLabels := make(models.LabelSet, len(labels))
	for _, label := range labels {
		apiLabels[label.Name] = label.Value
	}

	return apiLabels
}

// Send an alert batch to all given Alertmanager clients.
// TODO(bwplotka): https://github.com/thanos-io/thanos/issues/660.
func (s *Sender) Send(ctx context.Context, alerts []*notifier.Alert) {
	if len(alerts) == 0 {
		return
	}

	payload := make(map[APIVersion][]byte)
	for _, version := range s.versions {
		var (
			b   []byte
			err error
		)
		switch version {
		case APIv1:
			if b, err = json.Marshal(alerts); err != nil {
				level.Warn(s.logger).Log("msg", "encoding alerts for v1 API failed", "err", err)
				return
			}
		case APIv2:
			apiAlerts := make(models.PostableAlerts, 0, len(alerts))
			for _, a := range alerts {
				apiAlerts = append(apiAlerts, &models.PostableAlert{
					Annotations: toAPILabels(a.Annotations),
					EndsAt:      strfmt.DateTime(a.EndsAt),
					StartsAt:    strfmt.DateTime(a.StartsAt),
					Alert: models.Alert{
						GeneratorURL: strfmt.URI(a.GeneratorURL),
						Labels:       toAPILabels(a.Labels),
					},
				})
			}
			if b, err = json.Marshal(apiAlerts); err != nil {
				level.Warn(s.logger).Log("msg", "encoding alerts for v2 API failed", "err", err)
				return
			}
		}
		payload[version] = b
	}

	var (
		wg         sync.WaitGroup
		numSuccess atomic.Uint64
	)
	for _, am := range s.alertmanagers {
		for _, u := range am.dispatcher.Endpoints() {
			wg.Add(1)
			go func(am *Alertmanager, u url.URL) {
				defer wg.Done()

				level.Debug(s.logger).Log("msg", "sending alerts", "alertmanager", u.Host, "numAlerts", len(alerts))
				start := time.Now()
				u.Path = path.Join(u.Path, fmt.Sprintf("/api/%s/alerts", string(am.version)))

				tracing.DoInSpan(ctx, "post_alerts HTTP[client]", func(ctx context.Context) {
					if err := am.postAlerts(ctx, u, bytes.NewReader(payload[am.version])); err != nil {
						level.Warn(s.logger).Log(
							"msg", "sending alerts failed",
							"alertmanager", u.Host,
							"alerts", string(payload[am.version]),
							"err", err,
						)
						s.errs.WithLabelValues(u.Host).Inc()
						return
					}
					s.latency.WithLabelValues(u.Host).Observe(time.Since(start).Seconds())
					s.sent.WithLabelValues(u.Host).Add(float64(len(alerts)))

					numSuccess.Inc()
				})
			}(am, *u)
		}
	}
	wg.Wait()

	if numSuccess.Load() > 0 {
		return
	}

	s.dropped.Add(float64(len(alerts)))
	level.Warn(s.logger).Log("msg", "failed to send alerts to all alertmanagers", "numAlerts", len(alerts))
}

type Dispatcher interface {
	// Endpoints returns the list of endpoint URLs the dispatcher knows about.
	Endpoints() []*url.URL
	// Do sends an HTTP request and returns a response.
	Do(*http.Request) (*http.Response, error)
}

// Alertmanager is an HTTP client that can send alerts to a cluster of Alertmanager endpoints.
type Alertmanager struct {
	logger     log.Logger
	dispatcher Dispatcher
	timeout    time.Duration
	version    APIVersion
}

// NewAlertmanager returns a new Alertmanager client.
func NewAlertmanager(logger log.Logger, dispatcher Dispatcher, timeout time.Duration, version APIVersion) *Alertmanager {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	return &Alertmanager{
		logger:     logger,
		dispatcher: dispatcher,
		timeout:    timeout,
		version:    version,
	}
}

func (a *Alertmanager) postAlerts(ctx context.Context, u url.URL, r io.Reader) error {
	req, err := http.NewRequest("POST", u.String(), r)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", contentTypeJSON)

	resp, err := a.dispatcher.Do(req)
	if err != nil {
		return errors.Wrapf(err, "send request to %q", u.String())
	}
	defer runutil.ExhaustCloseWithLogOnErr(a.logger, resp.Body, "send one alert")

	if resp.StatusCode/100 != 2 {
		return errors.Errorf("bad response status %v from %q", resp.Status, u.String())
	}
	return nil
}
