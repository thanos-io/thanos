// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package alert

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/notifier"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestQueue_Pop_all_Pushed(t *testing.T) {
	qcapacity := 10
	batchsize := 1
	pushes := 3

	q := NewQueue(nil, nil, qcapacity, batchsize, nil, nil, nil)
	for i := 0; i < pushes; i++ {
		q.Push([]*notifier.Alert{
			{},
			{},
		})
	}

	timeoutc := make(chan struct{}, 1)
	time.AfterFunc(time.Second, func() { timeoutc <- struct{}{} })
	popped := 0
	for p := q.Pop(timeoutc); p != nil; p = q.Pop(timeoutc) {
		popped += len(p)
	}

	testutil.Equals(t, pushes*2, popped)
}

func TestQueue_Push_Relabelled(t *testing.T) {
	q := NewQueue(nil, nil, 10, 10, labels.FromStrings("a", "1", "replica", "A"), []string{"b", "replica"}, nil)

	q.Push([]*notifier.Alert{
		{Labels: labels.FromStrings("b", "2", "c", "3")},
		{Labels: labels.FromStrings("c", "3")},
		{Labels: labels.FromStrings("a", "2")},
	})

	testutil.Equals(t, 3, len(q.queue))
	testutil.Equals(t, labels.FromStrings("a", "1", "c", "3"), q.queue[0].Labels)
	testutil.Equals(t, labels.FromStrings("a", "1", "c", "3"), q.queue[1].Labels)
	testutil.Equals(t, labels.FromStrings("a", "1"), q.queue[2].Labels)
}

func TestQueue_Push_Relabelled_Alerts(t *testing.T) {
	q := NewQueue(
		nil, nil, 10, 10, labels.New(), []string{},
		[]*relabel.Config{
			{
				SourceLabels: model.LabelNames{"a"},
				Separator:    ";",
				Regex:        relabel.MustNewRegexp(".*(b).*"),
				TargetLabel:  "d",
				Action:       relabel.Replace,
				Replacement:  "$1",
			},
		},
	)

	q.Push([]*notifier.Alert{
		{Labels: labels.FromMap(map[string]string{
			"a": "abc",
		})},
	})

	testutil.Equals(t, 1, len(q.queue))
	testutil.Equals(
		t, labels.FromMap(map[string]string{
			"a": "abc",
			"d": "b",
		}),
		q.queue[0].Labels,
	)
}

func TestQueue_Push_RelabelDropAlerts(t *testing.T) {
	q := NewQueue(nil, nil, 10, 10, nil, nil,
		[]*relabel.Config{
			{
				SourceLabels: model.LabelNames{"a"},
				Regex:        relabel.MustNewRegexp("1"),
				Action:       relabel.Drop,
			},
		})

	q.Push([]*notifier.Alert{
		{Labels: labels.FromStrings("a", "1")},
		{Labels: labels.FromStrings("a", "2")},
		{Labels: labels.FromStrings("b", "3")},
	})

	testutil.Equals(t, 2, len(q.queue))
	testutil.Equals(t, labels.FromStrings("a", "2"), q.queue[0].Labels)
	testutil.Equals(t, labels.FromStrings("b", "3"), q.queue[1].Labels)
}

func assertSameHosts(t *testing.T, expected, found []*url.URL) {
	testutil.Equals(t, len(expected), len(found))

	host := map[string]struct{}{}
	for _, u := range expected {
		host[u.Host] = struct{}{}
	}

	for _, u := range found {
		_, ok := host[u.Host]
		testutil.Assert(t, ok, "host %s not found in expected URL list %v", u.Host, expected)
	}
}

type fakeClient struct {
	urls []*url.URL
	dof  func(u *url.URL) (*http.Response, error)
	mtx  sync.Mutex
	seen []*url.URL
}

func (f *fakeClient) Endpoints() []*url.URL {
	return f.urls
}

func (f *fakeClient) Do(req *http.Request) (*http.Response, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	u := req.URL
	f.seen = append(f.seen, u)
	if f.dof == nil {
		rec := httptest.NewRecorder()
		rec.WriteHeader(http.StatusOK)
		return rec.Result(), nil
	}
	return f.dof(u)
}

func TestSenderSendsOk(t *testing.T) {
	poster := &fakeClient{
		urls: []*url.URL{{Host: "am1:9090"}, {Host: "am2:9090"}},
	}
	s := NewSender(nil, nil, []*Alertmanager{NewAlertmanager(nil, poster, time.Minute, APIv1)})

	s.Send(context.Background(), []*notifier.Alert{{}, {}})

	assertSameHosts(t, poster.urls, poster.seen)

	testutil.Equals(t, 2, int(promtestutil.ToFloat64(s.sent.WithLabelValues(poster.urls[0].Host))))
	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.errs.WithLabelValues(poster.urls[0].Host))))

	testutil.Equals(t, 2, int(promtestutil.ToFloat64(s.sent.WithLabelValues(poster.urls[1].Host))))
	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.errs.WithLabelValues(poster.urls[1].Host))))
	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.dropped)))
}

func TestSenderSendsOneFails(t *testing.T) {
	poster := &fakeClient{
		urls: []*url.URL{{Host: "am1:9090"}, {Host: "am2:9090"}},
		dof: func(u *url.URL) (*http.Response, error) {
			rec := httptest.NewRecorder()
			if u.Host == "am1:9090" {
				rec.WriteHeader(http.StatusBadRequest)
			} else {
				rec.WriteHeader(http.StatusOK)
			}
			return rec.Result(), nil
		},
	}
	s := NewSender(nil, nil, []*Alertmanager{NewAlertmanager(nil, poster, time.Minute, APIv1)})

	s.Send(context.Background(), []*notifier.Alert{{}, {}})

	assertSameHosts(t, poster.urls, poster.seen)

	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.sent.WithLabelValues(poster.urls[0].Host))))
	testutil.Equals(t, 1, int(promtestutil.ToFloat64(s.errs.WithLabelValues(poster.urls[0].Host))))

	testutil.Equals(t, 2, int(promtestutil.ToFloat64(s.sent.WithLabelValues(poster.urls[1].Host))))
	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.errs.WithLabelValues(poster.urls[1].Host))))
	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.dropped)))
}

func TestSenderSendsAllFail(t *testing.T) {
	poster := &fakeClient{
		urls: []*url.URL{{Host: "am1:9090"}, {Host: "am2:9090"}},
		dof: func(u *url.URL) (*http.Response, error) {
			return nil, errors.New("no such host")
		},
	}
	s := NewSender(nil, nil, []*Alertmanager{NewAlertmanager(nil, poster, time.Minute, APIv1)})

	s.Send(context.Background(), []*notifier.Alert{{}, {}})

	assertSameHosts(t, poster.urls, poster.seen)

	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.sent.WithLabelValues(poster.urls[0].Host))))
	testutil.Equals(t, 1, int(promtestutil.ToFloat64(s.errs.WithLabelValues(poster.urls[0].Host))))

	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.sent.WithLabelValues(poster.urls[1].Host))))
	testutil.Equals(t, 1, int(promtestutil.ToFloat64(s.errs.WithLabelValues(poster.urls[1].Host))))
	testutil.Equals(t, 2, int(promtestutil.ToFloat64(s.dropped)))
}
