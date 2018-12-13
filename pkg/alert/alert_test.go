package alert

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
)

func TestQueue_Push_Relabelled(t *testing.T) {
	q := NewQueue(
		nil, nil, 10, 10,
		labels.FromStrings("a", "1", "replica", "A"), // Labels to be added.
		[]string{"b", "replica"},                     // Labels to be dropped (excluding those added).
	)

	q.Push([]*Alert{
		{Labels: labels.FromStrings("b", "2", "c", "3")},
		{Labels: labels.FromStrings("c", "3")},
		{Labels: labels.FromStrings("a", "2")},
	})

	testutil.Equals(t, 3, len(q.queue))
	testutil.Equals(t, labels.FromStrings("a", "1", "c", "3"), q.queue[0].Labels)
	testutil.Equals(t, labels.FromStrings("a", "1", "c", "3"), q.queue[1].Labels)
	testutil.Equals(t, labels.FromStrings("a", "1"), q.queue[2].Labels)
}

func assertSameHosts(t *testing.T, expected []*url.URL, found []*url.URL) {
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

func TestSender_Send_OK(t *testing.T) {
	var (
		expectedHosts = []*url.URL{{Host: "am1:9090"}, {Host: "am2:9090"}}
		spottedHosts  []*url.URL
		spottedMu     sync.Mutex
	)

	okDo := func(req *http.Request) (response *http.Response, e error) {
		spottedMu.Lock()
		defer spottedMu.Unlock()

		spottedHosts = append(spottedHosts, req.URL)

		return &http.Response{
			Body:       ioutil.NopCloser(bytes.NewBuffer(nil)),
			StatusCode: http.StatusOK,
		}, nil
	}
	s := NewSender(nil, nil, func() []*url.URL { return expectedHosts }, okDo, 10*time.Second)

	s.Send(context.Background(), []*Alert{{}, {}})

	assertSameHosts(t, expectedHosts, spottedHosts)

	testutil.Equals(t, 2, int(promtestutil.ToFloat64(s.sent.WithLabelValues(expectedHosts[0].Host))))
	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.errs.WithLabelValues(expectedHosts[0].Host))))

	testutil.Equals(t, 2, int(promtestutil.ToFloat64(s.sent.WithLabelValues(expectedHosts[1].Host))))
	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.errs.WithLabelValues(expectedHosts[1].Host))))
	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.dropped)))
}

func TestSender_Send_OneFails(t *testing.T) {
	var (
		expectedHosts = []*url.URL{{Host: "am1:9090"}, {Host: "am2:9090"}}
		spottedHosts  []*url.URL
		spottedMu     sync.Mutex
	)

	do := func(req *http.Request) (response *http.Response, e error) {
		spottedMu.Lock()
		defer spottedMu.Unlock()

		spottedHosts = append(spottedHosts, req.URL)

		if req.Host == expectedHosts[0].Host {
			return nil, errors.New("no such host")
		}
		return &http.Response{
			Body:       ioutil.NopCloser(bytes.NewBuffer(nil)),
			StatusCode: http.StatusOK,
		}, nil
	}
	s := NewSender(nil, nil, func() []*url.URL { return expectedHosts }, do, 10*time.Second)

	s.Send(context.Background(), []*Alert{{}, {}})

	assertSameHosts(t, expectedHosts, spottedHosts)

	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.sent.WithLabelValues(expectedHosts[0].Host))))
	testutil.Equals(t, 1, int(promtestutil.ToFloat64(s.errs.WithLabelValues(expectedHosts[0].Host))))

	testutil.Equals(t, 2, int(promtestutil.ToFloat64(s.sent.WithLabelValues(expectedHosts[1].Host))))
	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.errs.WithLabelValues(expectedHosts[1].Host))))
	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.dropped)))
}

func TestSender_Send_AllFails(t *testing.T) {
	var (
		expectedHosts = []*url.URL{{Host: "am1:9090"}, {Host: "am2:9090"}}
		spottedHosts  []*url.URL
		spottedMu     sync.Mutex
	)

	do := func(req *http.Request) (response *http.Response, e error) {
		spottedMu.Lock()
		defer spottedMu.Unlock()

		spottedHosts = append(spottedHosts, req.URL)

		return nil, errors.New("no such host")
	}
	s := NewSender(nil, nil, func() []*url.URL { return expectedHosts }, do, 10*time.Second)

	s.Send(context.Background(), []*Alert{{}, {}})

	assertSameHosts(t, expectedHosts, spottedHosts)

	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.sent.WithLabelValues(expectedHosts[0].Host))))
	testutil.Equals(t, 1, int(promtestutil.ToFloat64(s.errs.WithLabelValues(expectedHosts[0].Host))))

	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.sent.WithLabelValues(expectedHosts[1].Host))))
	testutil.Equals(t, 1, int(promtestutil.ToFloat64(s.errs.WithLabelValues(expectedHosts[1].Host))))
	testutil.Equals(t, 2, int(promtestutil.ToFloat64(s.dropped)))
}
