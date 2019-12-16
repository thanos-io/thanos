package alert

import (
	"context"
	"io"
	"net/url"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/pkg/errors"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/thanos-io/thanos/pkg/testutil"
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

type fakeClient struct {
	urls  []*url.URL
	postf func(u *url.URL) error
	mtx   sync.Mutex
	seen  []*url.URL
}

func (f *fakeClient) Endpoints() []*url.URL {
	return f.urls
}

func (f *fakeClient) Do(ctx context.Context, u *url.URL, r io.Reader) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.seen = append(f.seen, u)
	if f.postf == nil {
		return nil
	}
	return f.postf(u)
}

func TestSenderSendsOk(t *testing.T) {
	poster := &fakeClient{
		urls: []*url.URL{{Host: "am1:9090"}, {Host: "am2:9090"}},
	}
	s := NewSender(nil, nil, []AlertmanagerClient{poster})

	s.Send(context.Background(), []*Alert{{}, {}})

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
		postf: func(u *url.URL) error {
			if u.Host == "am1:9090" {
				return errors.New("no such host")
			}
			return nil
		},
	}
	s := NewSender(nil, nil, []AlertmanagerClient{poster})

	s.Send(context.Background(), []*Alert{{}, {}})

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
		postf: func(u *url.URL) error {
			return errors.New("no such host")
		},
	}
	s := NewSender(nil, nil, []AlertmanagerClient{poster})

	s.Send(context.Background(), []*Alert{{}, {}})

	assertSameHosts(t, poster.urls, poster.seen)

	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.sent.WithLabelValues(poster.urls[0].Host))))
	testutil.Equals(t, 1, int(promtestutil.ToFloat64(s.errs.WithLabelValues(poster.urls[0].Host))))

	testutil.Equals(t, 0, int(promtestutil.ToFloat64(s.sent.WithLabelValues(poster.urls[1].Host))))
	testutil.Equals(t, 1, int(promtestutil.ToFloat64(s.errs.WithLabelValues(poster.urls[1].Host))))
	testutil.Equals(t, 2, int(promtestutil.ToFloat64(s.dropped)))
}
