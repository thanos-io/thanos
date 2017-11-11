package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/chunks"
)

// PrometheusProxy implements the store node API on top of the Prometheus
// HTTP v1 API.
type PrometheusProxy struct {
	logger         log.Logger
	base           *url.URL
	client         *http.Client
	externalLabels func() labels.Labels
}

var _ storepb.StoreServer = (*PrometheusProxy)(nil)

// NewPrometheusProxy returns a new PrometheusProxy that uses the given HTTP client
// to talk to Prometheus.
// It attaches the provided external labels to all results.
func NewPrometheusProxy(
	logger log.Logger,
	reg prometheus.Registerer,
	client *http.Client,
	baseURL *url.URL,
	externalLabels func() labels.Labels,
) (*PrometheusProxy, error) {
	if client == nil {
		client = http.DefaultClient
	}
	if logger == nil {
		logger = log.NewNopLogger()
	}
	p := &PrometheusProxy{
		logger:         logger,
		base:           baseURL,
		client:         client,
		externalLabels: externalLabels,
	}
	return p, nil
}

// Info returns store information about the Prometheus instance.
func (p *PrometheusProxy) Info(ctx context.Context, r *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	lset := p.externalLabels()

	res := &storepb.InfoResponse{
		Labels: make([]storepb.Label, 0, len(lset)),
	}
	for _, l := range lset {
		res.Labels = append(res.Labels, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return res, nil
}

// Series returns all series for a requested time range and label matcher. The returned data may
// exceed the requested time bounds.
//
// Prometheus's range query API is not suitable to give us all datapoints. We use the
// instant API and do a range selection in PromQL to cover the queried time range.
func (p *PrometheusProxy) Series(ctx context.Context, r *storepb.SeriesRequest) (
	*storepb.SeriesResponse, error,
) {
	sel, err := selectorString(r.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	level.Debug(p.logger).Log("msg", "received request",
		"mint", timestamp.Time(r.MinTime), "maxt", timestamp.Time(r.MaxTime), "matchers", sel)

	// We can only provide second precision so we have to round up. This could
	// cause additional samples to be fetched, which we have to remove further down.
	rng := (r.MaxTime-r.MinTime)/1000 + 1
	q := fmt.Sprintf("%s[%ds]", sel, rng)

	args := url.Values{}

	args.Add("query", q)
	args.Add("time", timestamp.Time(r.MaxTime).Format(time.RFC3339Nano))

	u := *p.base
	u.Path = path.Join(u.Path, "/api/v1/query")
	u.RawQuery = args.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	resp, err := p.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}
	defer resp.Body.Close()

	var m struct {
		Data struct {
			Result model.Matrix `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	res := &storepb.SeriesResponse{
		Series: make([]storepb.Series, 0, len(m.Data.Result)),
	}

	for _, e := range m.Data.Result {
		lset := translateAndExtendLabels(e.Metric, p.externalLabels())
		// We generally expect all samples of the requested range to be traversed
		// so we just encode all samples into one big chunk regardless of size.
		//
		// Drop all data before r.MinTime since we might have fetched more than
		// the requested range (see above).
		enc, b, err := encodeChunk(e.Values, r.MinTime)
		if err != nil {
			return nil, status.Error(codes.Unknown, err.Error())
		}
		res.Series = append(res.Series, storepb.Series{
			Labels: lset,
			Chunks: []storepb.Chunk{{
				MinTime: int64(e.Values[0].Timestamp),
				MaxTime: int64(e.Values[len(e.Values)-1].Timestamp),
				Type:    enc,
				Data:    b,
			}},
		})
	}
	return res, nil
}

// encodeChunk translates the sample pairs into a chunk. It takes a minimum timestamp
// and drops all samples before that one.
func encodeChunk(ss []model.SamplePair, mint int64) (storepb.Chunk_Encoding, []byte, error) {
	c := chunks.NewXORChunk()
	a, err := c.Appender()
	if err != nil {
		return 0, nil, err
	}
	for _, s := range ss {
		if int64(s.Timestamp) < mint {
			continue
		}
		a.Append(int64(s.Timestamp), float64(s.Value))
	}
	return storepb.Chunk_XOR, c.Bytes(), nil
}

// translateAndExtendLabels transforms a metrics into a protobuf label set. It additionally
// attaches the given labels to it, overwriting existing ones on colllision.
func translateAndExtendLabels(m model.Metric, extend labels.Labels) []storepb.Label {
	lset := make([]storepb.Label, 0, len(m)+len(extend))

	for k, v := range m {
		if extend.Get(string(k)) != "" {
			continue
		}
		lset = append(lset, storepb.Label{
			Name:  string(k),
			Value: string(v),
		})
	}
	for _, l := range extend {
		lset = append(lset, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	sort.Slice(lset, func(i, j int) bool {
		return lset[i].Name < lset[j].Name
	})
	return lset
}

// LabelNames returns all known label names.
func (p *PrometheusProxy) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// LabelValues returns all known label values for a given label name.
func (p *PrometheusProxy) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	u := *p.base
	u.Path = path.Join(u.Path, "/api/v1/label/", r.Label, "/values")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	resp, err := p.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}
	defer resp.Body.Close()

	var m struct {
		Data []string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}
	sort.Strings(m.Data)

	return &storepb.LabelValuesResponse{Values: m.Data}, nil
}

func selectorString(ms ...storepb.LabelMatcher) (string, error) {
	var b bytes.Buffer
	b.WriteByte('{')

	for i, m := range ms {
		if i != 0 {
			b.WriteByte(',')
		}
		b.WriteString(m.Name)

		switch m.Type {
		case storepb.LabelMatcher_EQ:
			b.WriteByte('=')
		case storepb.LabelMatcher_NEQ:
			b.WriteString("!=")
		case storepb.LabelMatcher_RE:
			b.WriteString("=~")
		case storepb.LabelMatcher_NRE:
			b.WriteString("!~")
		default:
			return "", errors.New("unknown matcher type")
		}

		b.WriteString(strconv.Quote(m.Value))
	}

	b.WriteByte('}')
	return b.String(), nil
}
