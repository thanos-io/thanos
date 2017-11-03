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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/improbable-eng/promlts/pkg/store/storepb"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/chunks"
)

// PrometheusProxy implements the store node API on top of the Prometheus
// HTTP v1 API.
type PrometheusProxy struct {
	base   *url.URL
	client *http.Client
}

var _ storepb.StoreServer = (*PrometheusProxy)(nil)

// NewPrometheusProxy returns a new PrometheusProxy that uses the given HTTP client
// to talk to Prometheus.
func NewPrometheusProxy(client *http.Client, baseURL string) (*PrometheusProxy, error) {
	if client == nil {
		client = http.DefaultClient
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, errors.Wrap(err, "parse base URL")
	}
	return &PrometheusProxy{base: u, client: client}, nil
}

// Series returns all series for a requested time range and label matcher. The returned data may
// exceed the requested time bounds.
func (p *PrometheusProxy) Series(ctx context.Context, r *storepb.SeriesRequest) (
	*storepb.SeriesResponse, error,
) {
	// Prometheus's range query API is not suitable to give us all datapoints. We use the
	// instant API and do a range selection in PromQL to cover the queried time range.
	args := url.Values{}

	// We can only provide second precision so we have to round up.
	rng := (r.MaxTime-r.MinTime)/1000 + 1
	q := fmt.Sprintf("%s[%ds]", selectorString(r.Matchers...), rng)

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
		lset := translateLabels(e.Metric)
		// We generally expect all samples of the requested range to be traversed
		// so we just encode all samples into one big chunk regardless of size.
		enc, b, err := encodeChunk(e.Values, r.MinTime)
		if err != nil {
			return nil, status.Error(codes.Unknown, err.Error())
		}
		res.Series = append(res.Series, storepb.Series{
			Labels: lset,
			Chunks: []storepb.Chunk{{Type: enc, Data: b}},
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

func translateLabels(m model.Metric) []storepb.Label {
	lset := make([]storepb.Label, 0, len(m))

	for k, v := range m {
		lset = append(lset, storepb.Label{
			Name:  string(k),
			Value: string(v),
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

func selectorString(ms ...storepb.LabelMatcher) string {
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
			panic("unknown matcher type")
		}

		b.WriteString(strconv.Quote(m.Value))
	}

	b.WriteByte('}')
	return b.String()
}
