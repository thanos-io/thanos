package store

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/store/prompb"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/chunks"
)

// PrometheusProxy implements the store node API on top of the Prometheus
// HTTP v1 API.
type PrometheusProxy struct {
	logger         log.Logger
	base           *url.URL
	client         *http.Client
	buffers        sync.Pool
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

func (p *PrometheusProxy) getBuffer() []byte {
	b := p.buffers.Get()
	if b == nil {
		return make([]byte, 0, 32*1024) // 32KB seems like a good minimum starting size.
	}
	return b.([]byte)
}

func (p *PrometheusProxy) putBuffer(b []byte) {
	p.buffers.Put(b[:0])
}

// Series returns all series for a requested time range and label matcher. The returned data may
// exceed the requested time bounds.
//
// Prometheus's range query API is not suitable to give us all datapoints. We use the
// instant API and do a range selection in PromQL to cover the queried time range.
func (p *PrometheusProxy) Series(r *storepb.SeriesRequest, s storepb.Store_SeriesServer) error {
	ext := p.externalLabels()

	match, newMatchers, err := extLabelsMatches(ext, r.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return nil
	}
	q := prompb.Query{StartTimestampMs: r.MinTime, EndTimestampMs: r.MaxTime}

	// TODO(fabxc): import common definitions from prompb once we have a stable gRPC
	// query API there.
	for _, m := range newMatchers {
		pm := prompb.LabelMatcher{Name: m.Name, Value: m.Value}

		switch m.Type {
		case storepb.LabelMatcher_EQ:
			pm.Type = prompb.LabelMatcher_EQ
		case storepb.LabelMatcher_NEQ:
			pm.Type = prompb.LabelMatcher_NEQ
		case storepb.LabelMatcher_RE:
			pm.Type = prompb.LabelMatcher_RE
		case storepb.LabelMatcher_NRE:
			pm.Type = prompb.LabelMatcher_NRE
		default:
			return errors.New("unrecognized matcher type")
		}
		q.Matchers = append(q.Matchers, pm)
	}

	reqb, err := proto.Marshal(&prompb.ReadRequest{Queries: []prompb.Query{q}})
	if err != nil {
		return errors.Wrap(err, "marshal read request")
	}

	u := *p.base
	u.Path = "/api/v1/read"

	preq, err := http.NewRequest("POST", u.String(), bytes.NewReader(snappy.Encode(nil, reqb)))
	if err != nil {
		return errors.Wrap(err, "unable to create request")
	}
	preq.Header.Add("Content-Encoding", "snappy")
	preq.Header.Set("Content-Type", "application/x-protobuf")
	preq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	preq = preq.WithContext(s.Context())

	presp, err := p.client.Do(preq)
	if err != nil {
		return errors.Wrap(err, "send request")
	}
	defer presp.Body.Close()

	if presp.StatusCode/100 != 2 {
		return errors.Errorf("request failed with code %s", presp.Status)
	}

	buf := bytes.NewBuffer(p.getBuffer())
	defer func() {
		p.putBuffer(buf.Bytes())
	}()
	if _, err := io.Copy(buf, presp.Body); err != nil {
		return errors.Wrap(err, "copy response")
	}
	decomp, err := snappy.Decode(p.getBuffer(), buf.Bytes())
	defer p.putBuffer(decomp)
	if err != nil {
		return errors.Wrap(err, "decompress response")
	}

	var data prompb.ReadResponse
	if err := proto.Unmarshal(decomp, &data); err != nil {
		return errors.Wrap(err, "unmarshal response")
	}
	if len(data.Results) != 1 {
		return errors.Errorf("unexepected result size %d", len(data.Results))
	}

	var res storepb.SeriesResponse

	for _, e := range data.Results[0].Timeseries {
		lset := translateAndExtendLabels(e.Labels, ext)
		// We generally expect all samples of the requested range to be traversed
		// so we just encode all samples into one big chunk regardless of size.
		//
		// Drop all data before r.MinTime since we might have fetched more than
		// the requested range (see above).
		enc, b, err := encodeChunk(e.Samples, r.MinTime)
		if err != nil {
			return status.Error(codes.Unknown, err.Error())
		}
		res.Series = storepb.Series{
			Labels: lset,
			Chunks: []storepb.Chunk{{
				MinTime: int64(e.Samples[0].Timestamp),
				MaxTime: int64(e.Samples[len(e.Samples)-1].Timestamp),
				Type:    enc,
				Data:    b,
			}},
		}
		if err := s.Send(&res); err != nil {
			return err
		}
	}
	return nil
}

func extLabelsMatches(extLabels labels.Labels, ms []storepb.LabelMatcher) (bool, []storepb.LabelMatcher, error) {
	var newMatcher []storepb.LabelMatcher
	for _, m := range ms {
		extValue := extLabels.Get(m.Name)
		if extValue == "" {
			// Agnostic to external labels.
			newMatcher = append(newMatcher, m)
			continue
		}

		m, err := translateMatcher(m)
		if err != nil {
			return false, nil, err
		}

		if !m.Matches(extValue) {
			// External label does not match. This should not happen - it should be filtered out on query node,
			// but let's do that anyway here.
			return false, nil, nil
		}
	}

	return true, newMatcher, nil
}

// encodeChunk translates the sample pairs into a chunk. It takes a minimum timestamp
// and drops all samples before that one.
func encodeChunk(ss []prompb.Sample, mint int64) (storepb.Chunk_Encoding, []byte, error) {
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
func translateAndExtendLabels(m []prompb.Label, extend labels.Labels) []storepb.Label {
	lset := make([]storepb.Label, 0, len(m)+len(extend))

	for _, l := range m {
		if extend.Get(l.Name) != "" {
			continue
		}
		lset = append(lset, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
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
