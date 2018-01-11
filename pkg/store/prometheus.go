package store

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
	"sort"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/store/prompb"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusStore implements the store node API on top of the Prometheus remote read API.
type PrometheusStore struct {
	logger         log.Logger
	base           *url.URL
	client         *http.Client
	buffers        sync.Pool
	externalLabels func() labels.Labels
}

// NewPrometheusStore returns a new PrometheusStore that uses the given HTTP client
// to talk to Prometheus.
// It attaches the provided external labels to all results.
func NewPrometheusStore(
	logger log.Logger,
	reg prometheus.Registerer,
	client *http.Client,
	baseURL *url.URL,
	externalLabels func() labels.Labels,
) (*PrometheusStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if client == nil {
		client = &http.Client{
			Transport: tracing.HTTPTripperware(logger, http.DefaultTransport),
		}
	}
	p := &PrometheusStore{
		logger:         logger,
		base:           baseURL,
		client:         client,
		externalLabels: externalLabels,
	}
	return p, nil
}

// Info returns store information about the Prometheus instance.
// NOTE(bplotka): MaxTime & MinTime are not accurate nor adjusted dynamically like these included in gossip meta.
// This is fine for now, but might be needed in future.
func (p *PrometheusStore) Info(ctx context.Context, r *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	lset := p.externalLabels()

	res := &storepb.InfoResponse{
		MinTime: 0,
		MaxTime: math.MaxInt64,
		Labels:  make([]storepb.Label, 0, len(lset)),
	}
	for _, l := range lset {
		res.Labels = append(res.Labels, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return res, nil
}

func (p *PrometheusStore) getBuffer() []byte {
	b := p.buffers.Get()
	if b == nil {
		return make([]byte, 0, 32*1024) // 32KB seems like a good minimum starting size.
	}
	return b.([]byte)
}

func (p *PrometheusStore) putBuffer(b []byte) {
	p.buffers.Put(b[:0])
}

// Series returns all series for a requested time range and label matcher.
func (p *PrometheusStore) Series(r *storepb.SeriesRequest, s storepb.Store_SeriesServer) error {
	ext := p.externalLabels()

	match, newMatchers, err := labelsMatches(ext, r.Matchers)
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

	resp, err := p.promSeries(s.Context(), q)
	if err != nil {
		return errors.Wrap(err, "query Prometheus")
	}

	span, _ := tracing.StartSpan(s.Context(), "transform_and_respond")
	defer span.Finish()

	for _, e := range resp.Results[0].Timeseries {
		lset := p.translateAndExtendLabels(e.Labels, ext)
		// We generally expect all samples of the requested range to be traversed
		// so we just encode all samples into one big chunk regardless of size.
		enc, cb, err := p.encodeChunk(e.Samples)
		if err != nil {
			return status.Error(codes.Unknown, err.Error())
		}
		resp := storepb.NewSeriesResponse(&storepb.Series{
			Labels: lset,
			Chunks: []storepb.AggrChunk{{
				MinTime: int64(e.Samples[0].Timestamp),
				MaxTime: int64(e.Samples[len(e.Samples)-1].Timestamp),
				Raw:     &storepb.Chunk{Type: enc, Data: cb},
			}},
		})
		if err := s.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func (p *PrometheusStore) promSeries(ctx context.Context, q prompb.Query) (*prompb.ReadResponse, error) {
	span, ctx := tracing.StartSpan(ctx, "query_prometheus")
	defer span.Finish()

	reqb, err := proto.Marshal(&prompb.ReadRequest{Queries: []prompb.Query{q}})
	if err != nil {
		return nil, errors.Wrap(err, "marshal read request")
	}

	u := *p.base
	u.Path = "/api/v1/read"

	preq, err := http.NewRequest("POST", u.String(), bytes.NewReader(snappy.Encode(nil, reqb)))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create request")
	}
	preq.Header.Add("Content-Encoding", "snappy")
	preq.Header.Set("Content-Type", "application/x-protobuf")
	preq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	preq = preq.WithContext(ctx)

	presp, err := p.client.Do(preq)
	if err != nil {
		return nil, errors.Wrap(err, "send request")
	}
	defer presp.Body.Close()

	if presp.StatusCode/100 != 2 {
		return nil, errors.Errorf("request failed with code %s", presp.Status)
	}

	buf := bytes.NewBuffer(p.getBuffer())
	defer func() {
		p.putBuffer(buf.Bytes())
	}()
	if _, err := io.Copy(buf, presp.Body); err != nil {
		return nil, errors.Wrap(err, "copy response")
	}
	decomp, err := snappy.Decode(p.getBuffer(), buf.Bytes())
	defer p.putBuffer(decomp)
	if err != nil {
		return nil, errors.Wrap(err, "decompress response")
	}

	var data prompb.ReadResponse
	if err := proto.Unmarshal(decomp, &data); err != nil {
		return nil, errors.Wrap(err, "unmarshal response")
	}
	if len(data.Results) != 1 {
		return nil, errors.Errorf("unexepected result size %d", len(data.Results))
	}
	return &data, nil
}

func labelsMatches(lset labels.Labels, ms []storepb.LabelMatcher) (bool, []storepb.LabelMatcher, error) {
	var newMatcher []storepb.LabelMatcher
	for _, m := range ms {
		// Validate all matchers.
		tm, err := translateMatcher(m)
		if err != nil {
			return false, nil, err
		}

		extValue := lset.Get(m.Name)
		if extValue == "" {
			// Agnostic to external labels.
			newMatcher = append(newMatcher, m)
			continue
		}

		if !tm.Matches(extValue) {
			// External label does not match. This should not happen - it should be filtered out on query node,
			// but let's do that anyway here.
			return false, nil, nil
		}
	}
	return true, newMatcher, nil
}

// encodeChunk translates the sample pairs into a chunk.
func (p *PrometheusStore) encodeChunk(ss []prompb.Sample) (storepb.Chunk_Encoding, []byte, error) {
	c := chunkenc.NewXORChunk()

	a, err := c.Appender()
	if err != nil {
		return 0, nil, err
	}
	for _, s := range ss {
		a.Append(int64(s.Timestamp), float64(s.Value))
	}
	return storepb.Chunk_XOR, c.Bytes(), nil
}

// translateAndExtendLabels transforms a metrics into a protobuf label set. It additionally
// attaches the given labels to it, overwriting existing ones on colllision.
func (p *PrometheusStore) translateAndExtendLabels(m []prompb.Label, extend labels.Labels) []storepb.Label {
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

	return extendLset(lset, extend)
}

func extendLset(lset []storepb.Label, extend labels.Labels) []storepb.Label {
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
func (p *PrometheusStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// LabelValues returns all known label values for a given label name.
func (p *PrometheusStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	u := *p.base
	u.Path = path.Join(u.Path, "/api/v1/label/", r.Label, "/values")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	span, ctx := tracing.StartSpan(ctx, "/prom_label_values HTTP[client]")
	defer span.Finish()

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
