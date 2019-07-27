package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"path"
	"sort"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/prompb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var statusToCode = map[int]codes.Code{
	http.StatusBadRequest:          codes.InvalidArgument,
	http.StatusNotFound:            codes.NotFound,
	http.StatusUnprocessableEntity: codes.Internal,
	http.StatusServiceUnavailable:  codes.Unavailable,
	http.StatusInternalServerError: codes.Internal,
}

// PrometheusStore implements the store node API on top of the Prometheus remote read API.
type PrometheusStore struct {
	logger         log.Logger
	base           *url.URL
	client         *http.Client
	buffers        sync.Pool
	component      component.StoreAPI
	externalLabels func() labels.Labels
	timestamps     func() (mint int64, maxt int64)
}

// NewPrometheusStore returns a new PrometheusStore that uses the given HTTP client
// to talk to Prometheus.
// It attaches the provided external labels to all results.
func NewPrometheusStore(
	logger log.Logger,
	client *http.Client,
	baseURL *url.URL,
	component component.StoreAPI,
	externalLabels func() labels.Labels,
	timestamps func() (mint int64, maxt int64),
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
		component:      component,
		externalLabels: externalLabels,
		timestamps:     timestamps,
	}
	return p, nil
}

// Info returns store information about the Prometheus instance.
// NOTE(bplotka): MaxTime & MinTime are not accurate nor adjusted dynamically.
// This is fine for now, but might be needed in future.
func (p *PrometheusStore) Info(ctx context.Context, r *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	lset := p.externalLabels()
	mint, maxt := p.timestamps()

	res := &storepb.InfoResponse{
		Labels:    make([]storepb.Label, 0, len(lset)),
		StoreType: p.component.ToProto(),
		MinTime:   mint,
		MaxTime:   maxt,
	}
	for _, l := range lset {
		res.Labels = append(res.Labels, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}

	// Until we deprecate the single labels in the reply, we just duplicate
	// them here for migration/compatibility purposes.
	res.LabelSets = []storepb.LabelSet{}
	if len(res.Labels) > 0 {
		res.LabelSets = append(res.LabelSets, storepb.LabelSet{
			Labels: res.Labels,
		})
	}
	return res, nil
}

func (p *PrometheusStore) getBuffer() *[]byte {
	b := p.buffers.Get()
	if b == nil {
		buf := make([]byte, 0, 32*1024) // 32KB seems like a good minimum starting size.
		return &buf
	}
	return b.(*[]byte)
}

func (p *PrometheusStore) putBuffer(b *[]byte) {
	p.buffers.Put(b)
}

// Series returns all series for a requested time range and label matcher.
func (p *PrometheusStore) Series(r *storepb.SeriesRequest, s storepb.Store_SeriesServer) error {
	externalLabels := p.externalLabels()

	match, newMatchers, err := matchesExternalLabels(r.Matchers, externalLabels)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	if !match {
		return nil
	}

	if len(newMatchers) == 0 {
		return status.Error(codes.InvalidArgument, errors.New("no matchers specified (excluding external labels)").Error())
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
	span.SetTag("series_count", len(resp.Results[0].Timeseries))

	for _, e := range resp.Results[0].Timeseries {
		lset := p.translateAndExtendLabels(e.Labels, externalLabels)

		if len(e.Samples) == 0 {
			// As found in https://github.com/thanos-io/thanos/issues/381
			// Prometheus can give us completely empty time series. Ignore these with log until we figure out that
			// this is expected from Prometheus perspective.
			level.Warn(p.logger).Log(
				"msg",
				"found timeseries without any chunk. See https://github.com/thanos-io/thanos/issues/381 for details",
				"lset",
				fmt.Sprintf("%v", lset),
			)
			continue
		}

		// XOR encoding supports a max size of 2^16 - 1 samples, so we need
		// to chunk all samples into groups of no more than 2^16 - 1
		// See: https://github.com/thanos-io/thanos/pull/718
		aggregatedChunks, err := p.chunkSamples(e, math.MaxUint16)
		if err != nil {
			return err
		}

		resp := storepb.NewSeriesResponse(&storepb.Series{
			Labels: lset,
			Chunks: aggregatedChunks,
		})
		if err := s.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func (p *PrometheusStore) chunkSamples(series prompb.TimeSeries, maxSamplesPerChunk int) (chks []storepb.AggrChunk, err error) {
	samples := series.Samples

	for len(samples) > 0 {
		chunkSize := len(samples)
		if chunkSize > maxSamplesPerChunk {
			chunkSize = maxSamplesPerChunk
		}

		enc, cb, err := p.encodeChunk(samples[:chunkSize])
		if err != nil {
			return nil, status.Error(codes.Unknown, err.Error())
		}

		chks = append(chks, storepb.AggrChunk{
			MinTime: int64(samples[0].Timestamp),
			MaxTime: int64(samples[chunkSize-1].Timestamp),
			Raw:     &storepb.Chunk{Type: enc, Data: cb},
		})

		samples = samples[chunkSize:]
	}

	return chks, nil
}

func (p *PrometheusStore) promSeries(ctx context.Context, q prompb.Query) (*prompb.ReadResponse, error) {
	span, ctx := tracing.StartSpan(ctx, "query_prometheus")
	defer span.Finish()

	reqb, err := proto.Marshal(&prompb.ReadRequest{Queries: []prompb.Query{q}})
	if err != nil {
		return nil, errors.Wrap(err, "marshal read request")
	}

	u := *p.base
	u.Path = path.Join(u.Path, "api/v1/read")

	preq, err := http.NewRequest("POST", u.String(), bytes.NewReader(snappy.Encode(nil, reqb)))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create request")
	}
	preq.Header.Add("Content-Encoding", "snappy")
	preq.Header.Set("Content-Type", "application/x-protobuf")
	preq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")
	spanReqDo, ctx := tracing.StartSpan(ctx, "query_prometheus_request")
	preq = preq.WithContext(ctx)
	presp, err := p.client.Do(preq)
	if err != nil {
		return nil, errors.Wrap(err, "send request")
	}
	spanReqDo.Finish()
	defer runutil.ExhaustCloseWithLogOnErr(p.logger, presp.Body, "prom series request body")

	if presp.StatusCode/100 != 2 {
		return nil, errors.Errorf("request failed with code %s", presp.Status)
	}

	c := p.getBuffer()
	buf := bytes.NewBuffer(*c)
	defer p.putBuffer(c)
	if _, err := io.Copy(buf, presp.Body); err != nil {
		return nil, errors.Wrap(err, "copy response")
	}

	spanSnappyDecode, ctx := tracing.StartSpan(ctx, "decompress_response")
	sc := p.getBuffer()
	decomp, err := snappy.Decode(*sc, buf.Bytes())
	spanSnappyDecode.Finish()
	defer p.putBuffer(sc)
	if err != nil {
		return nil, errors.Wrap(err, "decompress response")
	}

	var data prompb.ReadResponse
	spanUnmarshal, _ := tracing.StartSpan(ctx, "unmarshal_response")
	if err := proto.Unmarshal(decomp, &data); err != nil {
		return nil, errors.Wrap(err, "unmarshal response")
	}
	spanUnmarshal.Finish()
	if len(data.Results) != 1 {
		return nil, errors.Errorf("unexepected result size %d", len(data.Results))
	}
	return &data, nil
}

// matchesExternalLabels filters out external labels matching from matcher if exsits as the local storage does not have them.
// It also returns false if given matchers are not matching external labels.
func matchesExternalLabels(ms []storepb.LabelMatcher, externalLabels labels.Labels) (bool, []storepb.LabelMatcher, error) {
	if len(externalLabels) == 0 {
		return true, ms, nil
	}

	var newMatcher []storepb.LabelMatcher
	for _, m := range ms {
		// Validate all matchers.
		tm, err := translateMatcher(m)
		if err != nil {
			return false, nil, err
		}

		extValue := externalLabels.Get(m.Name)
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
func (p *PrometheusStore) LabelNames(ctx context.Context, _ *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	u := *p.base
	u.Path = path.Join(u.Path, "/api/v1/labels")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	span, ctx := tracing.StartSpan(ctx, "/prom_label_names HTTP[client]")
	defer span.Finish()

	resp, err := p.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer runutil.ExhaustCloseWithLogOnErr(p.logger, resp.Body, "label names request body")

	if resp.StatusCode/100 != 2 {
		return nil, status.Error(codes.Internal, fmt.Sprintf("request Prometheus server failed, code %s", resp.Status))
	}

	if resp.StatusCode == http.StatusNoContent {
		return &storepb.LabelNamesResponse{Names: []string{}}, nil
	}

	var m struct {
		Data   []string `json:"data"`
		Status string   `json:"status"`
		Error  string   `json:"error"`
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err = json.Unmarshal(body, &m); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if m.Status != "success" {
		code, exists := statusToCode[resp.StatusCode]
		if !exists {
			return nil, status.Error(codes.Internal, m.Error)
		}
		return nil, status.Error(code, m.Error)
	}

	return &storepb.LabelNamesResponse{Names: m.Data}, nil
}

// LabelValues returns all known label values for a given label name.
func (p *PrometheusStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	externalLset := p.externalLabels()

	// First check for matching external label which has priority.
	if l := externalLset.Get(r.Label); l != "" {
		return &storepb.LabelValuesResponse{Values: []string{l}}, nil
	}

	u := *p.base
	u.Path = path.Join(u.Path, "/api/v1/label/", r.Label, "/values")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	span, ctx := tracing.StartSpan(ctx, "/prom_label_values HTTP[client]")
	defer span.Finish()

	resp, err := p.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer runutil.ExhaustCloseWithLogOnErr(p.logger, resp.Body, "label values request body")

	if resp.StatusCode/100 != 2 {
		return nil, status.Error(codes.Internal, fmt.Sprintf("request Prometheus server failed, code %s", resp.Status))
	}

	if resp.StatusCode == http.StatusNoContent {
		return &storepb.LabelValuesResponse{Values: []string{}}, nil
	}

	var m struct {
		Data   []string `json:"data"`
		Status string   `json:"status"`
		Error  string   `json:"error"`
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err = json.Unmarshal(body, &m); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	sort.Strings(m.Data)

	if m.Status != "success" {
		code, exists := statusToCode[resp.StatusCode]
		if !exists {
			return nil, status.Error(codes.Internal, m.Error)
		}
		return nil, status.Error(code, m.Error)
	}

	return &storepb.LabelValuesResponse{Values: m.Data}, nil
}
