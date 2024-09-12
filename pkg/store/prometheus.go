// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/blang/semver/v4"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/thanos-io/thanos/pkg/clientconfig"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// PrometheusStore implements the store node API on top of the Prometheus remote read API.
type PrometheusStore struct {
	logger           log.Logger
	base             *url.URL
	client           *promclient.Client
	buffers          sync.Pool
	component        component.StoreAPI
	externalLabelsFn func() labels.Labels

	promVersion func() string
	timestamps  func() (mint int64, maxt int64)

	remoteReadAcceptableResponses []prompb.ReadRequest_ResponseType

	framesRead prometheus.Histogram

	storepb.UnimplementedStoreServer
}

// Label{Values,Names} call with matchers is supported for Prometheus versions >= 2.24.0.
// https://github.com/prometheus/prometheus/commit/caa173d2aac4c390546b1f78302104b1ccae0878.
var baseVer, _ = semver.Make("2.24.0")

const initialBufSize = 32 * 1024 // 32KB seems like a good minimum starting size for sync pool size.

// NewPrometheusStore returns a new PrometheusStore that uses the given HTTP client
// to talk to Prometheus.
// It attaches the provided external labels to all results. Provided external labels has to be sorted.
func NewPrometheusStore(
	logger log.Logger,
	reg prometheus.Registerer,
	client *promclient.Client,
	baseURL *url.URL,
	component component.StoreAPI,
	externalLabelsFn func() labels.Labels,
	timestamps func() (mint int64, maxt int64),
	promVersion func() string,
) (*PrometheusStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	p := &PrometheusStore{
		logger:                        logger,
		base:                          baseURL,
		client:                        client,
		component:                     component,
		externalLabelsFn:              externalLabelsFn,
		promVersion:                   promVersion,
		timestamps:                    timestamps,
		remoteReadAcceptableResponses: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS, prompb.ReadRequest_SAMPLES},
		buffers: sync.Pool{New: func() interface{} {
			b := make([]byte, 0, initialBufSize)
			return &b
		}},
		framesRead: promauto.With(reg).NewHistogram(
			prometheus.HistogramOpts{
				Name:    "prometheus_store_received_frames",
				Help:    "Number of frames received per streamed response.",
				Buckets: prometheus.ExponentialBuckets(10, 10, 5),
			},
		),
	}
	return p, nil
}

func (p *PrometheusStore) labelCallsSupportMatchers() bool {
	version, parseErr := semver.Parse(p.promVersion())
	return parseErr == nil && version.GTE(baseVer)
}

func (p *PrometheusStore) getBuffer() *[]byte {
	b := p.buffers.Get()
	return b.(*[]byte)
}

func (p *PrometheusStore) putBuffer(b *[]byte) {
	p.buffers.Put(b)
}

// Series returns all series for a requested time range and label matcher.
func (p *PrometheusStore) Series(r *storepb.SeriesRequest, seriesSrv storepb.Store_SeriesServer) error {
	s := newFlushableServer(seriesSrv, sortingStrategyStore)

	extLset := p.externalLabelsFn()

	match, matchers, err := matchesExternalLabels(r.Matchers, extLset)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return nil
	}
	if len(matchers) == 0 {
		return status.Error(codes.InvalidArgument, "no matchers specified (excluding external labels)")
	}

	// Don't ask for more than available time. This includes potential `minTime` flag limit.
	availableMinTime, _ := p.timestamps()
	if r.MinTime < availableMinTime {
		r.MinTime = availableMinTime
	}

	extLsetToRemove := map[string]struct{}{}
	for _, lbl := range r.WithoutReplicaLabels {
		extLsetToRemove[lbl] = struct{}{}
	}

	if r.SkipChunks {
		finalExtLset := rmLabels(extLset.Copy(), extLsetToRemove)
		labelMaps, err := p.client.SeriesInGRPC(s.Context(), p.base, matchers, r.MinTime, r.MaxTime, int(r.Limit))
		if err != nil {
			return err
		}
		var b labels.Builder
		for _, lbm := range labelMaps {
			b.Reset(labels.EmptyLabels())
			for k, v := range lbm {
				b.Set(k, v)
			}
			// external labels should take precedence
			finalExtLset.Range(func(l labels.Label) {
				b.Set(l.Name, l.Value)
			})
			lset := labelpb.PromLabelsToLabelpbLabels(b.Labels())
			if err = s.Send(storepb.NewSeriesResponse(&storepb.Series{Labels: lset})); err != nil {
				return err
			}
		}
		return s.Flush()
	}

	shardMatcher := r.ShardInfo.Matcher(&p.buffers)
	defer shardMatcher.Close()

	q := &prompb.Query{StartTimestampMs: r.MinTime, EndTimestampMs: r.MaxTime}
	for _, m := range matchers {
		pm := &prompb.LabelMatcher{Name: m.Name, Value: m.Value}

		switch m.Type {
		case labels.MatchEqual:
			pm.Type = prompb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			pm.Type = prompb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			pm.Type = prompb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			pm.Type = prompb.LabelMatcher_NRE
		default:
			return errors.New("unrecognized matcher type")
		}
		q.Matchers = append(q.Matchers, pm)
	}

	queryPrometheusSpan, ctx := tracing.StartSpan(s.Context(), "query_prometheus")
	queryPrometheusSpan.SetTag("query.request", q.String())

	httpResp, err := p.startPromRemoteRead(ctx, q)
	if err != nil {
		queryPrometheusSpan.Finish()
		return errors.Wrap(err, "query Prometheus")
	}

	// Negotiate content. We requested streamed chunked response type, but still we need to support old versions of
	// remote read.
	contentType := httpResp.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "application/x-protobuf") {
		return p.handleSampledPrometheusResponse(s, httpResp, queryPrometheusSpan, extLset, enableChunkHashCalculation, extLsetToRemove)
	}

	if !strings.HasPrefix(contentType, "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse") {
		return errors.Errorf("not supported remote read content type: %s", contentType)
	}
	return p.handleStreamedPrometheusResponse(s, shardMatcher, httpResp, queryPrometheusSpan, extLset, enableChunkHashCalculation, extLsetToRemove)
}

func (p *PrometheusStore) handleSampledPrometheusResponse(
	s flushableServer,
	httpResp *http.Response,
	querySpan tracing.Span,
	extLset labels.Labels,
	calculateChecksums bool,
	extLsetToRemove map[string]struct{},
) error {
	level.Debug(p.logger).Log("msg", "started handling ReadRequest_SAMPLED response type.")

	resp, err := p.fetchSampledResponse(s.Context(), httpResp)
	querySpan.Finish()
	if err != nil {
		return err
	}

	span, _ := tracing.StartSpan(s.Context(), "transform_and_respond")
	defer span.Finish()
	span.SetTag("series_count", len(resp.Results[0].Timeseries))

	for _, e := range resp.Results[0].Timeseries {
		// https://github.com/prometheus/prometheus/blob/3f6f5d3357e232abe53f1775f893fdf8f842712c/storage/remote/read_handler.go#L166
		// MergeLabels() prefers local labels over external labels but we prefer
		// external labels hence we need to do this:
		lset := rmLabels(labelpb.ExtendSortedLabels(labelpb.LabelpbLabelsToPromLabels(e.Labels), extLset), extLsetToRemove)
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

		aggregatedChunks, err := p.chunkSamples(e, MaxSamplesPerChunk, calculateChecksums)
		if err != nil {
			return err
		}

		if err := s.Send(storepb.NewSeriesResponse(&storepb.Series{
			Labels: labelpb.PromLabelsToLabelpbLabels(lset),
			Chunks: aggregatedChunks,
		})); err != nil {
			return err
		}
	}
	level.Debug(p.logger).Log("msg", "handled ReadRequest_SAMPLED request.", "series", len(resp.Results[0].Timeseries))
	return s.Flush()
}

func (p *PrometheusStore) handleStreamedPrometheusResponse(
	s flushableServer,
	shardMatcher *storepb.ShardMatcher,
	httpResp *http.Response,
	querySpan tracing.Span,
	extLset labels.Labels,
	calculateChecksums bool,
	extLsetToRemove map[string]struct{},
) error {
	level.Debug(p.logger).Log("msg", "started handling ReadRequest_STREAMED_XOR_CHUNKS streamed read response.")

	framesNum := 0

	defer func() {
		p.framesRead.Observe(float64(framesNum))
		querySpan.SetTag("frames", framesNum)
		querySpan.Finish()
	}()
	defer runutil.CloseWithLogOnErr(p.logger, httpResp.Body, "prom series request body")

	var data = p.getBuffer()
	defer p.putBuffer(data)

	bodySizer := NewBytesRead(httpResp.Body)
	seriesStats := &storepb.SeriesStatsCounter{}

	// TODO(bwplotka): Put read limit as a flag.
	stream := NewChunkedReader(bodySizer, remote.DefaultChunkedReadLimit, *data)
	hasher := hashPool.Get().(hash.Hash64)
	defer hashPool.Put(hasher)
	for {
		res := &prompb.ChunkedReadResponse{}
		err := stream.NextProto(res)
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "next proto")
		}

		if len(res.ChunkedSeries) != 1 {
			level.Warn(p.logger).Log("msg", "Prometheus ReadRequest_STREAMED_XOR_CHUNKS returned non 1 series in frame", "series", len(res.ChunkedSeries))
		}

		framesNum++
		for _, series := range res.ChunkedSeries {
			// MergeLabels() prefers local labels over external labels but we prefer
			// external labels hence we need to do this:
			// https://github.com/prometheus/prometheus/blob/3f6f5d3357e232abe53f1775f893fdf8f842712c/storage/remote/codec.go#L210.
			completeLabelset := rmLabels(labelpb.ExtendSortedLabels(labelpb.LabelpbLabelsToPromLabels(series.Labels), extLset), extLsetToRemove)
			if !shardMatcher.MatchesLabels(labelpb.PromLabelsToLabelpbLabels(completeLabelset)) {
				continue
			}

			seriesStats.CountSeries(series.Labels)
			thanosChks := make([]*storepb.AggrChunk, len(series.Chunks))

			for i, chk := range series.Chunks {
				chkHash := hashChunk(hasher, chk.Data, calculateChecksums)
				thanosChks[i] = &storepb.AggrChunk{
					MaxTime: chk.MaxTimeMs,
					MinTime: chk.MinTimeMs,
					Raw: &storepb.Chunk{
						Data: chk.Data,
						// Prometheus ChunkEncoding vs ours https://github.com/thanos-io/thanos/blob/master/pkg/store/storepb/types.proto#L19
						// has one difference. Prometheus has Chunk_UNKNOWN Chunk_Encoding = 0 vs we start from
						// XOR as 0. Compensate for that here:
						Type: storepb.Chunk_Encoding(chk.Type - 1),
						Hash: chkHash,
					},
				}
				seriesStats.Samples += thanosChks[i].Raw.XORNumSamples()
				seriesStats.Chunks++

				// Drop the reference to data from non protobuf for GC.
				series.Chunks[i].Data = nil
			}

			r := storepb.NewSeriesResponse(&storepb.Series{
				Labels: labelpb.PromLabelsToLabelpbLabels(completeLabelset),
				Chunks: thanosChks,
			})
			if err := s.Send(r); err != nil {
				return err
			}
		}
	}

	querySpan.SetTag("processed.series", seriesStats.Series)
	querySpan.SetTag("processed.chunks", seriesStats.Chunks)
	querySpan.SetTag("processed.samples", seriesStats.Samples)
	querySpan.SetTag("processed.bytes", bodySizer.BytesCount())
	level.Debug(p.logger).Log("msg", "handled ReadRequest_STREAMED_XOR_CHUNKS request.", "frames", framesNum)

	return s.Flush()
}

type BytesCounter struct {
	io.ReadCloser
	bytesCount int
}

func NewBytesRead(rc io.ReadCloser) *BytesCounter {
	return &BytesCounter{ReadCloser: rc}
}

func (s *BytesCounter) Read(p []byte) (n int, err error) {
	n, err = s.ReadCloser.Read(p)
	s.bytesCount += n
	return n, err
}

func (s *BytesCounter) BytesCount() int {
	return s.bytesCount
}

func (p *PrometheusStore) fetchSampledResponse(ctx context.Context, resp *http.Response) (_ *prompb.ReadResponse, err error) {
	defer runutil.ExhaustCloseWithLogOnErr(p.logger, resp.Body, "prom series request body")

	b := p.getBuffer()
	buf := bytes.NewBuffer(*b)
	defer p.putBuffer(b)
	if _, err := io.Copy(buf, resp.Body); err != nil {
		return nil, errors.Wrap(err, "copy response")
	}

	sb := p.getBuffer()
	var decomp []byte
	tracing.DoInSpan(ctx, "decompress_response", func(ctx context.Context) {
		decomp, err = snappy.Decode(*sb, buf.Bytes())
	})
	defer p.putBuffer(sb)
	if err != nil {
		return nil, errors.Wrap(err, "decompress response")
	}

	var data prompb.ReadResponse
	tracing.DoInSpan(ctx, "unmarshal_response", func(ctx context.Context) {
		err = proto.Unmarshal(decomp, &data)
	})
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal response")
	}
	if len(data.Results) != 1 {
		return nil, errors.Errorf("unexpected result size %d", len(data.Results))
	}

	return &data, nil
}

func (p *PrometheusStore) chunkSamples(series *prompb.TimeSeries, maxSamplesPerChunk int, calculateChecksums bool) (chks []*storepb.AggrChunk, err error) {
	samples := series.Samples
	hasher := hashPool.Get().(hash.Hash64)
	defer hashPool.Put(hasher)

	for len(samples) > 0 {
		chunkSize := len(samples)
		if chunkSize > maxSamplesPerChunk {
			chunkSize = maxSamplesPerChunk
		}

		enc, cb, err := p.encodeChunk(samples[:chunkSize])
		if err != nil {
			return nil, status.Error(codes.Unknown, err.Error())
		}

		chkHash := hashChunk(hasher, cb, calculateChecksums)
		chks = append(chks, &storepb.AggrChunk{
			MinTime: samples[0].Timestamp,
			MaxTime: samples[chunkSize-1].Timestamp,
			Raw:     &storepb.Chunk{Type: enc, Data: cb, Hash: chkHash},
		})

		samples = samples[chunkSize:]
	}

	return chks, nil
}

func (p *PrometheusStore) startPromRemoteRead(ctx context.Context, q *prompb.Query) (presp *http.Response, err error) {
	reqb, err := proto.Marshal(&prompb.ReadRequest{
		Queries:               []*prompb.Query{q},
		AcceptedResponseTypes: p.remoteReadAcceptableResponses,
	})
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
	preq.Header.Set("Content-Type", "application/x-stream-protobuf")
	preq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	preq.Header.Set("User-Agent", clientconfig.ThanosUserAgent)
	presp, err = p.client.Do(preq.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "send request")
	}
	if presp.StatusCode/100 != 2 {
		// Best effort read.
		b, err := io.ReadAll(presp.Body)
		if err != nil {
			level.Error(p.logger).Log("msg", "failed to read response from non 2XX remote read request", "err", err)
		}
		_ = presp.Body.Close()
		return nil, errors.Errorf("request failed with code %s; msg %s", presp.Status, string(b))
	}

	return presp, nil
}

// matchesExternalLabels returns false if given matchers are not matching external labels.
// If true, matchesExternalLabels also returns Prometheus matchers without those matching external labels.
func matchesExternalLabels(ms []*storepb.LabelMatcher, externalLabels labels.Labels) (bool, []*labels.Matcher, error) {
	tms, err := storepb.MatchersToPromMatchers(ms...)
	if err != nil {
		return false, nil, err
	}

	if externalLabels.IsEmpty() {
		return true, tms, nil
	}

	var newMatchers []*labels.Matcher
	for i, tm := range tms {
		// Validate all matchers.
		extValue := externalLabels.Get(tm.Name)
		if extValue == "" {
			// Agnostic to external labels.
			tms = append(tms[:i], tms[i:]...)
			newMatchers = append(newMatchers, tm)
			continue
		}

		if !tm.Matches(extValue) {
			// External label does not match. This should not happen - it should be filtered out on query node,
			// but let's do that anyway here.
			return false, nil, nil
		}
	}
	return true, newMatchers, nil
}

// encodeChunk translates the sample pairs into a chunk.
// TODO(kakkoyun): Linter - result 0 (github.com/thanos-io/thanos/pkg/store/storepb.Chunk_Encoding) is always 0.
func (p *PrometheusStore) encodeChunk(ss []*prompb.Sample) (storepb.Chunk_Encoding, []byte, error) { //nolint:unparam
	c := chunkenc.NewXORChunk()

	a, err := c.Appender()
	if err != nil {
		return 0, nil, err
	}
	for _, s := range ss {
		a.Append(s.Timestamp, s.Value)
	}
	return storepb.Chunk_XOR, c.Bytes(), nil
}

// LabelNames returns all known label names of series that match the given matchers.
func (p *PrometheusStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	extLset := p.externalLabelsFn()

	match, matchers, err := matchesExternalLabels(r.Matchers, extLset)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return &storepb.LabelNamesResponse{Names: nil}, nil
	}

	var lbls []string
	if len(matchers) == 0 || p.labelCallsSupportMatchers() {
		lbls, err = p.client.LabelNamesInGRPC(ctx, p.base, matchers, r.Start, r.End, int(r.Limit))
		if err != nil {
			return nil, err
		}
	} else {
		sers, err := p.client.SeriesInGRPC(ctx, p.base, matchers, r.Start, r.End, int(r.Limit))
		if err != nil {
			return nil, err
		}

		// Using set to handle duplicate values.
		labelNamesSet := make(map[string]struct{})
		for _, s := range sers {
			for labelName := range s {
				labelNamesSet[labelName] = struct{}{}
			}
		}

		for key := range labelNamesSet {
			lbls = append(lbls, key)
		}
	}

	extLsetToRemove := map[string]struct{}{}
	for _, lbl := range r.WithoutReplicaLabels {
		extLsetToRemove[lbl] = struct{}{}
	}

	if len(lbls) > 0 {
		extLset.Range(func(l labels.Label) {
			if _, ok := extLsetToRemove[l.Name]; !ok {
				lbls = append(lbls, l.Name)
			}
		})
		sort.Strings(lbls)
	}

	return &storepb.LabelNamesResponse{Names: lbls}, nil
}

// LabelValues returns all known label values for a given label name.
func (p *PrometheusStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	if r.Label == "" {
		return nil, status.Error(codes.InvalidArgument, "label name parameter cannot be empty")
	}
	for i := range r.WithoutReplicaLabels {
		if r.Label == r.WithoutReplicaLabels[i] {
			return &storepb.LabelValuesResponse{}, nil
		}
	}

	extLset := p.externalLabelsFn()

	match, matchers, err := matchesExternalLabels(r.Matchers, extLset)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return &storepb.LabelValuesResponse{}, nil
	}

	var (
		sers []map[string]string
		vals []string
	)

	// If we request label values for an external label while selecting an additional matcher for other label values
	if val := extLset.Get(r.Label); val != "" {
		if len(matchers) == 0 {
			return &storepb.LabelValuesResponse{Values: []string{val}}, nil
		}
		sers, err = p.client.SeriesInGRPC(ctx, p.base, matchers, r.Start, r.End, int(r.Limit))
		if err != nil {
			return nil, err
		}
		if len(sers) > 0 {
			return &storepb.LabelValuesResponse{Values: []string{val}}, nil
		}
		return &storepb.LabelValuesResponse{}, nil
	}

	if len(matchers) == 0 || p.labelCallsSupportMatchers() {
		vals, err = p.client.LabelValuesInGRPC(ctx, p.base, r.Label, matchers, r.Start, r.End, int(r.Limit))
		if err != nil {
			return nil, err
		}
	} else {
		sers, err = p.client.SeriesInGRPC(ctx, p.base, matchers, r.Start, r.End, int(r.Limit))
		if err != nil {
			return nil, err
		}

		// Using set to handle duplicate values.
		labelValuesSet := make(map[string]struct{})
		for _, s := range sers {
			if val, exists := s[r.Label]; exists {
				labelValuesSet[val] = struct{}{}
			}
		}
		for key := range labelValuesSet {
			vals = append(vals, key)
		}
	}

	sort.Strings(vals)
	return &storepb.LabelValuesResponse{Values: vals}, nil
}

func (p *PrometheusStore) LabelSet() []*labelpb.LabelSet {
	labels := labelpb.PromLabelsToLabelpbLabels(p.externalLabelsFn())

	labelset := []*labelpb.LabelSet{}
	if len(labels) > 0 {
		labelset = append(labelset, &labelpb.LabelSet{
			Labels: labels,
		})
	}

	return labelset
}

func (p *PrometheusStore) TSDBInfos() []*infopb.TSDBInfo {
	labels := p.LabelSet()
	if len(labels) == 0 {
		return []*infopb.TSDBInfo{}
	}

	mint, maxt := p.Timestamps()
	return []*infopb.TSDBInfo{
		{
			Labels: &labelpb.LabelSet{
				Labels: labels[0].Labels,
			},
			MinTime: mint,
			MaxTime: maxt,
		},
	}
}

func (p *PrometheusStore) Timestamps() (mint int64, maxt int64) {
	return p.timestamps()
}

// NewChunkedReader constructs a ChunkedReader.
// It allows passing data slice for byte slice reuse, which will be increased to needed size if smaller.
func NewChunkedReader(r io.Reader, sizeLimit uint64, data []byte) *ChunkedReader {
	return &ChunkedReader{b: bufio.NewReader(r), sizeLimit: sizeLimit, data: data, crc32: crc32.New(castagnoliTable)}
}

// Next returns the next length-delimited record from the input, or io.EOF if
// there are no more records available. Returns io.ErrUnexpectedEOF if a short
// record is found, with a length of n but fewer than n bytes of data.
// Next also verifies the given checksum with Castagnoli polynomial CRC-32 checksum.
//
// NOTE: The slice returned is valid only until a subsequent call to Next. It's a caller's responsibility to copy the
// returned slice if needed.
func (r *ChunkedReader) Next() ([]byte, error) {
	size, err := binary.ReadUvarint(r.b)
	if err != nil {
		return nil, err
	}

	if size > r.sizeLimit {
		return nil, fmt.Errorf("chunkedReader: message size exceeded the limit %v bytes; got: %v bytes", r.sizeLimit, size)
	}

	if cap(r.data) < int(size) {
		r.data = make([]byte, size)
	} else {
		r.data = r.data[:size]
	}

	var crc32 uint32
	if err := binary.Read(r.b, binary.BigEndian, &crc32); err != nil {
		return nil, err
	}

	r.crc32.Reset()
	if _, err := io.ReadFull(io.TeeReader(r.b, r.crc32), r.data); err != nil {
		return nil, err
	}

	if r.crc32.Sum32() != crc32 {
		return nil, errors.New("chunkedReader: corrupted frame; checksum mismatch")
	}
	return r.data, nil
}

// NextProto consumes the next available record by calling r.Next, and decodes
// it into the protobuf with proto.Unmarshal.
func (r *ChunkedReader) NextProto(pb proto.Message) error {
	rec, err := r.Next()
	if err != nil {
		return err
	}
	return proto.Unmarshal(rec, pb)
}

// ChunkedReader is a buffered reader that expects uvarint delimiter and checksum before each message.
// It will allocate as much as the biggest frame defined by delimiter (on top of bufio.Reader allocations).
type ChunkedReader struct {
	b         *bufio.Reader
	data      []byte
	sizeLimit uint64

	crc32 hash.Hash32
}

// DefaultChunkedReadLimit is the default value for the maximum size of the protobuf frame client allows.
// 50MB is the default. This is equivalent to ~100k full XOR chunks and average labelset.
const DefaultChunkedReadLimit = 5e+7

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}
