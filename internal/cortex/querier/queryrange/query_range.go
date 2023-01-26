// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/thanos-io/thanos/internal/cortex/cortexpb"
	"github.com/thanos-io/thanos/internal/cortex/util"
	"github.com/thanos-io/thanos/internal/cortex/util/spanlogger"
)

// StatusSuccess Prometheus success result.
const StatusSuccess = "success"

var (
	matrix = model.ValMatrix.String()
	json   = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: true,
	}.Froze()
	errEndBeforeStart = httpgrpc.Errorf(http.StatusBadRequest, "end timestamp must not be before start time")
	errNegativeStep   = httpgrpc.Errorf(http.StatusBadRequest, "zero or negative query resolution step widths are not accepted. Try a positive integer")
	errStepTooSmall   = httpgrpc.Errorf(http.StatusBadRequest, "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")

	// PrometheusCodec is a codec to encode and decode Prometheus query range requests and responses.
	PrometheusCodec Codec = &prometheusCodec{}

	// Name of the cache control header.
	cacheControlHeader = "Cache-Control"
)

// Codec is used to encode/decode query range requests and responses so they can be passed down to middlewares.
type Codec interface {
	Merger
	// DecodeRequest decodes a Request from an http request.
	DecodeRequest(_ context.Context, request *http.Request, forwardHeaders []string) (Request, error)
	// DecodeResponse decodes a Response from an http response.
	// The original request is also passed as a parameter this is useful for implementation that needs the request
	// to merge result or build the result correctly.
	DecodeResponse(context.Context, *http.Response, Request) (Response, error)
	// EncodeRequest encodes a Request into an http request.
	EncodeRequest(context.Context, Request) (*http.Request, error)
	// EncodeResponse encodes a Response into an http response.
	EncodeResponse(context.Context, Response) (*http.Response, error)
}

// Merger is used by middlewares making multiple requests to merge back all responses into a single one.
type Merger interface {
	// MergeResponse merges responses from multiple requests into a single Response
	MergeResponse(...Response) (Response, error)
}

// Request represents a query range request that can be process by middlewares.
type Request interface {
	// GetStart returns the start timestamp of the request in milliseconds.
	GetStart() int64
	// GetEnd returns the end timestamp of the request in milliseconds.
	GetEnd() int64
	// GetStep returns the step of the request in milliseconds.
	GetStep() int64
	// GetQuery returns the query of the request.
	GetQuery() string
	// GetCachingOptions returns the caching options.
	GetCachingOptions() CachingOptions
	// WithStartEnd clone the current request with different start and end timestamp.
	WithStartEnd(startTime int64, endTime int64) Request
	// WithQuery clone the current request with a different query.
	WithQuery(string) Request
	proto.Message
	// LogToSpan writes information about this request to an OpenTracing span
	LogToSpan(opentracing.Span)
	// GetStats returns the stats of the request.
	GetStats() string
	// WithStats clones the current `PrometheusRequest` with a new stats.
	WithStats(stats string) Request
}

// Response represents a query range response.
type Response interface {
	proto.Message
	// GetHeaders returns the HTTP headers in the response.
	GetHeaders() []*PrometheusResponseHeader
	// GetStats returns the Prometheus query stats in the response.
	GetStats() *PrometheusResponseStats
}

type prometheusCodec struct{}

// WithStartEnd clones the current `PrometheusRequest` with a new `start` and `end` timestamp.
func (q *PrometheusRequest) WithStartEnd(start int64, end int64) Request {
	new := *q
	new.Start = start
	new.End = end
	return &new
}

// WithQuery clones the current `PrometheusRequest` with a new query.
func (q *PrometheusRequest) WithQuery(query string) Request {
	new := *q
	new.Query = query
	return &new
}

// WithStats clones the current `PrometheusRequest` with a new stats.
func (q *PrometheusRequest) WithStats(stats string) Request {
	new := *q
	new.Stats = stats
	return &new
}

// LogToSpan logs the current `PrometheusRequest` parameters to the specified span.
func (q *PrometheusRequest) LogToSpan(sp opentracing.Span) {
	sp.LogFields(
		otlog.String("query", q.GetQuery()),
		otlog.String("start", timestamp.Time(q.GetStart()).String()),
		otlog.String("end", timestamp.Time(q.GetEnd()).String()),
		otlog.Int64("step (ms)", q.GetStep()),
	)
}

type byFirstTime []*PrometheusResponse

func (a byFirstTime) Len() int           { return len(a) }
func (a byFirstTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byFirstTime) Less(i, j int) bool { return a[i].minTime() < a[j].minTime() }

func (resp *PrometheusResponse) minTime() int64 {
	result := resp.Data.Result
	if len(result) == 0 {
		return -1
	}
	if len(result[0].Samples) == 0 {
		return -1
	}
	return result[0].Samples[0].TimestampMs
}

func (resp *PrometheusResponse) GetStats() *PrometheusResponseStats {
	return resp.Data.Stats
}

func (resp *PrometheusInstantQueryResponse) GetStats() *PrometheusResponseStats {
	return resp.Data.Stats
}

// NewEmptyPrometheusResponse returns an empty successful Prometheus query range response.
func NewEmptyPrometheusResponse() *PrometheusResponse {
	return &PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result:     []SampleStream{},
		},
	}
}

// NewEmptyPrometheusInstantQueryResponse returns an empty successful Prometheus query range response.
func NewEmptyPrometheusInstantQueryResponse() *PrometheusInstantQueryResponse {
	return &PrometheusInstantQueryResponse{
		Status: StatusSuccess,
		Data: PrometheusInstantQueryData{
			ResultType: model.ValVector.String(),
			Result: PrometheusInstantQueryResult{
				Result: &PrometheusInstantQueryResult_Vector{},
			},
		},
	}
}

func (prometheusCodec) MergeResponse(responses ...Response) (Response, error) {
	if len(responses) == 0 {
		return NewEmptyPrometheusResponse(), nil
	}

	promResponses := make([]*PrometheusResponse, 0, len(responses))
	// we need to pass on all the headers for results cache gen numbers.
	var resultsCacheGenNumberHeaderValues []string

	for _, res := range responses {
		promResponses = append(promResponses, res.(*PrometheusResponse))
		resultsCacheGenNumberHeaderValues = append(resultsCacheGenNumberHeaderValues, getHeaderValuesWithName(res, ResultsCacheGenNumberHeaderName)...)
	}

	// Merge the responses.
	sort.Sort(byFirstTime(promResponses))

	response := PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result:     matrixMerge(promResponses),
			Stats:      StatsMerge(responses),
		},
	}

	if len(resultsCacheGenNumberHeaderValues) != 0 {
		response.Headers = []*PrometheusResponseHeader{{
			Name:   ResultsCacheGenNumberHeaderName,
			Values: resultsCacheGenNumberHeaderValues,
		}}
	}

	return &response, nil
}

func (prometheusCodec) DecodeRequest(_ context.Context, r *http.Request, forwardHeaders []string) (Request, error) {
	var result PrometheusRequest
	var err error
	result.Start, err = util.ParseTime(r.FormValue("start"))
	if err != nil {
		return nil, decorateWithParamName(err, "start")
	}

	result.End, err = util.ParseTime(r.FormValue("end"))
	if err != nil {
		return nil, decorateWithParamName(err, "end")
	}

	if result.End < result.Start {
		return nil, errEndBeforeStart
	}

	result.Step, err = parseDurationMs(r.FormValue("step"))
	if err != nil {
		return nil, decorateWithParamName(err, "step")
	}

	if result.Step <= 0 {
		return nil, errNegativeStep
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if (result.End-result.Start)/result.Step > 11000 {
		return nil, errStepTooSmall
	}

	result.Query = r.FormValue("query")
	result.Stats = r.FormValue("stats")
	result.Path = r.URL.Path

	// Include the specified headers from http request in prometheusRequest.
	for _, header := range forwardHeaders {
		for h, hv := range r.Header {
			if strings.EqualFold(h, header) {
				result.Headers = append(result.Headers, &PrometheusRequestHeader{Name: h, Values: hv})
				break
			}
		}
	}

	for _, value := range r.Header.Values(cacheControlHeader) {
		if strings.Contains(value, noStoreValue) {
			result.CachingOptions.Disabled = true
			break
		}
	}

	return &result, nil
}

func (prometheusCodec) EncodeRequest(ctx context.Context, r Request) (*http.Request, error) {
	promReq, ok := r.(*PrometheusRequest)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid request format")
	}
	params := url.Values{
		"start": []string{encodeTime(promReq.Start)},
		"end":   []string{encodeTime(promReq.End)},
		"step":  []string{encodeDurationMs(promReq.Step)},
		"query": []string{promReq.Query},
		"stats": []string{promReq.Stats},
	}
	u := &url.URL{
		Path:     promReq.Path,
		RawQuery: params.Encode(),
	}
	var h = http.Header{}

	for _, hv := range promReq.Headers {
		for _, v := range hv.Values {
			h.Add(hv.Name, v)
		}
	}

	req := &http.Request{
		Method:     "GET",
		RequestURI: u.String(), // This is what the httpgrpc code looks at.
		URL:        u,
		Body:       http.NoBody,
		Header:     h,
	}

	return req.WithContext(ctx), nil
}

func (prometheusCodec) DecodeResponse(ctx context.Context, r *http.Response, _ Request) (Response, error) {
	if r.StatusCode/100 != 2 {
		body, _ := ioutil.ReadAll(r.Body)
		return nil, httpgrpc.Errorf(r.StatusCode, string(body))
	}
	log, ctx := spanlogger.New(ctx, "ParseQueryRangeResponse") //nolint:ineffassign,staticcheck
	defer log.Finish()

	buf, err := BodyBuffer(r)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.LogFields(otlog.Int("bytes", len(buf)))

	var resp PrometheusResponse
	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
	}

	for h, hv := range r.Header {
		resp.Headers = append(resp.Headers, &PrometheusResponseHeader{Name: h, Values: hv})
	}
	return &resp, nil
}

// Buffer can be used to read a response body.
// This allows to avoid reading the body multiple times from the `http.Response.Body`.
type Buffer interface {
	Bytes() []byte
}

func BodyBuffer(res *http.Response) ([]byte, error) {
	// Attempt to cast the response body to a Buffer and use it if possible.
	// This is because the frontend may have already read the body and buffered it.
	if buffer, ok := res.Body.(Buffer); ok {
		return buffer.Bytes(), nil
	}
	// Preallocate the buffer with the exact size so we don't waste allocations
	// while progressively growing an initial small buffer. The buffer capacity
	// is increased by MinRead to avoid extra allocations due to how ReadFrom()
	// internally works.
	buf := bytes.NewBuffer(make([]byte, 0, res.ContentLength+bytes.MinRead))
	if _, err := buf.ReadFrom(res.Body); err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
	}
	return buf.Bytes(), nil
}

func (prometheusCodec) EncodeResponse(ctx context.Context, res Response) (*http.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "APIResponse.ToHTTPResponse")
	defer sp.Finish()

	a, ok := res.(*PrometheusResponse)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "invalid response format")
	}

	sp.LogFields(otlog.Int("series", len(a.Data.Result)))

	b, err := json.Marshal(a)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error encoding response: %v", err)
	}

	sp.LogFields(otlog.Int("bytes", len(b)))

	resp := http.Response{
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body:          ioutil.NopCloser(bytes.NewBuffer(b)),
		StatusCode:    http.StatusOK,
		ContentLength: int64(len(b)),
	}
	return &resp, nil
}

// UnmarshalJSON implements json.Unmarshaler and is used for unmarshalling
// a Prometheus range query response (matrix).
func (s *SampleStream) UnmarshalJSON(data []byte) error {
	var sampleStream model.SampleStream
	if err := json.Unmarshal(data, &sampleStream); err != nil {
		return err
	}

	s.Labels = cortexpb.FromMetricsToLabelAdapters(sampleStream.Metric)

	if len(sampleStream.Values) > 0 {
		s.Samples = make([]cortexpb.Sample, 0, len(sampleStream.Values))
		for _, sample := range sampleStream.Values {
			s.Samples = append(s.Samples, cortexpb.Sample{
				Value:       float64(sample.Value),
				TimestampMs: int64(sample.Timestamp),
			})
		}
	}

	if len(sampleStream.Histograms) > 0 {
		s.Histograms = make([]SampleHistogramPair, 0, len(sampleStream.Histograms))
		for _, h := range sampleStream.Histograms {
			s.Histograms = append(s.Histograms, fromModelSampleHistogramPair(h))
		}
	}

	return nil
}

// MarshalJSON implements json.Marshaler.
func (s *SampleStream) MarshalJSON() ([]byte, error) {
	var sampleStream model.SampleStream
	sampleStream.Metric = cortexpb.FromLabelAdaptersToMetric(s.Labels)

	sampleStream.Values = make([]model.SamplePair, 0, len(s.Samples))
	for _, sample := range s.Samples {
		sampleStream.Values = append(sampleStream.Values, model.SamplePair{
			Value:     model.SampleValue(sample.Value),
			Timestamp: model.Time(sample.TimestampMs),
		})
	}

	sampleStream.Histograms = make([]model.SampleHistogramPair, 0, len(s.Histograms))
	for _, h := range s.Histograms {
		sampleStream.Histograms = append(sampleStream.Histograms, toModelSampleHistogramPair(h))
	}

	return json.Marshal(sampleStream)
}

// UnmarshalJSON implements json.Unmarshaler and is used for unmarshalling
// a Prometheus instant query response (vector).
func (s *Sample) UnmarshalJSON(data []byte) error {
	var sample model.Sample
	if err := json.Unmarshal(data, &sample); err != nil {
		return err
	}
	s.Labels = cortexpb.FromMetricsToLabelAdapters(sample.Metric)
	s.SampleValue = float64(sample.Value)
	s.Timestamp = int64(sample.Timestamp)

	if sample.Histogram != nil {
		sh := fromModelSampleHistogram(sample.Histogram)
		s.Histogram = &sh
	} else {
		s.Histogram = nil
	}

	return nil
}

// MarshalJSON implements json.Marshaler.
func (s *Sample) MarshalJSON() ([]byte, error) {
	var sample model.Sample
	sample.Metric = cortexpb.FromLabelAdaptersToMetric(s.Labels)
	sample.Value = model.SampleValue(s.SampleValue)
	sample.Timestamp = model.Time(s.Timestamp)
	if s.Histogram != nil {
		msh := toModelSampleHistogram(*s.Histogram)
		sample.Histogram = &msh
	}
	return json.Marshal(sample)
}

// MarshalJSON implements json.Marshaler.
func (s StringSample) MarshalJSON() ([]byte, error) {
	v, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(model.String{
		Value:     s.Value,
		Timestamp: model.Time(s.TimestampMs),
	})
	if err != nil {
		return nil, err
	}
	return v, nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *StringSample) UnmarshalJSON(b []byte) error {
	var v model.String
	vs := [...]stdjson.Unmarshaler{&v}
	if err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(b, &vs); err != nil {
		return err
	}
	s.TimestampMs = int64(v.Timestamp)
	s.Value = v.Value
	return nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *PrometheusInstantQueryData) UnmarshalJSON(data []byte) error {
	var queryData struct {
		ResultType string                   `json:"resultType"`
		Result     jsoniter.RawMessage      `json:"result"`
		Stats      *PrometheusResponseStats `json:"stats,omitempty"`
	}

	if err := json.Unmarshal(data, &queryData); err != nil {
		return err
	}
	s.ResultType = queryData.ResultType
	s.Stats = queryData.Stats
	switch s.ResultType {
	case model.ValVector.String():
		var result struct {
			Samples []*Sample `json:"result"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return err
		}
		s.Result = PrometheusInstantQueryResult{
			Result: &PrometheusInstantQueryResult_Vector{Vector: &Vector{
				Samples: result.Samples,
			}},
		}
	case model.ValMatrix.String():
		var result struct {
			SampleStreams []*SampleStream `json:"result"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return err
		}
		s.Result = PrometheusInstantQueryResult{
			Result: &PrometheusInstantQueryResult_Matrix{Matrix: &Matrix{
				SampleStreams: result.SampleStreams,
			}},
		}
	case model.ValScalar.String():
		var result struct {
			Scalar cortexpb.Sample `json:"result"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return err
		}
		s.Result = PrometheusInstantQueryResult{
			Result: &PrometheusInstantQueryResult_Scalar{Scalar: &result.Scalar},
		}
	case model.ValString.String():
		var result struct {
			Sample model.String `json:"result"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return err
		}
		s.Result = PrometheusInstantQueryResult{
			Result: &PrometheusInstantQueryResult_StringSample{StringSample: &StringSample{
				TimestampMs: int64(result.Sample.Timestamp),
				Value:       result.Sample.Value,
			}},
		}
	default:
		return errors.New(fmt.Sprintf("%s result type not supported for PrometheusInstantQueryData", s.ResultType))
	}
	return nil
}

// MarshalJSON implements json.Marshaler.
func (s *PrometheusInstantQueryData) MarshalJSON() ([]byte, error) {
	switch s.ResultType {
	case model.ValVector.String():
		res := struct {
			ResultType string                   `json:"resultType"`
			Data       []*Sample                `json:"result"`
			Stats      *PrometheusResponseStats `json:"stats,omitempty"`
		}{
			ResultType: s.ResultType,
			Data:       s.Result.GetVector().Samples,
			Stats:      s.Stats,
		}
		return json.Marshal(res)
	case model.ValMatrix.String():
		res := struct {
			ResultType string                   `json:"resultType"`
			Data       []*SampleStream          `json:"result"`
			Stats      *PrometheusResponseStats `json:"stats,omitempty"`
		}{
			ResultType: s.ResultType,
			Data:       s.Result.GetMatrix().SampleStreams,
			Stats:      s.Stats,
		}
		return json.Marshal(res)
	case model.ValScalar.String():
		res := struct {
			ResultType string                   `json:"resultType"`
			Data       *cortexpb.Sample         `json:"result"`
			Stats      *PrometheusResponseStats `json:"stats,omitempty"`
		}{
			ResultType: s.ResultType,
			Data:       s.Result.GetScalar(),
			Stats:      s.Stats,
		}
		return json.Marshal(res)
	case model.ValString.String():
		res := struct {
			ResultType string                   `json:"resultType"`
			Data       *StringSample            `json:"result"`
			Stats      *PrometheusResponseStats `json:"stats,omitempty"`
		}{
			ResultType: s.ResultType,
			Data:       s.Result.GetStringSample(),
			Stats:      s.Stats,
		}
		return json.Marshal(res)
	default:
		return nil, errors.New(fmt.Sprintf("%s result type not supported for PrometheusInstantQueryData", s.ResultType))
	}
}

// StatsMerge merge the stats from 2 responses
// this function is similar to matrixMerge
func StatsMerge(resps []Response) *PrometheusResponseStats {
	output := map[int64]*PrometheusResponseQueryableSamplesStatsPerStep{}
	hasStats := false
	for _, resp := range resps {
		stats := resp.GetStats()
		if stats == nil {
			continue
		}

		hasStats = true
		if stats.Samples == nil {
			continue
		}

		for _, s := range stats.Samples.TotalQueryableSamplesPerStep {
			output[s.GetTimestampMs()] = s
		}
	}

	if !hasStats {
		return nil
	}

	keys := make([]int64, 0, len(output))
	for key := range output {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	result := &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{}}
	for _, key := range keys {
		result.Samples.TotalQueryableSamplesPerStep = append(result.Samples.TotalQueryableSamplesPerStep, output[key])
		result.Samples.TotalQueryableSamples += output[key].Value
	}

	return result
}

func matrixMerge(resps []*PrometheusResponse) []SampleStream {
	output := map[string]*SampleStream{}
	for _, resp := range resps {
		for _, stream := range resp.Data.Result {
			metric := cortexpb.FromLabelAdaptersToLabels(stream.Labels).String()
			existing, ok := output[metric]
			if !ok {
				existing = &SampleStream{
					Labels: stream.Labels,
				}
			}
			// We need to make sure we don't repeat samples. This causes some visualisations to be broken in Grafana.
			// The prometheus API is inclusive of start and end timestamps.
			if len(existing.Samples) > 0 && len(stream.Samples) > 0 {
				existingEndTs := existing.Samples[len(existing.Samples)-1].TimestampMs
				if existingEndTs == stream.Samples[0].TimestampMs {
					// Typically this the cases where only 1 sample point overlap,
					// so optimize with simple code.
					stream.Samples = stream.Samples[1:]
				} else if existingEndTs > stream.Samples[0].TimestampMs {
					// Overlap might be big, use heavier algorithm to remove overlap.
					stream.Samples = SliceSamples(stream.Samples, existingEndTs)
				} // else there is no overlap, yay!
			}
			// Same for histograms as for samples above.
			if len(existing.Histograms) > 0 && len(stream.Histograms) > 0 {
				existingEndTs := existing.Histograms[len(existing.Histograms)-1].GetTimestamp()
				if existingEndTs == stream.Histograms[0].GetTimestamp() {
					stream.Histograms = stream.Histograms[1:]
				} else if existingEndTs > stream.Histograms[0].GetTimestamp() {
					stream.Histograms = SliceHistogram(stream.Histograms, existingEndTs)
				}
			}

			if len(stream.Samples) > 0 {
				existing.Samples = append(existing.Samples, stream.Samples...)
			}

			if len(stream.Histograms) > 0 {
				existing.Histograms = append(existing.Histograms, stream.Histograms...)
			}

			output[metric] = existing
		}
	}

	keys := make([]string, 0, len(output))
	for key := range output {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make([]SampleStream, 0, len(output))
	for _, key := range keys {
		result = append(result, *output[key])
	}

	return result
}

// SliceSamples assumes given samples are sorted by timestamp in ascending order and
// return a sub slice whose first element's is the smallest timestamp that is strictly
// bigger than the given minTs. Empty slice is returned if minTs is bigger than all the
// timestamps in samples.
func SliceSamples(samples []cortexpb.Sample, minTs int64) []cortexpb.Sample {
	if len(samples) <= 0 || minTs < samples[0].TimestampMs {
		return samples
	}

	if len(samples) > 0 && minTs > samples[len(samples)-1].TimestampMs {
		return samples[len(samples):]
	}

	searchResult := sort.Search(len(samples), func(i int) bool {
		return samples[i].TimestampMs > minTs
	})

	return samples[searchResult:]
}

// SliceHistogram assumes given histogram are sorted by timestamp in ascending order and
// return a sub slice whose first element's is the smallest timestamp that is strictly
// bigger than the given minTs. Empty slice is returned if minTs is bigger than all the
// timestamps in histogram.
func SliceHistogram(histograms []SampleHistogramPair, minTs int64) []SampleHistogramPair {
	if len(histograms) <= 0 || minTs < histograms[0].GetTimestamp() {
		return histograms
	}

	if len(histograms) > 0 && minTs > histograms[len(histograms)-1].GetTimestamp() {
		return histograms[len(histograms):]
	}

	searchResult := sort.Search(len(histograms), func(i int) bool {
		return histograms[i].GetTimestamp() > minTs
	})

	return histograms[searchResult:]
}

func parseDurationMs(s string) (int64, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second/time.Millisecond)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, httpgrpc.Errorf(http.StatusBadRequest, "cannot parse %q to a valid duration. It overflows int64", s)
		}
		return int64(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return int64(d) / int64(time.Millisecond/time.Nanosecond), nil
	}
	return 0, httpgrpc.Errorf(http.StatusBadRequest, "cannot parse %q to a valid duration", s)
}

func encodeTime(t int64) string {
	f := float64(t) / 1.0e3
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func encodeDurationMs(d int64) string {
	return strconv.FormatFloat(float64(d)/float64(time.Second/time.Millisecond), 'f', -1, 64)
}

func decorateWithParamName(err error, field string) error {
	errTmpl := "invalid parameter %q; %v"
	if status, ok := status.FromError(err); ok {
		return httpgrpc.Errorf(int(status.Code()), errTmpl, field, status.Message())
	}
	return fmt.Errorf(errTmpl, field, err)
}

func PrometheusResponseQueryableSamplesStatsPerStepJsoniterDecode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	if !iter.ReadArray() {
		iter.ReportError("queryrange.PrometheusResponseQueryableSamplesStatsPerStep", "expected [")
		return
	}

	t := model.Time(iter.ReadFloat64() * float64(time.Second/time.Millisecond))

	if !iter.ReadArray() {
		iter.ReportError("queryrange.PrometheusResponseQueryableSamplesStatsPerStep", "expected ,")
		return
	}
	v := iter.ReadInt64()

	if iter.ReadArray() {
		iter.ReportError("queryrange.PrometheusResponseQueryableSamplesStatsPerStep", "expected ]")
	}

	*(*PrometheusResponseQueryableSamplesStatsPerStep)(ptr) = PrometheusResponseQueryableSamplesStatsPerStep{
		TimestampMs: int64(t),
		Value:       v,
	}
}

func PrometheusResponseQueryableSamplesStatsPerStepJsoniterEncode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	stats := (*PrometheusResponseQueryableSamplesStatsPerStep)(ptr)
	stream.WriteArrayStart()
	stream.WriteFloat64(float64(stats.TimestampMs) / float64(time.Second/time.Millisecond))
	stream.WriteMore()
	stream.WriteInt64(stats.Value)
	stream.WriteArrayEnd()
}

func init() {
	jsoniter.RegisterTypeEncoderFunc("queryrange.PrometheusResponseQueryableSamplesStatsPerStep", PrometheusResponseQueryableSamplesStatsPerStepJsoniterEncode, func(unsafe.Pointer) bool { return false })
	jsoniter.RegisterTypeDecoderFunc("queryrange.PrometheusResponseQueryableSamplesStatsPerStep", PrometheusResponseQueryableSamplesStatsPerStepJsoniterDecode)
}
