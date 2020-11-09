// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
)

// TODO(yeya24): add partial result when needed.
// ThanosRequest is a common interface defined for specific thanos requests.
type ThanosRequest interface {
	GetStoreMatchers() [][]*labels.Matcher
}

type ThanosQueryRangeRequest struct {
	Path                string
	Start               int64
	End                 int64
	Step                int64
	Timeout             time.Duration
	Query               string
	Dedup               bool
	PartialResponse     bool
	AutoDownsampling    bool
	MaxSourceResolution int64
	ReplicaLabels       []string
	StoreMatchers       [][]*labels.Matcher
	CachingOptions      queryrange.CachingOptions
}

// GetStart returns the start timestamp of the request in milliseconds.
func (r *ThanosQueryRangeRequest) GetStart() int64 { return r.Start }

// GetEnd returns the end timestamp of the request in milliseconds.
func (r *ThanosQueryRangeRequest) GetEnd() int64 { return r.End }

// GetStep returns the step of the request in milliseconds.
func (r *ThanosQueryRangeRequest) GetStep() int64 { return r.Step }

// GetQuery returns the query of the request.
func (r *ThanosQueryRangeRequest) GetQuery() string { return r.Query }

func (r *ThanosQueryRangeRequest) GetCachingOptions() queryrange.CachingOptions {
	return r.CachingOptions
}

// WithStartEnd clone the current request with different start and end timestamp.
func (r *ThanosQueryRangeRequest) WithStartEnd(start int64, end int64) queryrange.Request {
	q := *r
	q.Start = start
	q.End = end
	return &q
}

// WithQuery clone the current request with a different query.
func (r *ThanosQueryRangeRequest) WithQuery(query string) queryrange.Request {
	q := *r
	q.Query = query
	return &q
}

// LogToSpan writes information about this request to an OpenTracing span.
func (r *ThanosQueryRangeRequest) LogToSpan(sp opentracing.Span) {
	fields := []otlog.Field{
		otlog.String("query", r.GetQuery()),
		otlog.String("start", timestamp.Time(r.GetStart()).String()),
		otlog.String("end", timestamp.Time(r.GetEnd()).String()),
		otlog.Int64("step (ms)", r.GetStep()),
		otlog.Bool("dedup", r.Dedup),
		otlog.Bool("partial_response", r.PartialResponse),
		otlog.Object("replicaLabels", r.ReplicaLabels),
		otlog.Object("storeMatchers", r.StoreMatchers),
		otlog.Bool("auto-downsampling", r.AutoDownsampling),
		otlog.Int64("max_source_resolution (ms)", r.MaxSourceResolution),
	}

	sp.LogFields(fields...)
}

// Reset implements proto.Message interface required by queryrange.Request,
// which is not used in thanos.
func (r *ThanosQueryRangeRequest) Reset() {}

// String implements proto.Message interface required by queryrange.Request,
// which is not used in thanos.
func (r *ThanosQueryRangeRequest) String() string { return "" }

// ProtoMessage implements proto.Message interface required by queryrange.Request,
// which is not used in thanos.
func (r *ThanosQueryRangeRequest) ProtoMessage() {}

func (r *ThanosQueryRangeRequest) GetStoreMatchers() [][]*labels.Matcher { return r.StoreMatchers }

type ThanosLabelsRequest struct {
	Start           int64
	End             int64
	Label           string
	Path            string
	StoreMatchers   [][]*labels.Matcher
	PartialResponse bool
	CachingOptions  queryrange.CachingOptions
}

// GetStart returns the start timestamp of the request in milliseconds.
func (r *ThanosLabelsRequest) GetStart() int64 { return r.Start }

// GetEnd returns the end timestamp of the request in milliseconds.
func (r *ThanosLabelsRequest) GetEnd() int64 { return r.End }

// GetStep returns the step of the request in milliseconds. Returns 1 is a trick to avoid panic in
// https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/results_cache.go#L447.
func (r *ThanosLabelsRequest) GetStep() int64 { return 1 }

// GetQuery returns the query of the request.
func (r *ThanosLabelsRequest) GetQuery() string { return "" }

func (r *ThanosLabelsRequest) GetCachingOptions() queryrange.CachingOptions { return r.CachingOptions }

// WithStartEnd clone the current request with different start and end timestamp.
func (r *ThanosLabelsRequest) WithStartEnd(start int64, end int64) queryrange.Request {
	q := *r
	q.Start = start
	q.End = end
	return &q
}

// WithQuery clone the current request with a different query.
func (r *ThanosLabelsRequest) WithQuery(_ string) queryrange.Request {
	q := *r
	return &q
}

// LogToSpan writes information about this request to an OpenTracing span.
func (r *ThanosLabelsRequest) LogToSpan(sp opentracing.Span) {
	fields := []otlog.Field{
		otlog.String("start", timestamp.Time(r.GetStart()).String()),
		otlog.String("end", timestamp.Time(r.GetEnd()).String()),
		otlog.Bool("partial_response", r.PartialResponse),
		otlog.Object("storeMatchers", r.StoreMatchers),
	}
	if r.Label != "" {
		fields = append(fields, otlog.Object("label", r.Label))
	}

	sp.LogFields(fields...)
}

// Reset implements proto.Message interface required by queryrange.Request,
// which is not used in thanos.
func (r *ThanosLabelsRequest) Reset() {}

// String implements proto.Message interface required by queryrange.Request,
// which is not used in thanos.
func (r *ThanosLabelsRequest) String() string { return "" }

// ProtoMessage implements proto.Message interface required by queryrange.Request,
// which is not used in thanos.
func (r *ThanosLabelsRequest) ProtoMessage() {}

func (r *ThanosLabelsRequest) GetStoreMatchers() [][]*labels.Matcher { return r.StoreMatchers }

type ThanosSeriesRequest struct {
	Path            string
	Start           int64
	End             int64
	Dedup           bool
	PartialResponse bool
	ReplicaLabels   []string
	Matchers        [][]*labels.Matcher
	StoreMatchers   [][]*labels.Matcher
	CachingOptions  queryrange.CachingOptions
}

// GetStart returns the start timestamp of the request in milliseconds.
func (r *ThanosSeriesRequest) GetStart() int64 { return r.Start }

// GetEnd returns the end timestamp of the request in milliseconds.
func (r *ThanosSeriesRequest) GetEnd() int64 { return r.End }

// GetStep returns the step of the request in milliseconds. Returns 1 is a trick to avoid panic in
// https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/results_cache.go#L447.
func (r *ThanosSeriesRequest) GetStep() int64 { return 1 }

// GetQuery returns the query of the request.
func (r *ThanosSeriesRequest) GetQuery() string { return "" }

func (r *ThanosSeriesRequest) GetCachingOptions() queryrange.CachingOptions { return r.CachingOptions }

// WithStartEnd clone the current request with different start and end timestamp.
func (r *ThanosSeriesRequest) WithStartEnd(start int64, end int64) queryrange.Request {
	q := *r
	q.Start = start
	q.End = end
	return &q
}

// WithQuery clone the current request with a different query.
func (r *ThanosSeriesRequest) WithQuery(_ string) queryrange.Request {
	q := *r
	return &q
}

// LogToSpan writes information about this request to an OpenTracing span.
func (r *ThanosSeriesRequest) LogToSpan(sp opentracing.Span) {
	fields := []otlog.Field{
		otlog.String("start", timestamp.Time(r.GetStart()).String()),
		otlog.String("end", timestamp.Time(r.GetEnd()).String()),
		otlog.Bool("dedup", r.Dedup),
		otlog.Bool("partial_response", r.PartialResponse),
		otlog.Object("replicaLabels", r.ReplicaLabels),
		otlog.Object("matchers", r.Matchers),
		otlog.Object("storeMatchers", r.StoreMatchers),
	}

	sp.LogFields(fields...)
}

// Reset implements proto.Message interface required by queryrange.Request,
// which is not used in thanos.
func (r *ThanosSeriesRequest) Reset() {}

// String implements proto.Message interface required by queryrange.Request,
// which is not used in thanos.
func (r *ThanosSeriesRequest) String() string { return "" }

// ProtoMessage implements proto.Message interface required by queryrange.Request,
// which is not used in thanos.
func (r *ThanosSeriesRequest) ProtoMessage() {}

func (r *ThanosSeriesRequest) GetStoreMatchers() [][]*labels.Matcher { return r.StoreMatchers }
