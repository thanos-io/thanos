// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"time"

	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
)

// ThanosRequestStoreMatcherGetter is a an interface for store matching that all request share.
// TODO(yeya24): Add partial result when needed.
type ThanosRequestStoreMatcherGetter interface {
	GetStoreMatchers() [][]*labels.Matcher
}

// ShardedRequest interface represents a query request that can be sharded vertically.
type ShardedRequest interface {
	WithShardInfo(info *storepb.ShardInfo) queryrange.Request
}

type RequestHeader struct {
	Name   string
	Values []string
}

// ThanosRequestDedup is a an interface for all requests that share setting deduplication.
type ThanosRequestDedup interface {
	IsDedupEnabled() bool
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
	Headers             []*RequestHeader
	Stats               string
	ShardInfo           *storepb.ShardInfo
}

// IsDedupEnabled returns true if deduplication is enabled.
func (r *ThanosQueryRangeRequest) IsDedupEnabled() bool { return r.Dedup }

// GetStoreMatchers returns store matches.
func (r *ThanosQueryRangeRequest) GetStoreMatchers() [][]*labels.Matcher { return r.StoreMatchers }

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

func (r *ThanosQueryRangeRequest) GetStats() string { return r.Stats }

func (r *ThanosQueryRangeRequest) WithStats(stats string) queryrange.Request {
	q := *r
	q.Stats = stats
	return &q
}

// WithStartEnd clone the current request with different start and end timestamp.
func (r *ThanosQueryRangeRequest) WithStartEnd(start, end int64) queryrange.Request {
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

// WithShardInfo clones the current request with a different shard info.
func (r *ThanosQueryRangeRequest) WithShardInfo(info *storepb.ShardInfo) queryrange.Request {
	q := *r
	q.ShardInfo = info
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

type ThanosQueryInstantRequest struct {
	Path                string
	Time                int64
	Timeout             time.Duration
	Query               string
	Dedup               bool
	PartialResponse     bool
	AutoDownsampling    bool
	MaxSourceResolution int64
	ReplicaLabels       []string
	StoreMatchers       [][]*labels.Matcher
	Headers             []*RequestHeader
	Stats               string
	ShardInfo           *storepb.ShardInfo
}

// IsDedupEnabled returns true if deduplication is enabled.
func (r *ThanosQueryInstantRequest) IsDedupEnabled() bool { return r.Dedup }

// GetStoreMatchers returns store matches.
func (r *ThanosQueryInstantRequest) GetStoreMatchers() [][]*labels.Matcher { return r.StoreMatchers }

// GetStart returns the start timestamp of the request in milliseconds.
func (r *ThanosQueryInstantRequest) GetStart() int64 { return 0 }

// GetEnd returns the end timestamp of the request in milliseconds.
func (r *ThanosQueryInstantRequest) GetEnd() int64 { return 0 }

// GetStep returns the step of the request in milliseconds.
func (r *ThanosQueryInstantRequest) GetStep() int64 { return 0 }

// GetQuery returns the query of the request.
func (r *ThanosQueryInstantRequest) GetQuery() string { return r.Query }

func (r *ThanosQueryInstantRequest) GetCachingOptions() queryrange.CachingOptions {
	return queryrange.CachingOptions{}
}

func (r *ThanosQueryInstantRequest) GetStats() string { return r.Stats }

func (r *ThanosQueryInstantRequest) WithStats(stats string) queryrange.Request {
	q := *r
	q.Stats = stats
	return &q
}

// WithStartEnd clone the current request with different start and end timestamp.
func (r *ThanosQueryInstantRequest) WithStartEnd(_, _ int64) queryrange.Request { return nil }

// WithQuery clone the current request with a different query.
func (r *ThanosQueryInstantRequest) WithQuery(query string) queryrange.Request {
	q := *r
	q.Query = query
	return &q
}

// WithShardInfo clones the current request with a different shard info.
func (r *ThanosQueryInstantRequest) WithShardInfo(info *storepb.ShardInfo) queryrange.Request {
	q := *r
	q.ShardInfo = info
	return &q
}

// LogToSpan writes information about this request to an OpenTracing span.
func (r *ThanosQueryInstantRequest) LogToSpan(sp opentracing.Span) {
	fields := []otlog.Field{
		otlog.String("query", r.GetQuery()),
		otlog.Int64("time", r.Time),
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
func (r *ThanosQueryInstantRequest) Reset() {}

// String implements proto.Message interface required by queryrange.Request,
// which is not used in thanos.
func (r *ThanosQueryInstantRequest) String() string { return "" }

// ProtoMessage implements proto.Message interface required by queryrange.Request,
// which is not used in thanos.
func (r *ThanosQueryInstantRequest) ProtoMessage() {}

type ThanosLabelsRequest struct {
	Start           int64
	End             int64
	Label           string
	Path            string
	Matchers        [][]*labels.Matcher
	StoreMatchers   [][]*labels.Matcher
	PartialResponse bool
	CachingOptions  queryrange.CachingOptions
	Headers         []*RequestHeader
	Stats           string
}

// GetStoreMatchers returns store matches.
func (r *ThanosLabelsRequest) GetStoreMatchers() [][]*labels.Matcher { return r.StoreMatchers }

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

func (r *ThanosLabelsRequest) GetStats() string { return r.Stats }

func (r *ThanosLabelsRequest) WithStats(stats string) queryrange.Request {
	q := *r
	q.Stats = stats
	return &q
}

// WithStartEnd clone the current request with different start and end timestamp.
func (r *ThanosLabelsRequest) WithStartEnd(start, end int64) queryrange.Request {
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
		otlog.Object("matchers", r.Matchers),
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
	Headers         []*RequestHeader
	Stats           string
}

// IsDedupEnabled returns true if deduplication is enabled.
func (r *ThanosSeriesRequest) IsDedupEnabled() bool { return r.Dedup }

// GetStoreMatchers returns store matches.
func (r *ThanosSeriesRequest) GetStoreMatchers() [][]*labels.Matcher { return r.StoreMatchers }

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

func (r *ThanosSeriesRequest) GetStats() string { return r.Stats }

func (r *ThanosSeriesRequest) WithStats(stats string) queryrange.Request {
	q := *r
	q.Stats = stats
	return &q
}

// WithStartEnd clone the current request with different start and end timestamp.
func (r *ThanosSeriesRequest) WithStartEnd(start, end int64) queryrange.Request {
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
