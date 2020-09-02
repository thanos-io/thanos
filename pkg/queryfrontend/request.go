// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type ThanosRequest struct {
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
	StoreMatchers       [][]storepb.LabelMatcher
}

// GetStart returns the start timestamp of the request in milliseconds.
func (r *ThanosRequest) GetStart() int64 {
	return r.Start
}

// GetEnd returns the end timestamp of the request in milliseconds.
func (r *ThanosRequest) GetEnd() int64 {
	return r.End
}

// GetStep returns the step of the request in milliseconds.
func (r *ThanosRequest) GetStep() int64 {
	return r.Step
}

// GetQuery returns the query of the request.
func (r *ThanosRequest) GetQuery() string {
	return r.Query
}

// WithStartEnd clone the current request with different start and end timestamp.
func (r *ThanosRequest) WithStartEnd(start int64, end int64) queryrange.Request {
	q := *r
	q.Start = start
	q.End = end
	return &q
}

// WithQuery clone the current request with a different query.
func (r *ThanosRequest) WithQuery(query string) queryrange.Request {
	q := *r
	q.Query = query
	return &q
}

// LogToSpan writes information about this request to an OpenTracing span.
func (r *ThanosRequest) LogToSpan(sp opentracing.Span) {
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
func (r *ThanosRequest) Reset() {}

// String implements proto.Message interface required by queryrange.Request,
// which is not used in thanos.
func (r *ThanosRequest) String() string { return "" }

// ProtoMessage implements proto.Message interface required by queryrange.Request,
// which is not used in thanos.
func (r *ThanosRequest) ProtoMessage() {}
