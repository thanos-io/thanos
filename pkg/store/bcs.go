// Copyright (c) The BCS Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc/metadata"
)

const (
	// LabelMatchKey
	LabelMatchKey           = ctxKey(1)
	requestIDKey            = ctxKey(2)
	scopeClusterIDHeaderKey = ctxKey(3)
	requestIDHeaderKey      = "X-Request-ID"
)

const (
	ScopeProjectIDHeaderKey  = "X-Scope-Project-Id"
	ScopeProjectCodeHeadeKey = "X-Scope-Project-Code"
	ScopeClusterIDHeaderKey  = "X-Scope-Cluster-Id"
)

// RequestIdHeaderKey :
func RequestIdHeaderKey() string {
	return requestIDHeaderKey
}

// CopyLabelMatchContext copies the necessary trace context from given source context to target context.
func CopyLabelMatchContext(trgt, src context.Context) context.Context {
	requestID := RequestIDValue(src)
	clusterID := ClusterIDValue(src)
	v, ok := LabelMatchValue(src)
	if !ok {
		return WithRequestIDValue(trgt, requestID)
	}
	return WithScopeClusterIDValue(WithRequestIDValue(WithLabelMatchValue(trgt, v), requestID), clusterID)
}

// CopyToGRPCValues context to grpc
func CopyToGRPCValues(ctx context.Context) context.Context {
	requestID := RequestIDValue(ctx)
	clusterID := ClusterIDValue(ctx)
	return WithScopeClusterIDValue(WithRequestIDValue(ctx, requestID), clusterID)
}

// WithLabelMatchValue 设置值
func WithLabelMatchValue(ctx context.Context, matches [][]*labels.Matcher) context.Context {
	return context.WithValue(ctx, LabelMatchKey, matches)
}

// WithLabelMatchValue 设置值
func WithRequestIDValue(ctx context.Context, id string) context.Context {
	newCtx := context.WithValue(ctx, requestIDKey, id)
	return GRPCWithRequestIDValue(newCtx, id)
}

// WithLabelMatchValue 设置值
func WithScopeClusterIDValue(ctx context.Context, clusterID string) context.Context {
	newCtx := context.WithValue(ctx, scopeClusterIDHeaderKey, clusterID)
	return GRPCWithScopeClusterIDValue(newCtx, clusterID)
}

// GRPCWithRequestIDValue : grpc 需要单独处理
func GRPCWithRequestIDValue(ctx context.Context, id string) context.Context {
	ctx = metadata.AppendToOutgoingContext(ctx, requestIDHeaderKey, id)
	return ctx
}

// GRPCWithScopeClusterIDValue
func GRPCWithScopeClusterIDValue(ctx context.Context, clusterID string) context.Context {
	ctx = metadata.AppendToOutgoingContext(ctx, ScopeClusterIDHeaderKey, clusterID)
	return ctx
}

// LabelMatchValue 获取值，获取变量, 修改matcher, 支持 namespace 级别过滤
func LabelMatchValue(ctx context.Context) ([][]*labels.Matcher, bool) {
	v, ok := ctx.Value(LabelMatchKey).([][]*labels.Matcher)
	return v, ok
}

// RequestIDValue 获取值
func RequestIDValue(ctx context.Context) string {
	v, ok := ctx.Value(requestIDKey).(string)
	if !ok || v == "" {
		return GRPCRequestIDValue(ctx)
	}

	return v
}

// ClusterIDValue 集群ID值
func ClusterIDValue(ctx context.Context) string {
	v, ok := ctx.Value(scopeClusterIDHeaderKey).(string)
	if !ok || v == "" {
		return GRPCClusterIDValue(ctx)
	}

	return v
}

// GRPCRequestIDValue grpc 需要单独处理
func GRPCRequestIDValue(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(requestIDHeaderKey)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// GRPCClusterIDValue
func GRPCClusterIDValue(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(ScopeClusterIDHeaderKey)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// makeSeriesRequest
func makeSeriesRequest(ctx context.Context, r *storepb.SeriesRequest) []*storepb.SeriesRequest {
	matchValues, ok := LabelMatchValue(ctx)
	if !ok || len(matchValues) == 0 {
		return []*storepb.SeriesRequest{r}
	}

	reqs := make([]*storepb.SeriesRequest, 0, len(matchValues))
	for _, v := range matchValues {
		storeMatchers, _ := storepb.PromMatchersToMatchers(v...)
		newReq := *r
		newReq.Matchers = append(newReq.Matchers, storeMatchers...)
		reqs = append(reqs, &newReq)
	}

	return reqs
}

// makeLabelNamesRequest
func makeLabelNamesRequest(ctx context.Context, r *storepb.LabelNamesRequest) []*storepb.LabelNamesRequest {
	matchValues, ok := LabelMatchValue(ctx)
	if !ok || len(matchValues) == 0 {
		return []*storepb.LabelNamesRequest{r}
	}

	reqs := make([]*storepb.LabelNamesRequest, 0, len(matchValues))
	for _, v := range matchValues {
		storeMatchers, _ := storepb.PromMatchersToMatchers(v...)
		newReq := *r
		newReq.Matchers = append(newReq.Matchers, storeMatchers...)
		reqs = append(reqs, &newReq)
	}

	return reqs
}

// makeLabelValuesRequest
func makeLabelValuesRequest(ctx context.Context, r *storepb.LabelValuesRequest) []*storepb.LabelValuesRequest {
	matchValues, ok := LabelMatchValue(ctx)
	if !ok || len(matchValues) == 0 {
		return []*storepb.LabelValuesRequest{r}
	}

	reqs := make([]*storepb.LabelValuesRequest, 0, len(matchValues))
	for _, v := range matchValues {
		storeMatchers, _ := storepb.PromMatchersToMatchers(v...)
		newReq := *r
		newReq.Matchers = append(newReq.Matchers, storeMatchers...)
		reqs = append(reqs, &newReq)
	}

	return reqs
}

// storeMatchAnyMetadata 可以匹配任意Label
func storeMatchAnyMetadata(s Client, storeDebugMatchers [][]*labels.Matcher) (ok bool, reason string) {
	if len(storeDebugMatchers) == 0 {
		return true, ""
	}

	labelSets := s.LabelSets()
	for idx, ls := range labelSets {
		if addr, isLocalClient := s.Addr(); isLocalClient {
			labelSets[idx] = append(ls, labels.Label{Name: "__address__", Value: addr})
		}
	}

	for _, sm := range storeDebugMatchers {
		if labelSetsMatchAny(sm, labelSets...) {
			return true, ""
		}
	}

	return false, fmt.Sprintf("%v does not match debug store metadata matchers: %v", labelSets, storeDebugMatchers)
}

// labelSetsMatchAny 满足任意
func labelSetsMatchAny(matchers []*labels.Matcher, lset ...labels.Labels) bool {
	if len(lset) == 0 {
		return true
	}

	for _, ls := range lset {
		if labelSetsMatchAll(matchers, ls) {
			return true
		}
	}
	return false
}

// labelSetsMatchAll 满足所有
func labelSetsMatchAll(matchers []*labels.Matcher, ls labels.Labels) bool {
	// 空值返回 false
	if len(ls) == 0 {
		return false
	}

	for _, m := range matchers {
		// 值不存在或者不匹配都不符合
		if lv := ls.Get(m.Name); lv == "" || !m.Matches(lv) {
			return false
		}
	}
	return true
}
