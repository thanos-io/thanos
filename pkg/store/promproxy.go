package store

import (
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/improbable-eng/promlts/pkg/store/storepb"
)

// PrometheusProxy implements the store node API on top of the Prometheus
// HTTP v1 API.
type PrometheusProxy struct {
	addr   string
	client *http.Client
}

var _ storepb.StoreServer = (*PrometheusProxy)(nil)

// NewPrometheusProxy returns a new PrometheusProxy that uses the given HTTP client
// to talk to Prometheus.
func NewPrometheusProxy(client *http.Client, addr string) *PrometheusProxy {
	return &PrometheusProxy{addr: addr, client: client}
}

// Series returns all series for a requested time range and label matcher. The returned data may
// exceed the requested time bounds.
func (p *PrometheusProxy) Series(ctx context.Context, r *storepb.SeriesRequest) (
	*storepb.SeriesResponse, error,
) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
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
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
