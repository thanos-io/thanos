package rules

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"gopkg.in/yaml.v2"
)

type RemoteWriteConfig struct {
	// The URL of the endpoint to send samples to.
	URL string `json:"url"`
	// The name of the remote write queue, must be unique if specified. The
	// name is used in metrics and logging in order to differentiate queues.
	// Only valid in Prometheus versions 2.15.0 and newer.
	Name string `json:"name,omitempty"`
}

func LoadRemoteWriteConfig(content []byte) (RemoteWriteConfig, error) {
	var config RemoteWriteConfig
	err := yaml.UnmarshalStrict(content, &config)
	return config, err
}

type RemoteWrite struct {
	RemoteWriteConfig RemoteWriteConfig
}

func (r *RemoteWrite) Appender(ctx context.Context) storage.Appender {
	return &RemoteWriteAppender{mu: &sync.Mutex{}, config: r.RemoteWriteConfig}
}

func (r *RemoteWrite) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &RemoteWriteQueryable{}, nil
}

type RemoteWriteAppender struct {
	config RemoteWriteConfig
	mu     *sync.Mutex
	req    prompb.WriteRequest
}

func (r *RemoteWriteAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	// TODO: Maybe there's a way of type casting this?
	var reqLabels []prompb.Label
	for _, label := range l {
		reqLabels = append(reqLabels, prompb.Label{
			Name:  label.Name,
			Value: label.Value,
		})
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.req.Timeseries = append(r.req.Timeseries, prompb.TimeSeries{
		Labels: reqLabels,
		Samples: []prompb.Sample{{
			Timestamp: t,
			Value:     v,
		}},
	})

	return uint64(t), nil
}

func (r *RemoteWriteAppender) AddFast(ref uint64, t int64, v float64) error {
	return nil
}

func (r *RemoteWriteAppender) Commit() error {
	payload, err := proto.Marshal(&r.req)
	if err != nil {
		return fmt.Errorf("failed to marshal proto remote write request: %w", err)
	}

	payloadCompressed := snappy.Encode(nil, payload)

	req, err := http.NewRequest(http.MethodPost, r.config.URL, bytes.NewBuffer(payloadCompressed))
	if err != nil {
		return fmt.Errorf("failed to create HTTP remote-write request: %w", err)
	}
	//TODO: Add THANOS-TENANT header

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make remote-write request: %w", err)
	}

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("remote write requests didn't return with 200 OK: %d", resp.StatusCode)
	}

	return nil
}

func (r *RemoteWriteAppender) Rollback() error {
	return nil
}

type RemoteWriteQueryable struct{}

func (r *RemoteWriteQueryable) LabelValues(name string) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (r *RemoteWriteQueryable) LabelNames() ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (r *RemoteWriteQueryable) Close() error {
	return nil
}

func (r *RemoteWriteQueryable) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return nil
}
