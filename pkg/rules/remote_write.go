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
)

type RemoteWrite struct{}

func (r *RemoteWrite) Appender(ctx context.Context) storage.Appender {
	return &RemoteWriteAppender{mu: &sync.Mutex{}}
}

func (r *RemoteWrite) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &RemoteWriteQueryable{}, nil
}

type RemoteWriteAppender struct {
	mu  *sync.Mutex
	req prompb.WriteRequest
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
	fmt.Printf("Appender AddFast: %d, %d, %2.f\n", ref, t, v)
	return nil
}

func (r *RemoteWriteAppender) Commit() error {
	payload, err := proto.Marshal(&r.req)
	if err != nil {
		return fmt.Errorf("failed to marshal proto remote write request: %w", err)
	}

	payloadCompressed := snappy.Encode(nil, payload)

	req, err := http.NewRequest(http.MethodPost, "http://localhost:10908/api/v1/receive", bytes.NewBuffer(payloadCompressed))
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
	fmt.Println("Appender Rollback")
	return nil
}

type RemoteWriteQueryable struct{}

func (r *RemoteWriteQueryable) LabelValues(name string) ([]string, storage.Warnings, error) {
	fmt.Printf("Queryable LabelValues: %s\n", name)
	return nil, nil, nil
}

func (r *RemoteWriteQueryable) LabelNames() ([]string, storage.Warnings, error) {
	fmt.Printf("Queryable LabelNames\n")
	return nil, nil, nil
}

func (r *RemoteWriteQueryable) Close() error {
	fmt.Println("Queryable Close")
	return nil
}

func (r *RemoteWriteQueryable) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	fmt.Printf("Queryable Select: %t, %v, %v\n", sortSeries, hints, matchers)
	return nil
}
