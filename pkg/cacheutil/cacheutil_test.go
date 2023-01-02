// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/goleak"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/gate"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(
		m,
		// https://github.com/rueian/rueidis/blob/v0.0.90/pipe.go#L204.
		goleak.IgnoreTopFunction("github.com/rueian/rueidis.(*pipe).backgroundPing"),
	)
}

func TestDoWithBatch(t *testing.T) {
	tests := map[string]struct {
		items           []string
		batchSize       int
		expectedBatches int
		concurrency     gate.Gate
	}{
		"no items": {
			items:           []string{},
			batchSize:       2,
			expectedBatches: 0,
			concurrency:     nil,
		},

		"fewer than batch size": {
			items:           []string{"key1"},
			batchSize:       2,
			expectedBatches: 1,
			concurrency:     nil,
		},

		"perfect sized for batch": {
			items:           []string{"key1", "key2", "key3", "key4"},
			batchSize:       2,
			expectedBatches: 2,
			concurrency:     nil,
		},

		"odd sized for batch": {
			items:           []string{"key1", "key2", "key3", "key4", "key5"},
			batchSize:       2,
			expectedBatches: 3,
			concurrency:     nil,
		},

		"odd sized with concurrency limit": {
			items:           []string{"key1", "key2", "key3", "key4", "key5"},
			batchSize:       2,
			expectedBatches: 3,
			concurrency:     gate.New(prometheus.NewPedanticRegistry(), 1),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualBatches := atomic.Int64{}
			_ = doWithBatch(context.Background(), len(testData.items), testData.batchSize, testData.concurrency, func(startIndex, endIndex int) error {
				actualBatches.Inc()
				return nil
			})

			testutil.Equals(t, int64(testData.expectedBatches), actualBatches.Load())
		})
	}
}
