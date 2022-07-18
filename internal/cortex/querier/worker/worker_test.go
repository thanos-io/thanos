// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package worker

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/internal/cortex/util/services"
	"github.com/thanos-io/thanos/internal/cortex/util/test"
)

func TestResetConcurrency(t *testing.T) {
	tests := []struct {
		name                                  string
		parallelism                           int
		maxConcurrent                         int
		numTargets                            int
		expectedConcurrency                   int
		expectedConcurrencyAfterTargetRemoval int
	}{
		{
			name:                                  "Test create at least one processor per target",
			parallelism:                           0,
			maxConcurrent:                         0,
			numTargets:                            2,
			expectedConcurrency:                   2,
			expectedConcurrencyAfterTargetRemoval: 1,
		},
		{
			name:                                  "Test parallelism per target",
			parallelism:                           4,
			maxConcurrent:                         0,
			numTargets:                            2,
			expectedConcurrency:                   8,
			expectedConcurrencyAfterTargetRemoval: 4,
		},
		{
			name:                                  "Test Total Parallelism with a remainder",
			parallelism:                           1,
			maxConcurrent:                         7,
			numTargets:                            4,
			expectedConcurrency:                   7,
			expectedConcurrencyAfterTargetRemoval: 7,
		},
		{
			name:                                  "Test Total Parallelism dividing evenly",
			parallelism:                           1,
			maxConcurrent:                         6,
			numTargets:                            2,
			expectedConcurrency:                   6,
			expectedConcurrencyAfterTargetRemoval: 6,
		},
		{
			name:                                  "Test Total Parallelism at least one worker per target",
			parallelism:                           1,
			maxConcurrent:                         3,
			numTargets:                            6,
			expectedConcurrency:                   6,
			expectedConcurrencyAfterTargetRemoval: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Parallelism:           tt.parallelism,
				MatchMaxConcurrency:   tt.maxConcurrent > 0,
				MaxConcurrentRequests: tt.maxConcurrent,
			}

			w, err := newQuerierWorkerWithProcessor(cfg, log.NewNopLogger(), &mockProcessor{}, "", nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), w))

			for i := 0; i < tt.numTargets; i++ {
				// gRPC connections are virtual... they don't actually try to connect until they are needed.
				// This allows us to use dummy ports, and not get any errors.
				w.AddressAdded(fmt.Sprintf("127.0.0.1:%d", i))
			}

			test.Poll(t, 250*time.Millisecond, tt.expectedConcurrency, func() interface{} {
				return getConcurrentProcessors(w)
			})

			// now we remove an address and ensure we still have the expected concurrency
			w.AddressRemoved(fmt.Sprintf("127.0.0.1:%d", rand.Intn(tt.numTargets)))
			test.Poll(t, 250*time.Millisecond, tt.expectedConcurrencyAfterTargetRemoval, func() interface{} {
				return getConcurrentProcessors(w)
			})

			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), w))
			assert.Equal(t, 0, getConcurrentProcessors(w))
		})
	}
}

func getConcurrentProcessors(w *querierWorker) int {
	result := 0
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, mgr := range w.managers {
		result += int(mgr.currentProcessors.Load())
	}

	return result
}

type mockProcessor struct{}

func (m mockProcessor) processQueriesOnSingleStream(ctx context.Context, _ *grpc.ClientConn, _ string) {
	<-ctx.Done()
}

func (m mockProcessor) notifyShutdown(_ context.Context, _ *grpc.ClientConn, _ string) {}
