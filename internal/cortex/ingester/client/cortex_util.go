package client

import (
	context "context"
)

// SendQueryStream wraps the stream's Send() checking if the context is done
// before calling Send().
func SendQueryStream(s Ingester_QueryStreamServer, m *QueryStreamResponse) error {
	return sendWithContextErrChecking(s.Context(), func() error {
		return s.Send(m)
	})
}

// SendTimeSeriesChunk wraps the stream's Send() checking if the context is done
// before calling Send().
func SendTimeSeriesChunk(s Ingester_TransferChunksClient, m *TimeSeriesChunk) error {
	return sendWithContextErrChecking(s.Context(), func() error {
		return s.Send(m)
	})
}

func SendMetricsForLabelMatchersStream(s Ingester_MetricsForLabelMatchersStreamServer, m *MetricsForLabelMatchersStreamResponse) error {
	return sendWithContextErrChecking(s.Context(), func() error {
		return s.Send(m)
	})
}

func SendLabelValuesStream(s Ingester_LabelValuesStreamServer, l *LabelValuesStreamResponse) error {
	return sendWithContextErrChecking(s.Context(), func() error {
		return s.Send(l)
	})
}

func SendLabelNamesStream(s Ingester_LabelNamesStreamServer, l *LabelNamesStreamResponse) error {
	return sendWithContextErrChecking(s.Context(), func() error {
		return s.Send(l)
	})
}

func SendAsBatchToStream(totalItems int, streamBatchSize int, fn func(start, end int) error) error {
	for i := 0; i < totalItems; i += streamBatchSize {
		j := i + streamBatchSize
		if j > totalItems {
			j = totalItems
		}
		if err := fn(i, j); err != nil {
			return err
		}
	}

	return nil
}

func sendWithContextErrChecking(ctx context.Context, send func() error) error {
	// If the context has been canceled or its deadline exceeded, we should return it
	// instead of the cryptic error the Send() will return.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	if err := send(); err != nil {
		// Experimentally, we've seen the context switching to done after the Send()
		// has been  called, so here we do recheck the context in case of error.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		return err
	}

	return nil
}
