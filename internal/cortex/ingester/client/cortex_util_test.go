// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/internal/cortex/util/grpcutil"
	"github.com/thanos-io/thanos/internal/cortex/util/test"
)

func TestStreamingSends(t *testing.T) {
	tests := map[string]struct {
		streamName string
		serverSend func(grpc.ServerStream) error
		clientRecv func(context.Context, IngesterClient) error
	}{
		"testing SendQueryStream": {
			streamName: "QueryStream",
			serverSend: func(stream grpc.ServerStream) error {
				return SendQueryStream(stream.(Ingester_QueryStreamServer), &QueryStreamResponse{})
			},
			clientRecv: func(ctx context.Context, client IngesterClient) error {
				stream, err := client.QueryStream(ctx, &QueryRequest{})
				require.NoError(t, err)

				// Try to receive the response and assert the error we get is the context.Canceled
				// wrapped within a gRPC error.
				_, err = stream.Recv()
				return err
			},
		},
		"testing SendMetricsForLabelMatchersStream": {
			streamName: "MetricsForLabelMatchersStream",
			serverSend: func(stream grpc.ServerStream) error {
				return SendMetricsForLabelMatchersStream(stream.(Ingester_MetricsForLabelMatchersStreamServer), &MetricsForLabelMatchersStreamResponse{})
			},
			clientRecv: func(ctx context.Context, client IngesterClient) error {
				stream, err := client.MetricsForLabelMatchersStream(ctx, &MetricsForLabelMatchersRequest{})
				require.NoError(t, err)

				// Try to receive the response and assert the error we get is the context.Canceled
				// wrapped within a gRPC error.
				_, err = stream.Recv()
				return err
			},
		},
		"testing LabelNamesStream": {
			streamName: "LabelNamesStream",
			serverSend: func(stream grpc.ServerStream) error {
				return SendLabelNamesStream(stream.(Ingester_LabelNamesStreamServer), &LabelNamesStreamResponse{})
			},
			clientRecv: func(ctx context.Context, client IngesterClient) error {
				stream, err := client.LabelNamesStream(ctx, &LabelNamesRequest{})
				require.NoError(t, err)

				// Try to receive the response and assert the error we get is the context.Canceled
				// wrapped within a gRPC error.
				_, err = stream.Recv()
				return err
			},
		},
		"testing LabelValuesStream": {
			streamName: "LabelValuesStream",
			serverSend: func(stream grpc.ServerStream) error {
				return SendLabelValuesStream(stream.(Ingester_LabelValuesStreamServer), &LabelValuesStreamResponse{})
			},
			clientRecv: func(ctx context.Context, client IngesterClient) error {
				stream, err := client.LabelValuesStream(ctx, &LabelValuesRequest{})
				require.NoError(t, err)

				// Try to receive the response and assert the error we get is the context.Canceled
				// wrapped within a gRPC error.
				_, err = stream.Recv()
				return err
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Create a new gRPC server with in-memory communication.
			listen := bufconn.Listen(1024 * 1024)
			server := grpc.NewServer()
			bufDialer := func(context.Context, string) (net.Conn, error) {
				return listen.Dial()
			}

			conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer conn.Close()

			// Create a cancellable context for the client.
			clientCtx, clientCancel := context.WithCancel(context.Background())

			// Create a WaitGroup used to wait until the mocked server assertions
			// complete before returning.
			wg := sync.WaitGroup{}
			wg.Add(1)

			serverMock := &IngesterServerMock{}
			serverMock.On(testData.streamName, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
				defer wg.Done()

				stream := args.Get(1).(grpc.ServerStream)

				// Cancel the client request.
				clientCancel()

				// Wait until the cancelling has been propagated to the server.
				test.Poll(t, time.Second, context.Canceled, func() interface{} {
					return stream.Context().Err()
				})

				// Try to send the response and assert the error we get is the context.Canceled
				// and not transport.ErrIllegalHeaderWrite. This is the assertion we care about
				// in this test.
				err := testData.serverSend(stream)
				assert.Equal(t, context.Canceled, err)
			})

			RegisterIngesterServer(server, serverMock)

			go func() {
				require.NoError(t, server.Serve(listen))
			}()

			client := NewIngesterClient(conn)
			err = testData.clientRecv(clientCtx, client)
			assert.Equal(t, true, grpcutil.IsGRPCContextCanceled(err))

			// Wait until the assertions in the server mock have completed.
			wg.Wait()
		})
	}
}
