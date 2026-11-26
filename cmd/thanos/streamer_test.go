// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/streamer"
	streamer_pkg "github.com/thanos-io/thanos/pkg/streamer"
	"google.golang.org/grpc"
)

func TestConvertToSeriesReq(t *testing.T) {
	streamerReq := &streamer.StreamerRequest{
		RequestId:        "test_request",
		StartTimestampMs: 1672531200000,
		EndTimestampMs:   1672534800000,
		SkipChunks:       false,
		LabelMatchers: []streamer.LabelMatcher{
			{Name: "instance", Value: "localhost:9090", Type: streamer.LabelMatcher_EQ},
		},
		Metric: "cpu_usage",
	}

	expectedReq := &storepb.SeriesRequest{
		Aggregates: []storepb.Aggr{storepb.Aggr_RAW},
		Matchers: []storepb.LabelMatcher{
			{Name: "instance", Value: "localhost:9090", Type: storepb.LabelMatcher_EQ},
			{Name: "__name__", Value: "cpu_usage", Type: storepb.LabelMatcher_EQ},
		},
		MinTime:    streamerReq.StartTimestampMs,
		MaxTime:    streamerReq.EndTimestampMs,
		SkipChunks: streamerReq.SkipChunks,
	}
	config := &StreamerConfig{}

	actualReq := convertToSeriesReq(config, streamerReq)
	assert.Equal(t, expectedReq, actualReq, "Expected SeriesRequest to match")

	config.replicaLabel = "replica"
	expectedReq.WithoutReplicaLabels = []string{"replica"}
	actualReq = convertToSeriesReq(config, streamerReq)
	assert.Equal(t, expectedReq, actualReq, "Expected SeriesRequest to match with replica label removed")
}

type grpcServer struct {
	server   *grpc.Server
	addr     string
	exit     chan struct{}
	listener net.Listener
}

func (g *grpcServer) Stop() {
	g.server.Stop()
	g.listener.Close()
	// fmt.Print("Waiting for grpc server to exit")
	<-g.exit
	// fmt.Print("grpc server exit")
}

func startMockGRPCServer(t *testing.T) *grpcServer {
	listener, err := net.Listen("tcp", ":0")
	assert.Nil(t, err)

	exitChan := make(chan struct{})
	startChan := make(chan struct{})

	server := grpc.NewServer()
	go func() {
		close(startChan)
		err := server.Serve(listener)
		require.NoError(t, err)
		// fmt.Print("Closing exit chan")
		close(exitChan)
	}()
	<-startChan
	time.Sleep(time.Second)
	return &grpcServer{
		server:   server,
		listener: listener,
		addr:     listener.Addr().String(),
		exit:     exitChan,
	}
}

func TestNewStreamerWithMockServer(t *testing.T) {
	mockServer := startMockGRPCServer(t)
	defer mockServer.Stop()

	config := StreamerConfig{
		storeAddrPort:        mockServer.addr,
		streamTimeoutSeconds: 3,
	}
	logger := log.NewLogfmtLogger(os.Stdout)

	streamer, err := NewStreamer(config, logger)
	assert.NoError(t, err)
	assert.NotNil(t, streamer)
	assert.NotNil(t, streamer.grpcConn)
}

func TestStreamOneRequest(t *testing.T) {
	mockServer := startMockGRPCServer(t)
	defer mockServer.Stop()

	config := StreamerConfig{
		storeAddrPort:        mockServer.addr,
		streamTimeoutSeconds: 6,
	}
	logger := log.NewLogfmtLogger(os.Stdout)
	streamer, err := NewStreamer(config, logger)
	assert.NoError(t, err)
	assert.NotNil(t, streamer)

	request := &streamer_pkg.StreamerRequest{
		RequestId:        "test_request",
		StartTimestampMs: 1672531200000,
		EndTimestampMs:   1672534800000,
		SkipChunks:       false,
		LabelMatchers:    nil,
	}

	buffer := &bytes.Buffer{}
	err = streamer.streamOneRequest(request, buffer)
	assert.NoError(t, err)
	encodedData := buffer.String()
	assert.True(t, len(encodedData) > 0)
	errorResp := streamer_pkg.StreamerResponse{}
	// Remove the Deliminator byte from the encoded data
	err = errorResp.Decode(encodedData)
	assert.NoError(t, err)
	assert.Equal(t, "rpc error: code = Unimplemented desc = unknown service thanos.Store", errorResp.Err)
}

func TestStreamer_RunAndStop(t *testing.T) {
	tmpFile, err := os.CreateTemp("/tmp", "streamer_test.sock")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	mockServer := startMockGRPCServer(t)
	defer mockServer.Stop()

	logger := log.NewLogfmtLogger(os.Stdout)
	config := StreamerConfig{
		socketPath:           tmpFile.Name(),
		storeAddrPort:        mockServer.addr,
		streamTimeoutSeconds: 10,
	}
	streamer, err := NewStreamer(config, logger)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := streamer.Run()
		assert.NoError(t, err)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Second) // Give time for server to start
		conn, err := net.Dial("unix", config.socketPath)
		require.NoError(t, err)
		defer conn.Close()
		request := &streamer_pkg.StreamerRequest{
			RequestId:        "test_request",
			StartTimestampMs: 1672531200000,
			EndTimestampMs:   1672534800000,
			SkipChunks:       false,
			LabelMatchers: []streamer_pkg.LabelMatcher{
				{
					Name:  "__name__",
					Value: "up",
					Type:  streamer_pkg.LabelMatcher_EQ,
				},
			},
		}
		data, err := streamer_pkg.Encode(request)
		assert.NoError(t, err)
		err = writeBytes(conn, []byte(data), []byte(streamer_pkg.Deliminator))
		assert.NoError(t, err)
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		scanner := bufio.NewScanner(conn)
		require.True(t, scanner.Scan())
		encodedResp := scanner.Text()
		require.NoError(t, err)
		resp := &streamer_pkg.StreamerResponse{}
		err = resp.Decode(encodedResp)
		assert.NoError(t, err)
		assert.Equal(t, "rpc error: code = Unimplemented desc = unknown service thanos.Store", resp.Err)
	}()
	time.Sleep(5 * time.Second)
	streamer.Stop()
	wg.Wait()
}

func TestStreamer_ClientCrashAfterSendingRequest(t *testing.T) {
	tmpFile, err := os.CreateTemp("/tmp", "streamer_test.sock")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	mockServer := startMockGRPCServer(t)
	defer mockServer.Stop()

	logger := log.NewLogfmtLogger(os.Stdout)
	config := StreamerConfig{
		socketPath:           tmpFile.Name(),
		storeAddrPort:        mockServer.addr,
		streamTimeoutSeconds: 10,
	}
	streamer, err := NewStreamer(config, logger)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := streamer.Run()
		assert.NoError(t, err)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Second) // Give time for server to start
		conn, err := net.Dial("unix", config.socketPath)
		require.NoError(t, err)
		defer conn.Close()
		request := &streamer_pkg.StreamerRequest{}
		data, err := streamer_pkg.Encode(request)
		assert.NoError(t, err)
		err = writeBytes(conn, []byte(data), []byte(streamer_pkg.Deliminator))
		assert.NoError(t, err)
		time.Sleep(2 * time.Second)
	}()
	time.Sleep(5 * time.Second)
	streamer.Stop()
	wg.Wait()
}

func TestStreamer_ClientCrashBeforeSendingRequest(t *testing.T) {
	tmpFile, err := os.CreateTemp("/tmp", "streamer_test.sock")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	mockServer := startMockGRPCServer(t)
	defer mockServer.Stop()

	logger := log.NewLogfmtLogger(os.Stdout)
	config := StreamerConfig{
		socketPath:           tmpFile.Name(),
		storeAddrPort:        mockServer.addr,
		streamTimeoutSeconds: 100,
	}
	streamer, err := NewStreamer(config, logger)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := streamer.Run()
		assert.NoError(t, err)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Second) // Give time for server to start
		conn, err := net.Dial("unix", config.socketPath)
		require.NoError(t, err)
		defer conn.Close()
		time.Sleep(2 * time.Second)
	}()
	time.Sleep(5 * time.Second)
	streamer.Stop()
	wg.Wait()
}

func TestStreamer_Stop(t *testing.T) {
	tmpFile, err := os.CreateTemp("/tmp", "streamer_test.sock")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	mockServer := startMockGRPCServer(t)
	defer mockServer.Stop()

	logger := log.NewLogfmtLogger(os.Stdout)
	config := StreamerConfig{
		socketPath:           tmpFile.Name(),
		storeAddrPort:        mockServer.addr,
		streamTimeoutSeconds: 3,
	}
	streamer, err := NewStreamer(config, logger)
	require.NoError(t, err)

	time.Sleep(time.Second)
	streamer.Stop()
	assert.NotNil(t, streamer.ctx.Err())
}

func TestSocket(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stdout)

	tmpFile, err := os.CreateTemp("/tmp", "streamer.sock.")
	assert.NoError(t, err)
	os.Remove(tmpFile.Name())

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	listenConfig := &net.ListenConfig{}
	listener, err := listenConfig.Listen(ctx, "unix", tmpFile.Name())
	require.NoError(t, err)

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := listener.Accept()
		require.NotNil(t, err)
		_, err = listener.Accept()
		require.NotNil(t, err)
		level.Info(logger).Log("msg", "listener.Accept() returned")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			time.Sleep(1 * time.Second)
		}
		assert.NotNil(t, ctx.Err())
		level.Info(logger).Log("msg", "context canceled")
	}()

	listener.Close()
	cancel()
	assert.NotNil(t, ctx.Err())
	wg.Wait()
}
