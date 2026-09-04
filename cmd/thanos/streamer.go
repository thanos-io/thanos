// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/prober"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/streamer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type StreamerConfig struct {
	socketPath           string
	storeAddrPort        string
	streamTimeoutSeconds int
	replicaLabel         string
	ignoreWarnings       bool
}

func registerStreamer(app *extkingpin.App) {
	config := StreamerConfig{}
	cmd := app.Command("streamer", "Run a streamer Unix socket server")
	cmd.Flag("socket.path", "Path to the Unix socket").Default("/tmp/thanos-streamer.sock").StringVar(&config.socketPath)
	cmd.Flag("store", "Thanos Store API gRPC endpoint").Default("localhost:10901").StringVar(&config.storeAddrPort)
	cmd.Flag("stream.timeout_seconds", "One stream's overall timeout in seconds ").Default("36000").IntVar(&config.streamTimeoutSeconds)
	cmd.Flag("stream.replica_label", "Drop this replica label from all returns time series and dedup them.").Default("").StringVar(&config.replicaLabel)
	cmd.Flag("stream.ignore_warnings", "Don't return an error when a warning response is received. --no-stream.ignore_warnings to disable").Default("true").BoolVar(&config.ignoreWarnings)

	hc := &httpConfig{}
	hc = hc.registerFlag(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, debugLogging bool) error {

		httpProbe := prober.NewHTTP()
		statusProber := httpProbe

		srv := httpserver.New(logger, reg, component.Replicate, httpProbe,
			httpserver.WithListen(hc.bindAddress),
			httpserver.WithGracePeriod(time.Duration(hc.gracePeriod)),
			httpserver.WithTLSConfig(hc.tlsConfig),
			httpserver.WithEnableH2C(true),
		)

		g.Add(func() error {
			statusProber.Healthy()

			return srv.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			defer statusProber.NotHealthy(err)

			srv.Shutdown(err)
		})
		streamer, err := NewStreamer(config, logger)
		if err != nil {
			return err
		}
		g.Add(func() error {
			statusProber.Ready()
			return streamer.Run()
		}, func(error) {
			statusProber.NotReady(err)
			streamer.Stop()
		})
		return nil
	})
}

type Streamer struct {
	config   StreamerConfig
	logger   log.Logger
	grpcConn *grpc.ClientConn
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	listener net.Listener
	shutdown atomic.Bool
}

func NewStreamer(
	config StreamerConfig,
	logger log.Logger) (*Streamer, error) {
	conn, err := grpc.NewClient(
		config.storeAddrPort,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	level.Info(logger).Log("msg", "connected to Store gRPC server", "addr", config.storeAddrPort)
	ctx, cancel := context.WithCancel(context.Background())
	return &Streamer{
		config:   config,
		logger:   logger,
		grpcConn: conn,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

type aggrChunkByTimestamp []storepb.AggrChunk

func (c aggrChunkByTimestamp) Len() int      { return len(c) }
func (c aggrChunkByTimestamp) Swap(i, j int) { c[i], c[j] = c[j], c[i] }
func (c aggrChunkByTimestamp) Less(i, j int) bool {
	return c[i].MinTime < c[j].MinTime ||
		// Sort the longer time range first.
		(c[i].MinTime == c[j].MinTime && c[i].MaxTime > c[j].MaxTime)
}

func convertToSeriesReq(config *StreamerConfig, streamerReq *streamer.StreamerRequest) *storepb.SeriesRequest {
	req := &storepb.SeriesRequest{
		Aggregates: []storepb.Aggr{storepb.Aggr_RAW},
		Matchers:   make([]storepb.LabelMatcher, 0),
		MinTime:    streamerReq.StartTimestampMs,
		MaxTime:    streamerReq.EndTimestampMs,
		SkipChunks: streamerReq.SkipChunks,
	}
	if config.replicaLabel != "" {
		req.WithoutReplicaLabels = []string{config.replicaLabel}
	}
	for _, labelMatcher := range streamerReq.LabelMatchers {
		req.Matchers = append(req.Matchers, storepb.LabelMatcher{
			Type:  storepb.LabelMatcher_Type(labelMatcher.Type),
			Name:  labelMatcher.Name,
			Value: labelMatcher.Value,
		})
	}
	if streamerReq.Metric != "" {
		req.Matchers = append(req.Matchers, storepb.LabelMatcher{
			Type:  storepb.LabelMatcher_EQ,
			Name:  labels.MetricName,
			Value: streamerReq.Metric,
		})
	}
	return req
}

// NB: writeBytes() is not atomic and may end up with a part of the bytes.
func writeBytes(writer io.Writer, bytes ...[]byte) error {
	for _, b := range bytes {
		if n, err := writer.Write(b); err != nil || n < len(b) {
			if err == nil {
				err = fmt.Errorf("unexpected number of bytes written: %d != %d", n, len(b))
			}
			return err
		}
	}
	return nil
}

// streamOneRequest() returns an error if any write fails. Partial data may be written.
// The expectation is the client will retry a failed stream.
func (s *Streamer) streamOneRequest(request *streamer.StreamerRequest, writer io.Writer) error {
	chunksTotal := 0
	timeSeriesTotal := int64(0)
	samplesTotal := int64(0)
	outOfTimeRangeSampleTotal := 0
	outOfOrderSampleTotal := 0
	duplicateSampleTotal := 0
	duplicateChunks := 0
	storeReq := convertToSeriesReq(&s.config, request)
	responseSeq := 0

	// Write on a socket connection can be blocked until the overall stream timeout (config.streamTimeoutSeconds)
	// if the reader is not reading and the buffer is full.
	writeResponse := func(resp *streamer.StreamerResponse) error {
		stringToWrite, err := streamer.Encode(resp)
		if err != nil {
			return err
		}
		if strings.Contains(stringToWrite, streamer.Deliminator) {
			return fmt.Errorf("response contains a delimiter")
		}
		bytesToWrite := []byte(stringToWrite)
		// The encoded response shouldn't container streamer.Deliminator.
		level.Debug(s.logger).Log(
			"msg", "writing a response",
			"seq", responseSeq,
			"encoded_size", len(bytesToWrite),
			// "encoded_response", string(bytesToWrite),
		)
		responseSeq++
		return writeBytes(writer, bytesToWrite, []byte(streamer.Deliminator))
	}

	level.Info(s.logger).Log(
		"msg", "sending one series request to Store",
		"request_id", request.RequestId,
		"req", storeReq.String(),
		"num_matchers", len(storeReq.Matchers),
		"matchers", fmt.Sprintf("%+v", storeReq.Matchers),
	)
	storeClient := storepb.NewStoreClient(s.grpcConn)
	storeRes, err := storeClient.Series(s.ctx, storeReq)
	if err != nil {
		level.Error(s.logger).Log(
			"err", err, "msg",
			"error in starting a streaming request to Store",
			"request_id", request.RequestId,
			"connection state", s.grpcConn.GetState().String(),
		)
		return writeResponse(&streamer.StreamerResponse{Err: err.Error()})
	}

	defer func() {
		if timeSeriesTotal > 0 {
			level.Info(s.logger).Log(
				"msg", "finished one streaming request",
				"request_id", request.RequestId,
				"time_series_total", timeSeriesTotal,
				"num_chunks", chunksTotal,
				"num_samples", samplesTotal,
				"minTimeMs", storeReq.MinTime,
				"unix_time", time.Unix(storeReq.MinTime/1000, 0).UTC(),
				"maxTimeMs", storeReq.MaxTime,
				"out_of_time_range_samples", outOfTimeRangeSampleTotal,
				"out_of_order_samples", outOfOrderSampleTotal,
				"duplicate_samples", duplicateSampleTotal,
				"dupliate_chunks", duplicateChunks,
			)
		}
	}()
	// The call site may cancel the context to stop the streaming.
	for !s.shutdown.Load() {
		response, err := storeRes.Recv()
		if err == io.EOF {
			level.Info(s.logger).Log(
				"msg", "a streaming request reached EOF",
				"request_id", request.RequestId,
				// storeReq.MinTime and storeRq.MaxTime are in milliseconds.
				"minTimeTsMs", storeReq.MinTime,
				"unix_time", time.Unix(storeReq.MinTime/1000, 0).UTC(),
			)
			return writeResponse(&streamer.StreamerResponse{Eof: true})
		}
		if err != nil {
			level.Error(s.logger).Log("err", err, "msg", "error in recv() from Store gRPC stream")
			return writeResponse(&streamer.StreamerResponse{Err: err.Error()})
		}
		if warning := response.GetWarning(); warning != "" {
			level.Warn(s.logger).Log(
				"msg", "warning response from Store gRPC stream",
				"warning", warning,
				"ignore_warnings", s.config.ignoreWarnings,
				"request_id", request.RequestId)
			if s.config.ignoreWarnings {
				continue
			}
			return writeResponse(&streamer.StreamerResponse{Err: fmt.Sprintf("warning response from Store gRPC stream: %s", warning)})
		}
		seriesResp := response.GetSeries()
		if seriesResp == nil {
			err := fmt.Errorf("unexpectedly missing series data")
			level.Error(s.logger).Log("err", err, "request_id", request.RequestId)
			return writeResponse(&streamer.StreamerResponse{Err: err.Error()})
		}
		timeSeries := &streamer.TimeSeries{}
		metricName := ""
		for _, label := range seriesResp.Labels {
			timeSeries.Labels = append(timeSeries.Labels, streamer.Label{
				Name:  label.Name,
				Value: label.Value,
			})
			if label.Name == labels.MetricName {
				metricName = label.Value
			}
		}
		sort.Sort(aggrChunkByTimestamp(seriesResp.Chunks))
		samples := 0
		outOfTimeRangeSamples := 0
		outOfOrderSamples := 0
		duplicateSamples := 0
		prevTs := int64(0)
		prevChunkMinTime := int64(0)
		for _, chunk := range seriesResp.Chunks {
			if chunk.Raw == nil {
				// We only ask for and handle RAW
				err := fmt.Errorf("unexpectedly missing raw chunk data")
				level.Error(s.logger).Log("err", err, "request_id", request.RequestId)
				return writeResponse(&streamer.StreamerResponse{Err: err.Error()})
			}
			if chunk.Raw.Type != storepb.Chunk_XOR {
				err := fmt.Errorf("unexpected encoding type: %v", chunk.Raw.Type)
				level.Error(s.logger).Log("err", err, "request_id", request.RequestId)
				return writeResponse(&streamer.StreamerResponse{Err: err.Error()})
			}
			level.Debug(s.logger).Log(
				"msg", "received a chunk for a time series",
				"request_id", request.RequestId,
				"metric_name", metricName,
				"chunk_min_time", chunk.MinTime,
				"chunk_min_time_unix", time.Unix(chunk.MinTime/1000, 0).UTC(),
				"duration_seconds", float64(chunk.MaxTime-chunk.MinTime)/1000,
				"num_samples", len(chunk.Raw.Data),
				// A duplicate chunk is common because offline store has replication factor > 1 to avoid data loss during release.
				"is_duplicate_chunk", prevChunkMinTime == chunk.MinTime,
			)
			if prevChunkMinTime == chunk.MinTime {
				// Chunks are sorted by MinTime.
				duplicateChunks++
				continue
			}
			prevChunkMinTime = chunk.MinTime

			raw, err := chunkenc.FromData(chunkenc.EncXOR, chunk.Raw.Data)
			if err != nil {
				level.Error(s.logger).Log("err", err, "msg", "error in decoding chunk", "request_id", request.RequestId)
				return writeResponse(&streamer.StreamerResponse{Err: err.Error()})
			}

			for iter := raw.Iterator(nil); iter.Next() != chunkenc.ValNone; {
				ts, value := iter.At()
				if ts < request.StartTimestampMs || ts > request.EndTimestampMs {
					outOfTimeRangeSamples++
				} else if prevTs == ts {
					duplicateSamples++
				} else if prevTs > ts {
					outOfOrderSamples++
					level.Debug(s.logger).Log(
						"msg", "out of order sample",
						"request_id", request.RequestId,
						"metric_name", metricName,
						"prev_ts", prevTs,
						"ts", ts,
					)
				} else {
					timeSeries.Samples = append(timeSeries.Samples, streamer.DataSample{
						// In milliseconds since epoch
						Timestamp: ts,
						Value:     value,
					})
					samples++
					prevTs = ts
				}
			}
		}
		if outOfOrderSamples > 0 {
			level.Debug(s.logger).Log(
				"msg", "out of order samples",
				"request_id", request.RequestId,
				"metric_name", metricName,
				"num_chunks", len(seriesResp.Chunks),
				"num_samples", samples,
				"num_out_of_order_samples", outOfOrderSamples,
				"num_duplicate_samples", duplicateSamples,
			)
		}
		level.Debug(s.logger).Log(
			"msg", "received and processed chunks for a time series",
			"request_id", request.RequestId,
			"metric_name", metricName,
			"num_chunks", len(seriesResp.Chunks),
			"num_samples", samples,
			"out_of_time_range_samples", outOfTimeRangeSamples,
			"num_duplicate_samples", duplicateSamples,
			"num_out_of_order_samples", outOfOrderSamples,
		)
		chunksTotal += len(seriesResp.Chunks)
		timeSeriesTotal++
		samplesTotal += int64(samples)
		outOfOrderSampleTotal += outOfOrderSamples
		duplicateSampleTotal += duplicateSamples
		outOfTimeRangeSampleTotal += outOfTimeRangeSamples
		if err := writeResponse(&streamer.StreamerResponse{Data: timeSeries}); err != nil {
			level.Error(s.logger).Log("err", err, "msg", "error in writing a data response")
			return err
		}
	}
	return writeResponse(&streamer.StreamerResponse{Err: "unexpected interruption"})
}

// Run() is blocking and call Stop() to make it exit.
func (s *Streamer) Run() error {
	s.wg.Add(1)
	defer s.wg.Done()

	os.Remove(s.config.socketPath)

	listenConfig := &net.ListenConfig{}
	listener, err := listenConfig.Listen(s.ctx, "unix", s.config.socketPath)
	if err != nil {
		level.Error(s.logger).Log("err", err, "msg", "error in listening on a Unix socket")
		return err
	}
	s.listener = listener

	level.Info(s.logger).Log("msg", "server is listening on", "socket_path", s.config.socketPath)
	for !s.shutdown.Load() {
		level.Info(s.logger).Log("msg", "waiting for a connection")
		conn, err := listener.Accept()
		if err != nil {
			level.Error(s.logger).Log("err", err, "msg", "error in accepting a connection")
			continue
		}
		s.wg.Add(1)
		go func(conn net.Conn) {
			defer conn.Close()
			defer s.wg.Done()

			// All reads/writes will fail after the deadline.
			deadline := time.Now().Add(time.Second * time.Duration(s.config.streamTimeoutSeconds))
			_ = conn.SetDeadline(deadline)

			level.Info(s.logger).Log("msg", "accepted a connection",
				"overall_timeout", s.config.streamTimeoutSeconds,
				"overall_deadline", deadline)
			scanner := bufio.NewScanner(conn)
			if !scanner.Scan() {
				level.Error(s.logger).Log("err", err, "msg", "error in reading a request")
				return
			}
			encodedRequest := scanner.Text()
			request := &streamer.StreamerRequest{}
			if err := request.Decode(encodedRequest); err != nil {
				level.Error(s.logger).Log("err", err, "msg", "error in decoding a request")
				return
			}
			level.Info(s.logger).Log(
				"msg", "received a request and starting a stream",
				"request_id", request.RequestId,
				"start_timestamp_ms", request.StartTimestampMs,
				"end_timestamp_ms", request.EndTimestampMs,
				"request", fmt.Sprintf("%+v", request),
			)
			err = s.streamOneRequest(request, conn)
			if err != nil {
				level.Error(s.logger).Log(
					"msg", "error in streaming a request",
					"err", err,
					"request_id", request.RequestId,
					"start_timestamp_ms", request.StartTimestampMs,
					"end_timestamp_ms", request.EndTimestampMs,
				)
			}
			level.Info(s.logger).Log("msg", "finished a request", "request_id", request.RequestId)
		}(conn)
	}
	return nil
}

// Don't use s again after calling Stop().
func (s *Streamer) Stop() {
	s.shutdown.Store(true)
	level.Info(s.logger).Log("msg", "stopping the server")
	if s.listener != nil {
		level.Info(s.logger).Log("msg", "closing the listener")
		s.listener.Close()
	}
	s.cancel()
	level.Info(s.logger).Log("msg", "canceld the context", "context_err", s.ctx.Err())
	s.wg.Wait()
	level.Info(s.logger).Log("msg", "the server stopped")
}
