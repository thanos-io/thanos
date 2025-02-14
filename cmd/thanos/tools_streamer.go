// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/streamer"

	streamer_pkg "github.com/thanos-io/thanos/pkg/streamer"
)

type streamerToolConfig struct {
	socketPath string
	metric     string
	hoursAgo   int
	skipChunks bool
}

func registerStreamerTool(app extkingpin.AppClause) {
	cmd := app.Command("streamer", "Stream time series for a metric")

	conf := &streamerToolConfig{}
	cmd.Flag("socket.path", "Path to the Unix socket").Default("/tmp/thanos-streamer.sock").StringVar(&conf.socketPath)
	cmd.Flag("metric", "The metric name to stream time series for this metric").Default("node_cpu_seconds_total").StringVar(&conf.metric)
	cmd.Flag("hours_ago", "Stream the metric from this number of hours ago").Default("16").IntVar(&conf.hoursAgo)
	cmd.Flag("skip_chunks", "Skip chunks in the response").Default("false").BoolVar(&conf.skipChunks)

	cmd.Setup(func(
		g *run.Group,
		logger log.Logger,
		_ *prometheus.Registry,
		_ opentracing.Tracer,
		_ <-chan struct{},
		_ bool) error {
		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})
		err := runStreamerTool(conf, logger)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to run streamer tool", "err", err)
		}
		return err
	})
}

func runStreamerTool(conf *streamerToolConfig, logger log.Logger) error {
	conn, err := net.Dial("unix", conf.socketPath)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to connect to the Unix socket", "err", err, "socket", conf.socketPath)
		return err
	}
	defer conn.Close()

	nowMs := time.Now().Unix() * 1000
	startMs := nowMs - int64(conf.hoursAgo)*3600*1000
	request := &streamer.StreamerRequest{
		RequestId:        conf.metric,
		StartTimestampMs: startMs,
		EndTimestampMs:   startMs + 4*3600*1000,
		SkipChunks:       conf.skipChunks,
		Metric:           conf.metric,
		LabelMatchers: []streamer_pkg.LabelMatcher{
			{
				Name:  "__replica__",
				Value: "",
				Type:  streamer_pkg.LabelMatcher_EQ,
			},
		},
	}

	level.Info(logger).Log(
		"msg", "sending a socket request to Thanos streamer",
		"req", fmt.Sprintf("%+v", request),
	)

	data, err := streamer_pkg.Encode(request)
	if err != nil {
		level.Error(logger).Log("msg", "failed to encode a request", "err", err)
		return err
	}
	err = writeBytes(conn, []byte(data), []byte(streamer_pkg.Deliminator))
	if err != nil {
		level.Error(logger).Log("msg", "failed to write a request to the Unix socket", "err", err)
		return err
	}
	seq := 0
	scanner := bufio.NewScanner(conn)
	for ; scanner.Scan(); seq++ {
		encodedResp, err := scanner.Text(), scanner.Err()
		if err != nil {
			level.Error(logger).Log("msg", "failed to read a response from the Unix socket", "err", err)
			return err
		}
		resp := &streamer_pkg.StreamerResponse{}
		level.Info(logger).Log(
			"msg", "received a response from the Unix socket",
			"seq", seq,
			"encoded_size", len(encodedResp),
			// "encoded_response", encodedResp,
		)
		err = resp.Decode(encodedResp)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to decode a response from the Unix socket", "err", err,
				"encoded_size", len(encodedResp),
				"seq", seq,
				"encoded_response", fmt.Sprintf("%+v", encodedResp))
			return err
		}
		if resp.Eof {
			level.Info(logger).Log("msg", "successfully streamed all time series for the metric", "metric", conf.metric, "num_series", seq)
			return nil
		}
		if resp.Err != "" {
			level.Error(logger).Log("msg", "error in response", "err", resp.Err)
			return nil
		}
		if 0 == (seq % 1000) {
			level.Info(logger).Log("msg", "streaming time series", "seq", seq)
		}
		series := resp.Data
		metric := ""
		fmt.Printf("{")
		for _, label := range series.Labels {
			name := strings.Clone(label.Name)
			value := strings.Clone(label.Value)
			fmt.Printf("%s=%s,", label.Name, label.Value)
			if name == labels.MetricName {
				metric = value
			}
		}
		fmt.Printf("}")
		if metric != conf.metric {
			return fmt.Errorf("%d-th time series from the response has a different metric name:\n   Actual: %+v\n   Expected: %+v",
				seq, []byte(metric), []byte(conf.metric))
		}
		order := 0
		for _, sample := range series.Samples {
			if order < 5 {
				fmt.Printf(" %f @%d,", sample.Value, sample.Timestamp)
			}
			order++
		}
		fmt.Printf("\n")
	}
	return fmt.Errorf("unexpected interruption: %w", scanner.Err())
}
