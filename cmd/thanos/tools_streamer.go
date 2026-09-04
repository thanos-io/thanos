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
	socketPath           string
	metric               string
	hoursAgo             int
	hours                int
	skipChunks           bool
	equalLabelMatcher    string
	notEqualLabelMatcher string
	sleepSeconds         int
}

func registerStreamerTool(app extkingpin.AppClause) {
	cmd := app.Command("streamer", "Stream time series for a metric")

	conf := &streamerToolConfig{}
	cmd.Flag("socket.path", "Path to the Unix socket").Default("/tmp/thanos-streamer.sock").StringVar(&conf.socketPath)
	cmd.Flag("metric", "The metric name to stream time series for this metric").Default("node_cpu_seconds_total").StringVar(&conf.metric)
	cmd.Flag("hours_ago", "Stream the metric from this number of hours ago").Default("2").IntVar(&conf.hoursAgo)
	cmd.Flag("hours", "Stream the metric in the time window for this number of hours").Default("2").IntVar(&conf.hours)
	cmd.Flag("skip_chunks", "Skip chunks in the response").Default("false").BoolVar(&conf.skipChunks)
	cmd.Flag("eq_label_matcher", "One label matcher using equal in the format of name:value").Default("").StringVar(&conf.equalLabelMatcher)
	cmd.Flag("neq_label_matcher", "One label matcher using not equal in the format of name:value").Default("").StringVar(&conf.notEqualLabelMatcher)
	cmd.Flag("sleep_seconds_between_calls", "Sleep this amount of seconds between calling two gRPC calls").Default("0").IntVar(&conf.sleepSeconds)

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

	nowTime := time.Now()
	startTime := nowTime.Add(-time.Duration(conf.hoursAgo) * time.Hour)
	endTime := startTime.Add(time.Duration(conf.hours) * time.Hour)
	request := &streamer.StreamerRequest{
		RequestId:        conf.metric,
		StartTimestampMs: startTime.UnixMilli(),
		EndTimestampMs:   endTime.UnixMilli(),
		SkipChunks:       conf.skipChunks,
		Metric:           conf.metric,
	}
	addMatcher := func(mtype streamer_pkg.LabelMatcher_Type, matcher string) {
		if matcher == "" {
			return
		}
		parts := strings.Split(matcher, ":")
		if len(parts) != 2 {
			level.Error(logger).Log("msg", "ignoring an invalid label matcher", "matcher", matcher)
			return
		}
		request.LabelMatchers = append(request.LabelMatchers, streamer_pkg.LabelMatcher{
			Type:  mtype,
			Name:  parts[0],
			Value: parts[1],
		})
	}
	addMatcher(streamer_pkg.LabelMatcher_EQ, conf.equalLabelMatcher)
	addMatcher(streamer_pkg.LabelMatcher_NEQ, conf.notEqualLabelMatcher)

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
	scanner := bufio.NewScanner(conn)
	for seq := 0; scanner.Scan(); seq++ {
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
		if (seq % 1000) == 0 {
			level.Info(logger).Log("msg", "streaming time series", "seq", seq)
		}
		series := resp.Data
		fmt.Printf("%s{", conf.metric)
		comma := ""
		for _, label := range series.Labels {
			if label.Name != labels.MetricName {
				fmt.Printf("%s%s=%s,", comma, label.Name, label.Value)
				comma = ","
			}
		}
		fmt.Printf("}")
		for _, sample := range series.Samples {
			fmt.Printf(" [%d %.5f]", sample.Timestamp, sample.Value)
		}
		fmt.Print("")
		if conf.sleepSeconds > 0 {
			time.Sleep(time.Duration(conf.sleepSeconds) * time.Second)
		}
	}
	return fmt.Errorf("unexpected interruption: %w", scanner.Err())
}
