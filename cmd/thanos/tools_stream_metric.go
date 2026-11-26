// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type rawMetricConfig struct {
	storeAddr            string
	metric               string
	hoursAgo             int
	hours                int
	skipChunks           bool
	ignoreWarnings       bool
	equalLabelMatcher    string
	notEqualLabelMatcher string
}

func registerFlags(cmd extkingpin.FlagClause) *rawMetricConfig {
	conf := &rawMetricConfig{}
	cmd.Flag("store", "Thanos Store API gRPC endpoint").Default("localhost:10901").StringVar(&conf.storeAddr)
	cmd.Flag("metric", "The metric name to stream time series for this metric").Default("up").StringVar(&conf.metric)
	cmd.Flag("hours_ago", "Stream the metric from this number of hours ago").Default("2").IntVar(&conf.hoursAgo)
	cmd.Flag("hours", "Stream data samples for this number of hours").Default("2").IntVar(&conf.hours)
	cmd.Flag("skip_chunks", "Skip chunks in the response").Default("false").BoolVar(&conf.skipChunks)
	cmd.Flag("ignore_warnings", "Ignore warnings in the response. If false, exits and return an error.").Default("false").BoolVar(&conf.ignoreWarnings)
	cmd.Flag("eq_label_matcher", "One label matcher using equal in the format name:value").Default("").StringVar(&conf.equalLabelMatcher)
	cmd.Flag("neq_label_matcher", "One label matcher using not equal in the format name:value").Default("").StringVar(&conf.notEqualLabelMatcher)
	return conf
}

func registerStreamMetric(app extkingpin.AppClause) {
	cmd := app.Command("stream_metric", "Stream time series for a metric")

	conf := registerFlags(cmd)
	cmd.Setup(func(
		g *run.Group,
		logger log.Logger,
		_ *prometheus.Registry,
		_ opentracing.Tracer,
		_ <-chan struct{},
		_ bool) error {
		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})
		return streamMetric(conf, logger)
	})
}

func streamMetric(conf *rawMetricConfig, logger log.Logger) error {
	nowTime := time.Now()
	startTime := nowTime.Add(-time.Duration(conf.hoursAgo) * time.Hour)
	endTime := startTime.Add(time.Duration(conf.hours) * time.Hour)
	conn, err := grpc.NewClient(
		conf.storeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to create gRPC client", "err", err, "store", conf.storeAddr)
		return err
	}
	storeClient := storepb.NewStoreClient(conn)
	labelMatchers := []storepb.LabelMatcher{
		{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: conf.metric},
	}
	addMatcher := func(mtype storepb.LabelMatcher_Type, matcher string) {
		parts := strings.Split(matcher, ":")
		if len(parts) != 2 {
			level.Error(logger).Log("msg", "ignoring an invalid label matcher", "matcher", matcher)
			return
		}
		labelMatchers = append(labelMatchers, storepb.LabelMatcher{
			Type:  mtype,
			Name:  parts[0],
			Value: parts[1],
		})
	}
	addMatcher(storepb.LabelMatcher_EQ, conf.equalLabelMatcher)
	addMatcher(storepb.LabelMatcher_NEQ, conf.notEqualLabelMatcher)
	storeReq := &storepb.SeriesRequest{
		Aggregates: []storepb.Aggr{storepb.Aggr_RAW},
		Matchers:   labelMatchers,
		MinTime:    startTime.UnixMilli(),
		MaxTime:    endTime.UnixMilli(),
		SkipChunks: conf.skipChunks,
	}

	level.Info(logger).Log(
		"msg", "sending a gRPC call to store",
		"num_label_matchers", len(storeReq.Matchers),
		"label_matchers", fmt.Sprintf("%+v", storeReq.Matchers),
		"req", storeReq.String(),
	)

	storeRes, err := storeClient.Series(context.Background(), storeReq)
	if err != nil {
		return err
	}
	seq := 0
	for ; true; seq++ {
		resp, err := storeRes.Recv()
		if err == io.EOF {
			level.Info(logger).Log("msg", "Got EOF from store")
			break
		}
		if err != nil {
			return err
		}
		warning := resp.GetWarning()
		if len(warning) > 0 {
			level.Warn(logger).Log("msg", "got a warning from store", "warning", warning)
			if !conf.ignoreWarnings {
				return fmt.Errorf("got a warning from store: %v", warning)
			}
		}
		series := resp.GetSeries()
		if series == nil {
			return fmt.Errorf("unexpected nil series")
		}
		if (seq % 1000) == 0 {
			level.Info(logger).Log("msg", "streaming time series", "seq", seq)
		}
		fmt.Printf("%s{", conf.metric)
		comma := ""
		for _, label := range series.Labels {
			if label.Name != labels.MetricName {
				fmt.Printf("%s%s=\"%s\",", comma, label.Name, label.Value)
				comma = ","
			}
		}
		fmt.Printf("}")
		for i, chunk := range series.Chunks {
			raw, err := chunkenc.FromData(chunkenc.EncXOR, chunk.Raw.Data)
			if err != nil {
				level.Error(logger).Log("err", err, "msg", "error in decoding chunk")
				return err
			}
			fmt.Printf("  chunk %d:", i)
			iter := raw.Iterator(nil)
			for iter.Next() != chunkenc.ValNone {
				ts, value := iter.At()
				fmt.Printf(" [%d, %.2f]", ts, value)
			}
		}
		fmt.Printf("\n")
	}
	level.Info(logger).Log("msg", "successfully streamed all time series for the metric", "metric", conf.metric, "num_series", seq)
	return nil
}
