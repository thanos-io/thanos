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
	skipChunks           bool
	equalLabelMatcher    string
	notEqualLabelMatcher string
}

func registerFlags(cmd extkingpin.FlagClause) *rawMetricConfig {
	conf := &rawMetricConfig{}
	cmd.Flag("store", "Thanos Store API gRPC endpoint").Default("localhost:10901").StringVar(&conf.storeAddr)
	cmd.Flag("metric", "The metric name to stream time series for this metric").Default("node_cpu_seconds_total").StringVar(&conf.metric)
	cmd.Flag("hours_ago", "Stream the metric from this number of hours ago").Default("16").IntVar(&conf.hoursAgo)
	cmd.Flag("skip_chunks", "Skip chunks in the response").Default("false").BoolVar(&conf.skipChunks)
	cmd.Flag("eq_label_matcher", "One label matcher using equal").Default("").StringVar(&conf.equalLabelMatcher)
	cmd.Flag("neq_label_matcher", "One label matcher using not equal").Default("").StringVar(&conf.notEqualLabelMatcher)
	return conf
}

func registerStreamMetric(app extkingpin.AppClause) {
	cmd := app.Command("metric", "Stream time series for a metric")

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
	nowMs := time.Now().Unix() * 1000
	startMs := nowMs - int64(conf.hoursAgo)*3600*1000
	conn, err := grpc.Dial(
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
		parts := strings.Split(matcher, "=")
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
		MinTime:    startMs,
		MaxTime:    4*3600*1000 + startMs,
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
		resPtr, err := storeRes.Recv()
		if err == io.EOF {
			level.Info(logger).Log("msg", "Got EOF from store")
			break
		}
		if err != nil {
			return err
		}
		series := resPtr.GetSeries()
		if series == nil {
			return fmt.Errorf("got a nil series")
		}
		if (seq % 1000) == 0 {
			level.Info(logger).Log("msg", "streaming time series", "seq", seq)
		}
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
		for _, chunk := range series.Chunks {
			raw, err := chunkenc.FromData(chunkenc.EncXOR, chunk.Raw.Data)
			if err != nil {
				level.Error(logger).Log("err", err, "msg", "error in decoding chunk")
				continue
			}

			iter := raw.Iterator(nil)
			for iter.Next() != chunkenc.ValNone {
				ts, value := iter.At()
				if order < 5 {
					fmt.Printf(" %f @%d,", value, ts)
				}
				order++
			}
		}
		fmt.Printf("\n")
	}
	level.Info(logger).Log("msg", "successfully streamed all time series for the metric", "metric", conf.metric, "num_series", seq)
	return nil
}
