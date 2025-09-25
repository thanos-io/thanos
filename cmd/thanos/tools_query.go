// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/run"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"

	apiv1 "github.com/thanos-io/thanos/pkg/api/query"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/query"
)

type toolsQueryConfig struct {
	// grpc URI for server; see https://github.com/grpc/grpc/blob/master/doc/naming.md
	server   string
	timeout  time.Duration
	insecure bool
	// TODO flags for TLS config
	tlsconfig           tls.Config
	maxResolution       time.Duration
	promqlOpts          query.Opts
	skipChunks          bool
	maxLookbackDelta    time.Duration
	defaultPromqlEngine string
}

type toolsQueryRangeConfig struct {
	toolsQueryConfig
	query     string
	startTime model.TimeOrDurationValue
	endTime   model.TimeOrDurationValue
	step      time.Duration
}

type toolsQueryInstantConfig struct {
	toolsQueryConfig
	query string
	time  model.TimeOrDurationValue
}

func (tqc *toolsQueryConfig) registerToolsQueryFlag(cmd extkingpin.FlagClause) {
	cmd.Flag("server", "gRPC URI of the Thanos endpoint to query, usually a Thanos Query instance. E.g. dns://thanos-query.thanos.svc.cluster.local:10901 . See https://github.com/grpc/grpc/blob/master/doc/naming.md").
		Short('u').Required().StringVar(&tqc.server)
	cmd.Flag("timeout", "Timeout befsore abandoning query execution").Default("5m").DurationVar(&tqc.timeout)
	cmd.Flag("insecure", "Do not use TLS when connecting to the gRPC server").Default("false").BoolVar(&tqc.insecure)

	cmd.Flag("max-resolution", "Maximum resolution (minimum step) to query from the server. This is useful to limit the resolution when querying downsampled data.").
		Default("0s").DurationVar(&tqc.maxResolution)
	cmd.Flag("partial-response", "Whether to accept partial responses from the server when some of the data sources are down").
		Default("false").BoolVar(&tqc.promqlOpts.PartialResponse)
	cmd.Flag("query.replica-label", "A comma-separated or repeated-argument list of replica labels for deduplication. If omitted, no deduplication is performed").
		StringsVar(&tqc.promqlOpts.ReplicaLabels) // implies deduplicate if set
	cmd.Flag("query.promql-engine", "Default PromQL engine to use.").Default(string(apiv1.PromqlEnginePrometheus)).
		EnumVar(&tqc.defaultPromqlEngine, string(apiv1.PromqlEnginePrometheus), string(apiv1.PromqlEngineThanos))
	cmd.Flag("query.lookback-delta", "The maximum lookback duration for retrieving metrics during expression evaluations. PromQL always evaluates the query for the certain timestamp (query range timestamps are deduced by step). Since scrape intervals might be different, PromQL looks back for given amount of time to get latest sample. If it exceeds the maximum lookback delta it assumes series is stale and returns none (a gap). This is why lookback delta should be set to at least 2 times of the slowest scrape interval. If unset it will use the promql default of 5m.").DurationVar(&tqc.maxLookbackDelta)
}

func (tqrc *toolsQueryRangeConfig) registerToolsQueryRangeFlag(cmd extkingpin.FlagClause) {
	tqrc.registerToolsQueryFlag(cmd)
	cmd.Flag("query", "Query to execute against the Thanos endpoint.").
		Short('q').Required().StringVar(&tqrc.query)
	cmd.Flag("start", "Range-query evaluation start time. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Required().SetValue(&tqrc.startTime)
	cmd.Flag("end", "Range-query evaluation end time. Default current time. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0s").SetValue(&tqrc.endTime)
	cmd.Flag("step", "Range-query step interval").Required().DurationVar(&tqrc.step)
}

func (tqic *toolsQueryInstantConfig) registerToolsQueryInstantFlag(cmd extkingpin.FlagClause) {
	tqic.registerToolsQueryFlag(cmd)
	cmd.Flag("query", "Query to execute against the Thanos endpoint.").
		Short('q').Required().StringVar(&tqic.query)
	cmd.Flag("time", "Instant-query evaluation start time. Default now. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0s").SetValue(&tqic.time)
}

func registerToolsQuery(app extkingpin.AppClause) {
	cmd := app.Command("query", "Utility commands for querying Thanos endpoints")
	registerToolsQueryRange(cmd)
	registerToolsQueryInstant(cmd)
}

func (tqc *toolsQueryConfig) engineType() querypb.EngineType {
	switch tqc.defaultPromqlEngine {
	case string(apiv1.PromqlEnginePrometheus):
		return querypb.EngineType_prometheus
	case string(apiv1.PromqlEngineThanos):
		return querypb.EngineType_thanos
	default:
		fmt.Fprintf(os.Stderr, "warning: unknown promql engine type %q, defaulting to prometheus\n", tqc.defaultPromqlEngine)
		return querypb.EngineType_prometheus
	}
}
func (tqc *toolsQueryConfig) createClient() (querypb.QueryClient, *grpc.ClientConn, error) {
	opts := []grpc.DialOption{}
	if tqc.insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tqc.tlsconfig)))
		fmt.Fprintf(os.Stderr, "warning: TLS configuration not implemented")
	}
	conn, err := grpc.NewClient(tqc.server, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}
	return querypb.NewQueryClient(conn), conn, nil
}

// Run a range query via Thanos gRPC API; like 'promtool query range'
func registerToolsQueryRange(app extkingpin.AppClause) {
	cmd := app.Command("range", "Run an range query on a Thanos gRPC endpoint.")

	tqr := &toolsQueryRangeConfig{}
	tqr.registerToolsQueryRangeFlag(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, _ *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		// Ensure that we exit by providing a placeholder actor to the run group.
		g.Add(func() error { return nil }, func(error) {})

		// Ensure we can cancel the query in response to SIGINT or timeout
		// TODO should use a different timeout for the query than the app
		ctx, cancel := context.WithTimeout(context.Background(), tqr.timeout)
		defer cancel()

		warnOnFutureTimestamp(tqr.startTime, "--start")
		warnOnFutureTimestamp(tqr.endTime, "--end")
		if tqr.startTime.PrometheusTimestamp() >= tqr.endTime.PrometheusTimestamp() {
			if tqr.startTime.Dur != nil && tqr.endTime.Dur != nil {
				return fmt.Errorf("start duration must be before end duration, did you mean --start=-%s --end=-%s?", tqr.endTime.Dur.String(), tqr.startTime.Dur.String())
			}
			return fmt.Errorf("start time must be before end time")
		}

		qc, _, err := tqr.createClient()
		if err != nil {
			return err
		}
		req := &querypb.QueryRangeRequest{
			Query:                 tqr.query,
			StartTimeSeconds:      tqr.startTime.PrometheusTimestamp() / 1000,
			EndTimeSeconds:        tqr.endTime.PrometheusTimestamp() / 1000,
			IntervalSeconds:       int64(tqr.step.Seconds()),
			TimeoutSeconds:        int64(tqr.timeout.Seconds()),
			MaxResolutionSeconds:  int64(tqr.maxResolution.Seconds()),
			ReplicaLabels:         tqr.promqlOpts.ReplicaLabels,
			EnableDedup:           len(tqr.promqlOpts.ReplicaLabels) > 0,
			EnablePartialResponse: tqr.promqlOpts.PartialResponse,
			SkipChunks:            tqr.skipChunks,
			LookbackDeltaSeconds:  int64(tqr.maxLookbackDelta.Seconds()),
			Engine:                tqr.engineType(),
		}
		fmt.Fprintf(os.Stderr, "QueryRangeRequest: %+v\n", req)
		fmt.Fprintf(os.Stderr, "Issuing gRPC QueryRangeRequest: %+v\n", req)
		resp, err := qc.QueryRange(ctx, req)
		if err != nil {
			return fmt.Errorf("QueryRange gRPC error: %v\n", err)
		}
		defer resp.CloseSend()

		for {
			msg, err := resp.Recv()
			switch err {
			case nil:
				fmt.Fprintf(os.Stdout, "QueryRange response message: %+v\n", msg)
			case io.EOF:
				fmt.Fprintf(os.Stderr, "QueryRange: EOF\n")
				return nil
			default:
				return fmt.Errorf("QueryRange RecvMsg error: %v\n", err)
			}
		}
	})
}

// Run an instant query via Thanos gRPC API; like 'promtool query instant'
func registerToolsQueryInstant(app extkingpin.AppClause) {
	cmd := app.Command("instant", "Run an instant query on a Thanos gRPC endpoint.")

	tqi := &toolsQueryInstantConfig{}
	tqi.registerToolsQueryInstantFlag(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, _ *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		// Ensure that we exit by providing a placeholder actor to the run group.
		g.Add(func() error { return nil }, func(error) {})

		// Ensure we can cancel the query in response to SIGINT or timeout
		// TODO should use a different timeout for the query than the app
		ctx, cancel := context.WithTimeout(context.Background(), tqi.timeout)
		defer cancel()

		warnOnFutureTimestamp(tqi.time, "--time")

		qc, _, err := tqi.createClient()
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "t=%+v\n", tqi.time)
		req := &querypb.QueryRequest{
			Query:                 tqi.query,
			TimeSeconds:           tqi.time.PrometheusTimestamp() / 1000,
			TimeoutSeconds:        int64(tqi.timeout.Seconds()),
			MaxResolutionSeconds:  int64(tqi.maxResolution.Seconds()),
			ReplicaLabels:         tqi.promqlOpts.ReplicaLabels,
			EnableDedup:           len(tqi.promqlOpts.ReplicaLabels) > 0,
			EnablePartialResponse: tqi.promqlOpts.PartialResponse,
			SkipChunks:            tqi.skipChunks,
			LookbackDeltaSeconds:  int64(tqi.maxLookbackDelta.Seconds()),
			Engine:                tqi.engineType(),
		}
		fmt.Fprintf(os.Stderr, "QueryRequest: %+v\n", req)
		fmt.Fprintf(os.Stderr, "Issuing gRPC QueryRequest: %+v\n", req)
		resp, err := qc.Query(ctx, req)
		if err != nil {
			return fmt.Errorf("Query gRPC error: %v\n", err)
		}
		defer resp.CloseSend()

		for {
			msg, err := resp.Recv()
			switch err {
			case nil:
				fmt.Fprintf(os.Stdout, "QueryRange response message: %+v\n", msg)
			case io.EOF:
				fmt.Fprintf(os.Stderr, "QueryRange: EOF\n")
				return nil
			default:
				return fmt.Errorf("QueryRange RecvMsg error: %v\n", err)
			}
		}
	})
}

func warnOnFutureTimestamp(t model.TimeOrDurationValue, desc string) {
	if t.PrometheusTimestamp()/1000 > time.Now().Unix() {
		if t.Dur != nil {
			fmt.Fprintf(os.Stderr, "warning: %s is in the future, did you mean --time=-%s?\n", desc, t.Dur.String())
		} else {
			fmt.Fprintf(os.Stderr, "warning: %s is in the future\n", desc)
		}
	}
}
