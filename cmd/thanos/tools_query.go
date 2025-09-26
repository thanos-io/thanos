// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
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
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

type toolsQueryConfig struct {
	// grpc URI for server; see https://github.com/grpc/grpc/blob/master/doc/naming.md
	server   string
	timeout  time.Duration
	insecure bool
	// TODO flags for TLS config
	tlsconfig tls.Config
	// Flags for all queries
	maxResolution       time.Duration
	promqlOpts          query.Opts
	skipChunks          bool
	maxLookbackDelta    time.Duration
	defaultPromqlEngine string
	// Promtool compatible output format option
	outFormat string
	// Debug output
	verbose bool
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

	cmd.Flag("format", "Output format: promql (text format), json, jsonstream, raw (go structs). Note that json output is all buffered in memory, consider using jsonstream. Json output format will change to be promtool compatible in future.").
		Default("promql").EnumVar(&tqc.outFormat, "promql", "json", "jsonstream", "raw")
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

func (tqc *toolsQueryConfig) createPrinter(logger log.Logger, instant bool) (printer, error) {
	switch tqc.outFormat {
	case "promql":
		return &promqlPrinter{
			logger:  logger,
			out:     os.Stdout,
			instant: instant,
		}, nil
	case "json":
		return &jsonPrinter{
			logger:  logger,
			out:     os.Stdout,
			instant: instant,
		}, nil
	case "jsonstream":
		return &jsonStreamPrinter{
			logger:  logger,
			out:     os.Stdout,
			instant: instant,
		}, nil
	case "raw":
		return &rawPrinter{
			logger:  logger,
			out:     os.Stdout,
			instant: instant,
		}, nil
	default:
		return nil, fmt.Errorf("unknown or unimplemented output format %q", tqc.outFormat)
	}
}

func (tqc *toolsQueryConfig) printRangeResponse(logger log.Logger, p printer, msg *querypb.QueryRangeResponse) {
	level.Debug(logger).Log("msg", "QueryRange response message",
		"type", fmt.Sprintf("%T", msg), "msg", fmt.Sprintf("%+v", msg))
	switch r := msg.Result.(type) {
	case *querypb.QueryRangeResponse_Timeseries:
		p.printSeries(*r.Timeseries)
	case *querypb.QueryRangeResponse_Stats:
		p.printStats(r.Stats)
	case *querypb.QueryRangeResponse_Warnings:
		p.printWarnings(r.Warnings)
	default:
		level.Warn(logger).Log("msg", "unknown QueryRangeResponse type",
			"type", fmt.Sprintf("%T", r), "value", fmt.Sprintf("%+v", r))
	}
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

		p, err := tqr.createPrinter(logger, false)
		if err != nil {
			return err
		}
		defer p.finish()

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
		level.Debug(logger).Log("QueryRangeRequest", req)
		resp, err := qc.QueryRange(ctx, req)
		if err != nil {
			return fmt.Errorf("QueryRange gRPC error: %v\n", err)
		}
		defer resp.CloseSend()

		for {
			msg, err := resp.Recv()
			switch err {
			case nil:
				tqr.printRangeResponse(logger, p, msg)
			case io.EOF:
				level.Debug(logger).Log("msg", "QueryRange: EOF")
				return nil
			default:
				return fmt.Errorf("QueryRange RecvMsg error: %v\n", err)
			}
		}
	})
}

func (tqc *toolsQueryConfig) printInstantResponse(logger log.Logger, p printer, msg *querypb.QueryResponse) {
	level.Debug(logger).Log("msg", "QueryRange response message",
		"type", fmt.Sprintf("%T", msg), "msg", fmt.Sprintf("%+v", msg))
	switch r := msg.Result.(type) {
	case *querypb.QueryResponse_Timeseries:
		p.printSeries(*r.Timeseries)
	case *querypb.QueryResponse_Stats:
		p.printStats(r.Stats)
	case *querypb.QueryResponse_Warnings:
		p.printWarnings(r.Warnings)
	default:
		level.Warn(logger).Log("msg", "unknown QueryRangeResponse type",
			"type", fmt.Sprintf("%T", r), "value", fmt.Sprintf("%+v", r))
	}
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

		p, err := tqi.createPrinter(logger, true)
		if err != nil {
			return err
		}
		defer p.finish()

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
				tqi.printInstantResponse(logger, p, msg)
			case io.EOF:
				level.Debug(logger).Log("msg", "QueryRange: EOF")
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

/* TODO should these formatters live somewhere else, e.g. in pkg/internal ? */

type printer interface {
	printSeries(v prompb.TimeSeries)
	printStats(v *querypb.QueryStats)
	printWarnings(v string)
	finish()
}

type promqlPrinter struct {
	logger  log.Logger
	out     io.Writer
	instant bool
}

// TODO proper escaping of label values
func (p *promqlPrinter) printSeries(val prompb.TimeSeries) {
	var nameLabel string
	var seriesIdentifier strings.Builder
	// Build the series identifier and extract any name label.
	for _, l := range val.Labels {
		if l.Name == "__name__" {
			nameLabel = l.Value
			continue
		}
		if seriesIdentifier.Len() > 0 {
			seriesIdentifier.Write([]byte(", "))
		} else {
			seriesIdentifier.Write([]byte("{"))
		}
		seriesIdentifier.WriteString(l.Name)
		seriesIdentifier.WriteString("=")
		// TODO check if the quoting rules are exactly the same as Prometheus
		seriesIdentifier.WriteString(fmt.Sprintf("%q", l.Value))
	}
	if seriesIdentifier.Len() > 0 {
		seriesIdentifier.WriteByte('}')
	}

	fmt.Fprintf(p.out, "%s%s ", nameLabel, seriesIdentifier.String())
	if p.instant {
		// one sample
		if len(val.Samples) > 1 {
			level.Warn(p.logger).Log("msg", "instant query series has multiple samples, showing only first")
		}
		p.printSampleValue(val.Samples[0])
		p.out.Write([]byte("\n"))
	} else {
		// range of samples, newline after each series
		fmt.Fprint(p.out, "\n")
		for _, s := range val.Samples {
			p.printSampleValue(s)
			p.out.Write([]byte("\n"))
		}
	}
}

// TODO histograms and exemplars
func (p *promqlPrinter) printSampleValue(s prompb.Sample) {
	t := time.Unix(s.Timestamp/1000, (s.Timestamp%1000)*1e6)
	fmt.Fprintf(p.out, "%f %f", s.Value, float64(t.UnixMicro())/1000)
}

func (p *promqlPrinter) printStats(v *querypb.QueryStats) {
	fmt.Fprintf(p.out, "# stats: peak_samples=%d, total_samples=%d\n", v.PeakSamples, v.SamplesTotal)
}

func (p *promqlPrinter) printWarnings(v string) {
	fmt.Fprintf(p.out, "# warning: %s\n", v)
}

func (p *promqlPrinter) finish() {
	fmt.Fprintf(p.out, "\n")
}

type rawPrinter struct {
	logger  log.Logger
	out     io.Writer
	instant bool
}

func (p *rawPrinter) printSeries(v prompb.TimeSeries) {
	fmt.Fprintf(p.out, "%+v\n", v)
}

func (p *rawPrinter) printStats(v *querypb.QueryStats) {
	fmt.Fprintf(p.out, "%+v\n", v)
}

func (p *rawPrinter) printWarnings(v string) {
	fmt.Fprintf(p.out, "warning: %s\n", v)
}

func (p *rawPrinter) finish() {}

// the json printer must accumulate in memory, unless we use a separate tool that
// can generate a stream of json, or use text processing hacks. For now, just
// build it in memory.
type jsonPrinter struct {
	logger   log.Logger
	out      io.Writer
	series   []prompb.TimeSeries
	stats    []*querypb.QueryStats
	warnings []string
	instant  bool
}

func (p *jsonPrinter) printSeries(v prompb.TimeSeries) {
	p.series = append(p.series, v)
}

func (p *jsonPrinter) printStats(v *querypb.QueryStats) {
	p.stats = append(p.stats, v)
}

func (p *jsonPrinter) printWarnings(v string) {
	p.warnings = append(p.warnings, v)
}

func (p *jsonPrinter) finish() {
	// TODO create promtool-compatible JSON output
	type jsonOutput struct {
		Series   []prompb.TimeSeries   `json:"series,omitempty"`
		Stats    []*querypb.QueryStats `json:"stats,omitempty"`
		Warnings []string              `json:"warnings,omitempty"`
	}
	out := jsonOutput{
		Series:   p.series,
		Stats:    p.stats,
		Warnings: p.warnings,
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		level.Error(p.logger).Log("msg", "cannot marshal output to JSON", "error", err)
		return
	}
	_, err = p.out.Write(b)
	if err != nil {
		level.Error(p.logger).Log("msg", "cannot write JSON to output", "error", err)
		return
	}
}

type jsonStreamPrinter struct {
	logger  log.Logger
	out     io.Writer
	instant bool
}

type jsonMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

func (p *jsonStreamPrinter) printSeries(v prompb.TimeSeries) {
	b, err := json.Marshal(v)
	if err != nil {
		level.Error(p.logger).Log("msg", "cannot marshal series to JSON", "error", err)
		return
	}
	_, err = p.out.Write(b)
	if err != nil {
		level.Error(p.logger).Log("msg", "cannot write JSON to output", "error", err)
		return
	}
}

func (p *jsonStreamPrinter) printStats(v *querypb.QueryStats) {
	msg := jsonMessage{Type: "stats", Data: v}
	b, err := json.Marshal(msg)
	if err != nil {
		level.Error(p.logger).Log("msg", "cannot marshal stats to JSON", "error", err)
		return
	}
	_, err = p.out.Write(b)
	if err != nil {
		level.Error(p.logger).Log("msg", "cannot write JSON to output", "error", err)
		return
	}
}

func (p *jsonStreamPrinter) printWarnings(v string) {
	msg := jsonMessage{Type: "warning", Data: v}
	b, err := json.Marshal(msg)
	if err != nil {
		level.Error(p.logger).Log("msg", "cannot marshal warnings to JSON", "error", err)
		return
	}
	_, err = p.out.Write(b)
	if err != nil {
		level.Error(p.logger).Log("msg", "cannot write JSON to output", "error", err)
		return
	}
}

func (p *jsonStreamPrinter) finish() {}
