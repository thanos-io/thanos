// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"fmt"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/extflag"
	"gopkg.in/alecthomas/kingpin.v2"
)

func modelDuration(flags *kingpin.FlagClause) *model.Duration {
	value := new(model.Duration)
	flags.SetValue(value)

	return value
}

func regGRPCFlags(cmd *kingpin.CmdClause) (
	grpcBindAddr *string,
	grpcGracePeriod *model.Duration,
	grpcTLSSrvCert *string,
	grpcTLSSrvKey *string,
	grpcTLSSrvClientCA *string,
) {
	grpcBindAddr = cmd.Flag("grpc-address", "Listen ip:port address for gRPC endpoints (StoreAPI). Make sure this address is routable from other components.").
		Default("0.0.0.0:10901").String()
	grpcGracePeriod = modelDuration(cmd.Flag("grpc-grace-period", "Time to wait after an interrupt received for GRPC Server.").Default("2m")) // by default it's the same as query.timeout.

	grpcTLSSrvCert = cmd.Flag("grpc-server-tls-cert", "TLS Certificate for gRPC server, leave blank to disable TLS").Default("").String()
	grpcTLSSrvKey = cmd.Flag("grpc-server-tls-key", "TLS Key for the gRPC server, leave blank to disable TLS").Default("").String()
	grpcTLSSrvClientCA = cmd.Flag("grpc-server-tls-client-ca", "TLS CA to verify clients against. If no client CA is specified, there is no client verification on server side. (tls.NoClientCert)").Default("").String()

	return grpcBindAddr,
		grpcGracePeriod,
		grpcTLSSrvCert,
		grpcTLSSrvKey,
		grpcTLSSrvClientCA
}

func regHTTPFlags(cmd *kingpin.CmdClause) (httpBindAddr *string, httpGracePeriod *model.Duration) {
	httpBindAddr = cmd.Flag("http-address", "Listen host:port for HTTP endpoints.").Default("0.0.0.0:10902").String()
	httpGracePeriod = modelDuration(cmd.Flag("http-grace-period", "Time to wait after an interrupt received for HTTP Server.").Default("2m")) // by default it's the same as query.timeout.

	return httpBindAddr, httpGracePeriod
}

func regCommonObjStoreFlags(cmd *kingpin.CmdClause, suffix string, required bool, extraDesc ...string) *extflag.PathOrContent {
	help := fmt.Sprintf("YAML file that contains object store%s configuration. See format details: https://thanos.io/tip/thanos/storage.md/#configuration ", suffix)
	help = strings.Join(append([]string{help}, extraDesc...), " ")

	return extflag.RegisterPathOrContent(cmd, fmt.Sprintf("objstore%s.config", suffix), help, required)
}

func regCommonTracingFlags(app *kingpin.Application) *extflag.PathOrContent {
	return extflag.RegisterPathOrContent(
		app,
		"tracing.config",
		"YAML file with tracing configuration. See format details: https://thanos.io/tip/tracing.md/#configuration ",
		false,
	)
}

func regSelectorRelabelFlags(cmd *kingpin.CmdClause) *extflag.PathOrContent {
	return extflag.RegisterPathOrContent(
		cmd,
		"selector.relabel-config",
		"YAML file that contains relabeling configuration that allows selecting blocks. It follows native Prometheus relabel-config syntax. See format details: https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config ",
		false,
	)
}
