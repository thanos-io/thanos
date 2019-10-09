package main

import (
	"fmt"
	"strings"

	"github.com/thanos-io/thanos/pkg/extflag"

	"github.com/prometheus/common/model"
	"gopkg.in/alecthomas/kingpin.v2"
)

func regGRPCFlags(cmd *kingpin.CmdClause) (
	grpcBindAddr *string,
	grpcTLSSrvCert *string,
	grpcTLSSrvKey *string,
	grpcTLSSrvClientCA *string,
) {
	grpcBindAddr = cmd.Flag("grpc-address", "Listen ip:port address for gRPC endpoints (StoreAPI). Make sure this address is routable from other components.").
		Default("0.0.0.0:10901").String()

	grpcTLSSrvCert = cmd.Flag("grpc-server-tls-cert", "TLS Certificate for gRPC server, leave blank to disable TLS").Default("").String()
	grpcTLSSrvKey = cmd.Flag("grpc-server-tls-key", "TLS Key for the gRPC server, leave blank to disable TLS").Default("").String()
	grpcTLSSrvClientCA = cmd.Flag("grpc-server-tls-client-ca", "TLS CA to verify clients against. If no client CA is specified, there is no client verification on server side. (tls.NoClientCert)").Default("").String()

	return grpcBindAddr,
		grpcTLSSrvCert,
		grpcTLSSrvKey,
		grpcTLSSrvClientCA
}

// TODO(povilasv): we don't need this anymore.
func regCommonServerFlags(cmd *kingpin.CmdClause) (
	grpcBindAddr *string,
	httpBindAddr *string,
	grpcTLSSrvCert *string,
	grpcTLSSrvKey *string,
	grpcTLSSrvClientCA *string) {
	httpBindAddr = regHTTPAddrFlag(cmd)
	grpcBindAddr, grpcTLSSrvCert, grpcTLSSrvKey, grpcTLSSrvClientCA = regGRPCFlags(cmd)

	return grpcBindAddr,
		httpBindAddr,
		grpcTLSSrvCert,
		grpcTLSSrvKey,
		grpcTLSSrvClientCA
}

func regHTTPAddrFlag(cmd *kingpin.CmdClause) *string {
	return cmd.Flag("http-address", "Listen host:port for HTTP endpoints.").Default("0.0.0.0:10902").String()
}

func modelDuration(flags *kingpin.FlagClause) *model.Duration {
	value := new(model.Duration)
	flags.SetValue(value)

	return value
}

func regCommonObjStoreFlags(cmd *kingpin.CmdClause, suffix string, required bool, extraDesc ...string) *extflag.PathOrContent {
	help := fmt.Sprintf("YAML file that contains object store%s configuration. See format details: https://thanos.io/storage.md/#configuration ", suffix)
	help = strings.Join(append([]string{help}, extraDesc...), " ")

	return extflag.RegisterPathOrContent(cmd, fmt.Sprintf("objstore%s.config", suffix), help, required)
}

func regCommonTracingFlags(app *kingpin.Application) *extflag.PathOrContent {
	return extflag.RegisterPathOrContent(
		app,
		"tracing.config",
		fmt.Sprintf("YAML file with tracing configuration. See format details: https://thanos.io/tracing.md/#configuration "),
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
