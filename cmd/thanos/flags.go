package main

import (
	"fmt"
	"strings"

	"github.com/improbable-eng/thanos/pkg/flagutil"
	"github.com/prometheus/common/model"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
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

func regCommonObjStoreFlags(cmd *kingpin.CmdClause, suffix string, required bool, extraDesc ...string) *flagutil.PathOrContent {
	fileFlagName := fmt.Sprintf("objstore%s.config-file", suffix)
	contentFlagName := fmt.Sprintf("objstore%s.config", suffix)

	help := fmt.Sprintf("Path to YAML file that contains object store%s configuration.", suffix)
	help = strings.Join(append([]string{help}, extraDesc...), " ")
	bucketConfFile := cmd.Flag(fileFlagName, help).PlaceHolder("<bucket.config-yaml-path>")

	help = fmt.Sprintf("Alternative to '%s' flag. Object store%s configuration in YAML.", fileFlagName, suffix)
	help = strings.Join(append([]string{help}, extraDesc...), " ")
	bucketConf := cmd.Flag(contentFlagName, help).
		PlaceHolder("<bucket.config-yaml>")

	return flagutil.NewPathOrContentFlag(bucketConfFile, bucketConf, required)
}
