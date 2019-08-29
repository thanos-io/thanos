package main

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/pkg/errors"
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

type pathOrContent struct {
	fileFlagName    string
	contentFlagName string

	required bool
	path     *string
	content  *string
}

// Content returns content of the file. Flag that specifies path has priority.
// It returns error if the content is empty and required flag is set to true.
func (p *pathOrContent) Content() ([]byte, error) {
	if len(*p.path) > 0 && len(*p.content) > 0 {
		return nil, errors.Errorf("Both %s and %s flags set.", p.fileFlagName, p.contentFlagName)
	}

	var content []byte
	if len(*p.path) > 0 {
		c, err := ioutil.ReadFile(*p.path)
		if err != nil {
			return nil, errors.Wrapf(err, "loading YAML file %s for %s", *p.path, p.fileFlagName)
		}
		content = c
	} else {
		content = []byte(*p.content)
	}

	if len(content) == 0 && p.required {
		return nil, errors.Errorf("flag %s or %s is required for running this command and content cannot be empty.", p.fileFlagName, p.contentFlagName)
	}

	return content, nil
}

func regCommonObjStoreFlags(cmd *kingpin.CmdClause, suffix string, required bool, extraDesc ...string) *pathOrContent {
	fileFlagName := fmt.Sprintf("objstore%s.config-file", suffix)
	contentFlagName := fmt.Sprintf("objstore%s.config", suffix)

	help := fmt.Sprintf("Path to YAML file that contains object store%s configuration. See format details: https://thanos.io/storage.md/#configuration ", suffix)
	help = strings.Join(append([]string{help}, extraDesc...), " ")
	bucketConfFile := cmd.Flag(fileFlagName, help).PlaceHolder("<bucket.config-yaml-path>").String()

	help = fmt.Sprintf("Alternative to '%s' flag. Object store%s configuration in YAML. See format details: https://thanos.io/storage.md/#configuration ", fileFlagName, suffix)
	help = strings.Join(append([]string{help}, extraDesc...), " ")
	bucketConf := cmd.Flag(contentFlagName, help).
		PlaceHolder("<bucket.config-yaml>").String()

	return &pathOrContent{
		fileFlagName:    fileFlagName,
		contentFlagName: contentFlagName,
		required:        required,

		path:    bucketConfFile,
		content: bucketConf,
	}
}

func regCommonTracingFlags(app *kingpin.Application) *pathOrContent {
	fileFlagName := fmt.Sprintf("tracing.config-file")
	contentFlagName := fmt.Sprintf("tracing.config")

	help := fmt.Sprintf("Path to YAML file that contains tracing configuration. See fomrat details: https://thanos.io/tracing.md/#configuration ")
	tracingConfFile := app.Flag(fileFlagName, help).PlaceHolder("<tracing.config-yaml-path>").String()

	help = fmt.Sprintf("Alternative to '%s' flag. Tracing configuration in YAML. See format details: https://thanos.io/tracing.md/#configuration", fileFlagName)
	tracingConf := app.Flag(contentFlagName, help).PlaceHolder("<tracing.config-yaml>").String()

	return &pathOrContent{
		fileFlagName:    fileFlagName,
		contentFlagName: contentFlagName,
		required:        false,

		path:    tracingConfFile,
		content: tracingConf,
	}
}
