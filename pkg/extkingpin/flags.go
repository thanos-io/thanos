// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extkingpin

import (
	"fmt"
	"strings"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"gopkg.in/alecthomas/kingpin.v2"
)

func ModelDuration(flags *kingpin.FlagClause) *model.Duration {
	value := new(model.Duration)
	flags.SetValue(value)

	return value
}

// Custom parser for IP address flags.
type addressSlice []string

// addressSlice conforms to flag.Value interface.
func (a *addressSlice) Set(value string) error {
	*a = append(*a, value)
	if err := validateAddrs(*a); err != nil {
		return err
	}
	return nil
}

func (a *addressSlice) String() string {
	return strings.Join(*a, ",")
}

// Ensure flag is repeatable.
func (a *addressSlice) IsCumulative() bool {
	return true
}

func Addrs(flags *kingpin.FlagClause) (target *addressSlice) {
	target = &addressSlice{}
	flags.SetValue((*addressSlice)(target))
	return
}

// validateAddrs checks an address slice for duplicates and empty or invalid elements.
func validateAddrs(addrs addressSlice) error {
	set := map[string]struct{}{}

	for _, addr := range addrs {
		if addr == "" {
			return errors.New("Address is empty.")
		}

		qtypeAndName := strings.SplitN(addr, "+", 2)
		hostAndPort := strings.SplitN(addr, ":", 2)
		if len(qtypeAndName) != 2 && len(hostAndPort) != 2 {
			return errors.Errorf("Address %s is not of <host>:<port> format or a valid DNS query.", addr)
		}

		if _, ok := set[addr]; ok {
			return errors.Errorf("Address %s is duplicated.", addr)
		}

		set[addr] = struct{}{}
	}

	return nil
}

// RegisterHTTPFlags register flags commonly used to configure http servers with.
func RegisterHTTPFlags(cmd FlagClause) (httpBindAddr *string, httpGracePeriod *model.Duration, httpTLSConfig *string) {
	httpBindAddr = cmd.Flag("http-address", "Listen host:port for HTTP endpoints.").Default("0.0.0.0:10902").String()
	httpGracePeriod = ModelDuration(cmd.Flag("http-grace-period", "Time to wait after an interrupt received for HTTP Server.").Default("2m")) // by default it's the same as query.timeout.
	httpTLSConfig = cmd.Flag(
		"http.config",
		"[EXPERIMENTAL] Path to the configuration file that can enable TLS or authentication for all HTTP endpoints.",
	).Default("").String()
	return httpBindAddr, httpGracePeriod, httpTLSConfig
}

// RegisterCommonObjStoreFlags register flags to specify object storage configuration.
func RegisterCommonObjStoreFlags(cmd FlagClause, suffix string, required bool, extraDesc ...string) *extflag.PathOrContent {
	help := fmt.Sprintf("YAML file that contains object store%s configuration. See format details: https://thanos.io/tip/thanos/storage.md/#configuration ", suffix)
	help = strings.Join(append([]string{help}, extraDesc...), " ")
	opts := []extflag.Option{extflag.WithEnvSubstitution()}
	if required {
		opts = append(opts, extflag.WithRequired())
	}
	return extflag.RegisterPathOrContent(cmd, fmt.Sprintf("objstore%s.config", suffix), help, opts...)
}

// RegisterCommonTracingFlags registers flags to pass a tracing configuration to be used with OpenTracing.
func RegisterCommonTracingFlags(app FlagClause) *extflag.PathOrContent {
	return extflag.RegisterPathOrContent(
		app,
		"tracing.config",
		"YAML file with tracing configuration. See format details: https://thanos.io/tip/thanos/tracing.md/#configuration ",
		extflag.WithEnvSubstitution(),
	)
}

// RegisterRequestLoggingFlags registers flags to pass a request logging configuration to be used.
func RegisterRequestLoggingFlags(app FlagClause) *extflag.PathOrContent {
	return extflag.RegisterPathOrContent(
		app,
		"request.logging-config",
		"YAML file with request logging configuration. See format details: https://thanos.io/tip/thanos/logging.md/#configuration",
		extflag.WithEnvSubstitution(),
	)
}

// RegisterSelectorRelabelFlags register flags to specify relabeling configuration selecting blocks to process.
func RegisterSelectorRelabelFlags(cmd FlagClause) *extflag.PathOrContent {
	return extflag.RegisterPathOrContent(
		cmd,
		"selector.relabel-config",
		"YAML file with relabeling configuration that allows selecting blocks to act on based on their external labels. It follows thanos sharding relabel-config syntax. For format details see: https://thanos.io/tip/thanos/sharding.md/#relabelling ",
		extflag.WithEnvSubstitution(),
	)
}
