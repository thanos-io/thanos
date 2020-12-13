// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"github.com/thanos-io/thanos/pkg/receive"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/extkingpin"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/route"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/tls"
)

func registerReceiveRoute(app *extkingpin.App) {
	cmd := app.Command(component.ReceiveRoute.String(), "Accept Prometheus remote write API requests and forward them to a receive node.")

	httpBindAddr, httpGracePeriod := extkingpin.RegisterHTTPFlags(cmd)

	rwAddress := cmd.Flag("remote-write.address", "Address to listen on for remote write requests.").
		Default("0.0.0.0:19291").String()
	rwServerCert := cmd.Flag("remote-write.server-tls-cert", "TLS Certificate for HTTP server, leave blank to disable TLS.").Default("").String()
	rwServerKey := cmd.Flag("remote-write.server-tls-key", "TLS Key for the HTTP server, leave blank to disable TLS.").Default("").String()
	rwServerClientCA := cmd.Flag("remote-write.server-tls-client-ca", "TLS CA to verify clients against. If no client CA is specified, there is no client verification on server side. (tls.NoClientCert)").Default("").String()
	rwClientCert := cmd.Flag("remote-write.client-tls-cert", "TLS Certificates to use to identify this client to the server.").Default("").String()
	rwClientKey := cmd.Flag("remote-write.client-tls-key", "TLS Key for the client's certificate.").Default("").String()
	rwClientServerCA := cmd.Flag("remote-write.client-tls-ca", "TLS CA Certificates to use to verify servers.").Default("").String()
	rwClientServerName := cmd.Flag("remote-write.client-server-name", "Server name to verify the hostname on the returned gRPC certificates. See https://tools.ietf.org/html/rfc4366#section-3.1").Default("").String()
	hashringsFile := cmd.Flag("receive.hashrings-file", "Path to file that contains the hashring configuration.").
		PlaceHolder("<path>").String()

	refreshInterval := extkingpin.ModelDuration(cmd.Flag("receive.hashrings-file-refresh-interval", "Refresh interval to re-read the hashring configuration file. (used as a fallback)").
		Default("5m"))

	localEndpoint := cmd.Flag("receive.local-endpoint", "Endpoint of local receive node. Used to identify the local node in the hashring configuration.").String()

	tenantHeader := cmd.Flag("receive.tenant-header", "HTTP header to determine tenant for write requests.").Default(receive.DefaultTenantHeader).String()

	defaultTenantID := cmd.Flag("receive.default-tenant-id", "Default tenant ID to use when none is provided via a header.").Default(receive.DefaultTenant).String()

	replicationFactor := cmd.Flag("receive.replication-factor", "How many times to replicate incoming write requests.").Default("1").Uint64()

	forwardTimeout := extkingpin.ModelDuration(cmd.Flag("receive-forward-timeout", "Timeout for each forward request.").Default("5s").Hidden())


	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {

		var cw *route.ConfigWatcher
		var err error
		if *hashringsFile != "" {
			cw, err = route.NewConfigWatcher(log.With(logger, "component", "config-watcher"), reg, *hashringsFile, *refreshInterval)
			if err != nil {
				return err
			}
		}
		return runReceiveRoute(
			g,
			logger,
			reg,
			tracer,
			*httpBindAddr,
			time.Duration(*httpGracePeriod),
			*rwAddress,
			*rwServerCert,
			*rwServerKey,
			*rwServerClientCA,
			*rwClientCert,
			*rwClientKey,
			*rwClientServerCA,
			*rwClientServerName,
			cw,
			*localEndpoint,
			*tenantHeader,
			*defaultTenantID,
			*replicationFactor,
			time.Duration(*forwardTimeout),
			component.Receive,
		)
	})
}

func runReceiveRoute(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	httpBindAddr string,
	httpGracePeriod time.Duration,
	rwAddress string,
	rwServerCert string,
	rwServerKey string,
	rwServerClientCA string,
	rwClientCert string,
	rwClientKey string,
	rwClientServerCA string,
	rwClientServerName string,
	cw *route.ConfigWatcher,
	endpoint string,
	tenantHeader string,
	defaultTenantID string,
	replicationFactor uint64,
	forwardTimeout time.Duration,
	comp component.SourceStoreAPI,
) error {
	logger = log.With(logger, "component", "receive-route")
	level.Warn(logger).Log("msg", "setting up receive-route")
	rwTLSConfig, err := tls.NewServerConfig(log.With(logger, "protocol", "HTTP"), rwServerCert, rwServerKey, rwServerClientCA)
	if err != nil {
		return err
	}
	dialOpts, err := extgrpc.StoreClientGRPCOpts(logger, reg, tracer, rwServerCert != "", rwClientCert, rwClientKey, rwClientServerCA, rwClientServerName)
	if err != nil {
		return err
	}

	webHandler := route.NewHandler(log.With(logger, "component", "receive-handler"), &route.Options{
		ListenAddress:     rwAddress,
		Registry:          reg,
		TenantHeader:      tenantHeader,
		DefaultTenantID:   defaultTenantID,
		ReplicationFactor: replicationFactor,
		Tracer:            tracer,
		TLSConfig:         rwTLSConfig,
		DialOpts:          dialOpts,
		ForwardTimeout:    forwardTimeout,
	})

	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	// Start all components while we wait for TSDB to open but only load
	// initial config and mark ourselves as ready after it completed.

	level.Debug(logger).Log("msg", "setting up hashring")
	{
		// Note: the hashring configuration watcher
		// is the sender and thus closes the chan.
		// In the single-node case, which has no configuration
		// watcher, we close the chan ourselves.
		updates := make(chan route.Hashring, 1)

		// Check the hashring configuration on before running the watcher.
		if err := cw.ValidateConfig(); err != nil {
			cw.Stop()
			close(updates)
			return errors.Wrap(err, "failed to validate hashring configuration file")
		}
		{
			ctx, cancel := context.WithCancel(context.Background())
			g.Add(func() error {
				return route.HashringFromConfig(ctx, updates, cw)
			}, func(error) {
				cancel()
			})
		}
		cancel := make(chan struct{})
		g.Add(func() error {
			for {
				select {
				case h, ok := <-updates:
					if !ok {
						return nil
					}
					webHandler.Hashring(h)
					msg := "hashring has changed; server is not ready to receive web requests"
					statusProber.NotReady(errors.New(msg))
					level.Info(logger).Log("msg", msg)
				case <-cancel:
					return nil
				}
			}
		}, func(err error) {
			close(cancel)
		},
		)
	}

	level.Debug(logger).Log("msg", "setting up http server")
	srv := httpserver.New(logger, reg, comp, httpProbe,
		httpserver.WithListen(httpBindAddr),
		httpserver.WithGracePeriod(httpGracePeriod),
	)
	g.Add(func() error {
		statusProber.Healthy()

		return srv.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		srv.Shutdown(err)
	})

	level.Debug(logger).Log("msg", "setting up receive http handler")
	{
		g.Add(
			func() error {
				return errors.Wrap(webHandler.Run(), "error starting web server")
			},
			func(err error) {
				webHandler.Close()
			},
		)
	}

	level.Info(logger).Log("msg", "starting receive-route")
	return nil
}

