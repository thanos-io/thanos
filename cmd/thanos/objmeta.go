// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"time"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tags"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	hidden "github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/objmeta"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/tls"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/info"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/prober"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
)

func registerObjMeta(app *extkingpin.App) {
	cmd := app.Command(component.ObjMeta.String(), "ObjMeta for Thanos object metadata.")
	conf := &metaConfig{}
	conf.registerFlag(cmd)
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		tagOpts, grpcLogOpts, err := logging.ParsegRPCOptions("", conf.reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}
		return runObjMeta(g, logger, reg, tracer, component.ObjMeta, *conf, grpcLogOpts, tagOpts)
	})
}

func runObjMeta(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	comp component.Component,
	conf metaConfig,
	grpcLogOpts []grpc_logging.Option,
	tagOpts []tags.Option,
) error {

	objStoreConfContentYaml, err := conf.objStore.Content()
	if err != nil {
		return errors.Wrap(err, "getting object store config")
	}
	objMetaConfContentYaml, err := conf.objMeta.Content()
	if err != nil {
		return errors.Wrap(err, "getting objMeta config")
	}
	metaSrv, err := objmeta.NewServer(logger, reg, objStoreConfContentYaml, objMetaConfContentYaml, conf.blockMetaFetchConcurrency)
	if err != nil {
		return errors.Wrap(err, "new meta server")
	}
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return runutil.Repeat(conf.syncObjStoreInterval, ctx.Done(), func() error {
				if err := metaSrv.Sync(); err != nil {
					level.Info(logger).Log("msg", "sync error", "error", err)
				}
				return nil
			})
		}, func(err error) {
			cancel()
		})
	}
	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	srv := httpserver.New(logger, reg, comp, httpProbe,
		httpserver.WithListen(conf.http.bindAddress),
		httpserver.WithGracePeriod(time.Duration(conf.http.gracePeriod)),
		httpserver.WithTLSConfig(conf.http.tlsConfig),
	)

	g.Add(func() error {
		statusProber.Healthy()

		return srv.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		srv.Shutdown(err)
	})

	{

		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"),
			conf.grpc.tlsSrvCert, conf.grpc.tlsSrvKey, conf.grpc.tlsSrvClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		infoSrv := info.NewInfoServer(
			component.ObjMeta.String(),
		)

		s := grpcserver.New(logger, reg, tracer, grpcLogOpts, tagOpts, comp, grpcProbe,
			grpcserver.WithServer(objmeta.RegisterObjMetaServer(metaSrv)),
			grpcserver.WithServer(info.RegisterInfoServer(infoSrv)),
			grpcserver.WithListen(conf.grpc.bindAddress),
			grpcserver.WithGracePeriod(time.Duration(conf.grpc.gracePeriod)),
			grpcserver.WithTLSConfig(tlsCfg),
		)
		g.Add(func() error {
			statusProber.Ready()
			return s.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			s.Shutdown(err)
		})
	}

	level.Info(logger).Log("msg", "starting meta")
	return nil
}

type metaConfig struct {
	http                      httpConfig
	grpc                      grpcConfig
	reqLogConfig              *extflag.PathOrContent
	objStore                  extflag.PathOrContent
	objMeta                   extflag.PathOrContent
	syncObjStoreInterval      time.Duration
	blockMetaFetchConcurrency int
}

func (sc *metaConfig) registerFlag(cmd extkingpin.FlagClause) {
	sc.http.registerFlag(cmd)
	sc.grpc.registerFlag(cmd)
	sc.reqLogConfig = extkingpin.RegisterRequestLoggingFlags(cmd)
	sc.objStore = *extkingpin.RegisterCommonObjStoreFlags(cmd, "", false)
	sc.objMeta = *extflag.RegisterPathOrContent(
		hidden.HiddenCmdClause(cmd), "objmeta.config",
		"YAML that contains configuration for meta backend.",
		extflag.WithEnvSubstitution(),
	)
	cmd.Flag("objstore.sync-interval",
		"How often to sync from objstore").
		Default("24h").DurationVar(&sc.syncObjStoreInterval)
	cmd.Flag("block-meta-fetch-concurrency", "Number of goroutines to use when fetching block metadata from object storage.").
		Default("32").IntVar(&sc.blockMetaFetchConcurrency)
}
